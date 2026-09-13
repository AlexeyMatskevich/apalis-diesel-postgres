//! Regression specs for retained decode cleanup and local stream ownership.
use crate::test_support as support;
use crate::{CompactType, Config, Error, PgTask, PgTaskId, queries};
use apalis_core::{backend::TaskStream, task::status::Status, worker::context::WorkerContext};
use diesel::{
    RunQueryDsl, sql_query,
    sql_types::{Integer, Text},
};
use futures::{StreamExt, stream};
use lets_expect::*;
use std::{sync::Arc, time::Duration};

#[derive(Debug, diesel::QueryableByName)]
struct State {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempts: i32,
}

#[derive(Debug, Clone, Copy)]
enum Release {
    Immediate,
    AfterTransientFault,
    PersistentFault,
    ClaimSwept,
}

#[derive(Debug)]
struct DecodeOutcome {
    errors: Vec<&'static str>,
    corrupt_state: State,
    sibling: Option<PgTask<String>>,
    expected_sibling_id: PgTaskId,
    expected_worker: String,
    retired_after_next: bool,
    retired_after_drop: bool,
}

async fn decode_cleanup(
    release: Release,
    max_attempts: i32,
) -> Result<Option<DecodeOutcome>, String> {
    let Some(url) = support::database_url_or_skip()? else {
        return Ok(None);
    };
    let pool = crate::build_pool_with(url, |builder| {
        builder
            .max_size(1)
            .min_idle(Some(0))
            .connection_timeout(Duration::from_millis(50))
    })
    .map_err(|error| error.to_string())?;
    // Establish readiness separately from the short checkout timeout used to
    // inject the later held-pool failure. min_idle(0) does not create a connection.
    let startup_pool = pool.clone();
    crate::runtime::run_blocking(move || {
        drop(startup_pool.get_timeout(Duration::from_secs(30))?);
        Ok(())
    })
    .await
    .map_err(|error| format!("preparing the decode fixture connection: {error}"))?;
    crate::setup(&pool)
        .await
        .map_err(|error| error.to_string())?;
    let name = format!("decode-obligation-{}", ulid::Ulid::new());
    let config = Config::new(&name);
    let worker = WorkerContext::new::<()>(&name);
    let token: Arc<str> = format!("token-{name}").into();
    queries::initial_heartbeat(
        pool.clone(),
        config.clone(),
        worker.clone(),
        "decode-spec",
        token.clone(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let bad_id = PgTaskId::new(ulid::Ulid::new());
    let expected_sibling_id = PgTaskId::new(ulid::Ulid::new());
    let mut invalid = PgTask::new(b"{bad-json".to_vec());
    invalid.parts.task_id = Some(bad_id);
    invalid.parts.ctx = crate::PgContext::new().with_max_attempts(max_attempts);
    let mut sibling = PgTask::new(serde_json::to_vec("healthy-sibling").unwrap());
    sibling.parts.task_id = Some(expected_sibling_id);
    queries::push_tasks(pool.clone(), config.clone(), vec![invalid, sibling])
        .await
        .map_err(|error| error.to_string())?;
    let mut claimed = queries::fetch_next(
        pool.clone(),
        config.clone(),
        worker.clone(),
        Some(token.clone()),
    )
    .await
    .map_err(|error| error.to_string())?;
    if claimed.len() != 2 {
        return Err(format!("expected two claimed tasks, got {}", claimed.len()));
    }
    claimed.sort_by_key(|task| task.args != b"{bad-json");
    let leases = crate::lease::LeaseRegistry::default();
    let lease = leases.for_worker(&name);
    let compact: TaskStream<PgTask<CompactType>, Error> =
        stream::iter(claimed.into_iter().map(|task| Ok(Some(task)))).boxed();
    let compact = crate::fetcher::LeaseStream::new(compact, lease.clone(), true).boxed();
    let mut decoded = crate::fetcher::decode_task_stream::<String, crate::JsonCodec<CompactType>>(
        compact,
        pool.clone(),
        name.clone().into(),
        Some(token),
        lease.clone(),
    );
    // The single pooled connection is held so the release cannot check one out.
    if matches!(release, Release::ClaimSwept) {
        // Orphan recovery reclaimed the batch while the release was pending.
        let sweep_pool = pool.clone();
        let sweep_queue = name.clone();
        crate::runtime::run_blocking(move || {
            let mut conn = sweep_pool.get()?;
            sql_query(
                "UPDATE apalis.jobs SET status='Pending', lock_by=NULL, lock_at=NULL, \
                 attempts=attempts+1 WHERE job_type=$1",
            )
            .bind::<Text, _>(&sweep_queue)
            .execute(&mut conn)?;
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())?;
    }
    let mut held = match release {
        Release::Immediate | Release::ClaimSwept => None,
        Release::AfterTransientFault | Release::PersistentFault => {
            Some(pool.get().map_err(|error| error.to_string())?)
        }
    };
    if matches!(release, Release::AfterTransientFault) {
        // The fault clears while the stream is still retrying the release.
        let released_later = held.take();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(350)).await;
            drop(released_later);
        });
    }
    fn kind(item: &Option<Result<Option<PgTask<String>>, Error>>) -> &'static str {
        match item {
            Some(Err(Error::Decode(_))) => "decode",
            Some(Err(Error::Pool(_))) => "pool",
            Some(Err(Error::WorkerRetired { .. })) => "retired",
            Some(Err(_)) => "other_error",
            Some(Ok(Some(_))) => "task",
            Some(Ok(None)) => "empty",
            None => "ended",
        }
    }
    let mut outcomes = Vec::new();
    let first = decoded.next().await;
    outcomes.push(kind(&first));
    let retired_after_next = lease.is_retired();
    let second = decoded.next().await;
    outcomes.push(kind(&second));
    let sibling = match second {
        Some(Ok(Some(task))) => Some(task),
        _ => None,
    };
    drop(held);
    drop(decoded);
    let retired_after_drop = lease.is_retired();
    let name_cleanup = name.clone();
    let corrupt_state = crate::runtime::run_blocking(move || {
        let mut conn = pool.get()?;
        let row: State = sql_query("SELECT status, attempts FROM apalis.jobs WHERE id = $1")
            .bind::<Text, _>(bad_id.to_string())
            .get_result(&mut conn)?;
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&name_cleanup)
            .execute(&mut conn)?;
        sql_query("DELETE FROM apalis.workers WHERE id = $1")
            .bind::<Text, _>(&name_cleanup)
            .execute(&mut conn)?;
        Ok(row)
    })
    .await
    .map_err(|error| error.to_string())?;
    Ok(Some(DecodeOutcome {
        errors: outcomes,
        corrupt_state,
        sibling,
        expected_sibling_id,
        expected_worker: name,
        retired_after_next,
        retired_after_drop,
    }))
}

fn recovered(
    max_attempts: i32,
) -> impl Fn(&Result<Option<DecodeOutcome>, String>) -> AssertionResult {
    move |outcome| {
        let result = outcome.as_ref().map_err(|error| {
            AssertionError::new(vec![format!("decode cleanup scenario failed: {error}")])
        })?;
        let Some(outcome) = result else {
            return Ok(());
        };
        let expected_status = if max_attempts == 1 {
            "Killed"
        } else {
            "Failed"
        };
        let sibling_delivered = outcome.sibling.as_ref().is_some_and(|sibling| {
            sibling.args == "healthy-sibling"
                && sibling.parts.task_id == Some(outcome.expected_sibling_id)
                && sibling.parts.status.load() == Status::Running
                && sibling.parts.ctx.lock_by().as_deref() == Some(outcome.expected_worker.as_str())
                && sibling.parts.ctx.lock_at().is_some()
                && sibling.parts.attempt.current() == 0
        });
        if outcome.errors == ["decode", "task"]
            && outcome.corrupt_state.status == expected_status
            && outcome.corrupt_state.attempts == 1
            && sibling_delivered
            && !outcome.retired_after_next
            && outcome.retired_after_drop
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected the codec error then the sibling, {expected_status} after exactly one failed attempt, the original healthy sibling with Running owner/lock and attempt zero, an active worker until the whole stream is dropped; got {result:?}"
            )]))
        }
    }
}

fn retired_with_the_claim_intact()
-> impl Fn(&Result<Option<DecodeOutcome>, String>) -> AssertionResult {
    move |outcome| {
        let result = outcome.as_ref().map_err(|error| {
            AssertionError::new(vec![format!("decode cleanup scenario failed: {error}")])
        })?;
        let Some(outcome) = result else {
            return Ok(());
        };
        if outcome.errors == ["pool", "retired"]
            && outcome.corrupt_state.status == "Running"
            && outcome.corrupt_state.attempts == 0
            && outcome.sibling.is_none()
            && outcome.retired_after_next
            && outcome.retired_after_drop
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected the database error then WorkerRetired, the corrupt row still Running with no attempt consumed, no sibling delivered and the worker retired by the failed release; got {result:?}"
            )]))
        }
    }
}

fn retired_after_losing_the_claim()
-> impl Fn(&Result<Option<DecodeOutcome>, String>) -> AssertionResult {
    move |outcome| {
        let result = outcome.as_ref().map_err(|error| {
            AssertionError::new(vec![format!("decode cleanup scenario failed: {error}")])
        })?;
        let Some(outcome) = result else {
            return Ok(());
        };
        if outcome.errors == ["decode", "retired"]
            && outcome.corrupt_state.status == "Pending"
            && outcome.corrupt_state.attempts == 1
            && outcome.sibling.is_none()
            && outcome.retired_after_next
            && outcome.retired_after_drop
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected the codec error then WorkerRetired, the swept row untouched by the stale release, no sibling delivered and the worker retired for the lost claim; got {result:?}"
            )]))
        }
    }
}

lets_expect! { #tokio_test
    expect(decode_cleanup(release, max_attempts).await) as corrupt_claim_cleanup {
        let release = Release::Immediate;
        let max_attempts = 3_i32;
        to releases_the_corrupt_claim_before_delivering_the_valid_sibling { recovered(max_attempts) }
        when the_corrupt_task_has_exhausted_its_attempt_budget {
            let max_attempts = 1_i32;
            to kills_the_corrupt_task_and_preserves_the_valid_sibling { recovered(max_attempts) }
            when the_pool_recovers_while_the_release_is_retried {
                let release = Release::AfterTransientFault;
                to retains_the_terminal_cleanup_until_the_pool_recovers { recovered(max_attempts) }
            }
        }
        when the_pool_recovers_while_the_release_is_retried {
            let release = Release::AfterTransientFault;
            to absorbs_the_fault_and_releases_the_claim { recovered(max_attempts) }
        }
        when the_pool_stays_exhausted_past_the_retry_budget {
            let release = Release::PersistentFault;
            to retires_the_worker_so_orphan_recovery_can_reclaim_the_batch { retired_with_the_claim_intact() }
        }
        when orphan_recovery_swept_the_batch_before_the_release {
            let release = Release::ClaimSwept;
            to retires_the_worker_instead_of_delivering_reclaimed_siblings { retired_after_losing_the_claim() }
        }
    }
}
