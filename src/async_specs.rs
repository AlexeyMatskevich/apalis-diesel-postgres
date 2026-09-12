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

#[derive(Debug)]
struct DecodeOutcome {
    errors: Vec<&'static str>,
    corrupt_state: State,
    sibling: PgTask<String>,
    expected_sibling_id: PgTaskId,
    expected_worker: String,
    active_after_next: bool,
    retired_after_drop: bool,
}

async fn decode_cleanup(
    transient_failure: bool,
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
    );
    let held = if transient_failure {
        Some(pool.get().map_err(|error| error.to_string())?)
    } else {
        None
    };
    let mut outcomes = Vec::new();
    if transient_failure {
        match decoded.next().await {
            Some(Err(Error::Pool(_))) => outcomes.push("pool"),
            other => return Err(format!("expected transient pool error, got {other:?}")),
        }
    }
    drop(held);
    match decoded.next().await {
        Some(Err(Error::Decode(_))) => outcomes.push("decode"),
        other => return Err(format!("expected codec error after release, got {other:?}")),
    }
    let sibling = match decoded.next().await {
        Some(Ok(Some(task))) => task,
        other => return Err(format!("expected valid sibling, got {other:?}")),
    };
    let active_after_next = !lease.is_retired();
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
        active_after_next,
        retired_after_drop,
    }))
}

fn recovered(
    transient_failure: bool,
    max_attempts: i32,
) -> impl Fn(&Result<Option<DecodeOutcome>, String>) -> AssertionResult {
    move |outcome| {
        let result = outcome.as_ref().map_err(|error| {
            AssertionError::new(vec![format!("decode cleanup scenario failed: {error}")])
        })?;
        let Some(outcome) = result else {
            return Ok(());
        };
        let expected = if transient_failure {
            vec!["pool", "decode"]
        } else {
            vec!["decode"]
        };
        let expected_status = if max_attempts == 1 {
            "Killed"
        } else {
            "Failed"
        };
        if outcome.errors == expected
            && outcome.corrupt_state.status == expected_status
            && outcome.corrupt_state.attempts == 1
            && outcome.sibling.args == "healthy-sibling"
            && outcome.sibling.parts.task_id == Some(outcome.expected_sibling_id)
            && outcome.sibling.parts.status.load() == Status::Running
            && outcome.sibling.parts.ctx.lock_by().as_deref()
                == Some(outcome.expected_worker.as_str())
            && outcome.sibling.parts.ctx.lock_at().is_some()
            && outcome.sibling.parts.attempt.current() == 0
            && outcome.active_after_next
            && outcome.retired_after_drop
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected {expected:?}, {expected_status} after exactly one failed attempt, the original healthy sibling with Running owner/lock and attempt zero, active next and retired whole drop; got {result:?}"
            )]))
        }
    }
}

lets_expect! { #tokio_test
    expect(decode_cleanup(transient_failure, max_attempts).await) as corrupt_claim_cleanup {
        let transient_failure = false;
        let max_attempts = 3_i32;
        to releases_the_corrupt_claim_before_delivering_the_valid_sibling { recovered(transient_failure, max_attempts) }
        when the_corrupt_task_has_exhausted_its_attempt_budget {
            let max_attempts = 1_i32;
            to kills_the_corrupt_task_and_preserves_the_valid_sibling { recovered(transient_failure, max_attempts) }
            when the_pool_is_exhausted_during_decode_cleanup {
                let transient_failure = true;
                to retains_the_terminal_cleanup_until_the_pool_recovers { recovered(transient_failure, max_attempts) }
            }
        }
        when the_pool_is_exhausted_during_decode_cleanup {
            let transient_failure = true;
            to preserves_the_cleanup_obligation_until_the_pool_recovers { recovered(transient_failure, max_attempts) }
        }
    }
}
