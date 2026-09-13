//! Listener errors preserve accepted hints; overflow preserves durable poll recovery.
use crate::{
    Config, Error, PgPool, PgTaskId, SharedPostgresStorage, queries, shared::SharedFetcher,
    test_support as support,
};
use apalis_core::{
    backend::{
        poll_strategy::{StrategyBuilder, StreamStrategy},
        shared::MakeShared,
    },
    worker::context::WorkerContext,
};
use diesel::{
    QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Binary, Bool, Integer, Text},
};
use futures::{Stream, StreamExt, stream};
use lets_expect::*;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

const TOKEN: &str = "notification-regression-lease";
const WAIT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Failure {
    Healthy,
    Before,
    Between,
    After,
}

#[derive(QueryableByName, Debug)]
struct State {
    #[diesel(sql_type=Text)]
    id: String,
    #[diesel(sql_type=Text)]
    status: String,
    #[diesel(sql_type=Integer)]
    attempts: i32,
    #[diesel(sql_type=Bool)]
    owned: bool,
}

struct Fixture {
    pool: PgPool,
    queue: String,
    ids: Vec<PgTaskId>,
}
impl Fixture {
    async fn new(count: usize) -> Result<Option<Self>, String> {
        let Some(pool) = support::shared_pool().await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            pool,
            queue: format!("notification-regression-{}", ulid::Ulid::new()),
            ids: (0..count)
                .map(|_| PgTaskId::new(ulid::Ulid::new()))
                .collect(),
        }))
    }

    async fn insert(&self, register: bool) -> Result<(), String> {
        let queue = self.queue.clone();
        let ids = self.ids.clone();
        support::with_conn(self.pool.clone(), move |conn| {
            if register {
                sql_query("INSERT INTO apalis.workers(id,worker_type,storage_name,layers,last_seen,started_at,lease_token) VALUES($1,$1,'regression','',clock_timestamp(),clock_timestamp(),$2)")
                    .bind::<Text,_>(&queue).bind::<Text,_>(TOKEN).execute(conn).map_err(|e|e.to_string())?;
            }
            for id in ids {
                sql_query("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at) VALUES($1,$2,$3,'Pending',0,5,statement_timestamp())")
                    .bind::<Text,_>(id.to_string()).bind::<Text,_>(&queue)
                    .bind::<Binary,_>(b"\"payload\"".as_slice()).execute(conn).map_err(|e|e.to_string())?;
            }
            Ok(())
        }).await
    }

    async fn rows(&self) -> Result<Vec<State>, String> {
        let queue = self.queue.clone();
        support::with_conn(self.pool.clone(),move|conn| sql_query(
            "SELECT id,status,attempts,coalesce(lock_by=$1,false) AS owned FROM apalis.jobs WHERE job_type=$1 ORDER BY id")
            .bind::<Text,_>(queue).load::<State>(conn).map_err(|e|e.to_string())).await
    }

    async fn cleanup(&self) -> Result<(), String> {
        let queue = self.queue.clone();
        support::with_conn(self.pool.clone(), move |conn| {
            sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
                .bind::<Text, _>(&queue)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            sql_query("DELETE FROM apalis.workers WHERE worker_type=$1")
                .bind::<Text, _>(queue)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
    }

    fn expected(&self) -> Vec<String> {
        sorted_ids(&self.ids)
    }
}

fn sorted_ids(ids: &[PgTaskId]) -> Vec<String> {
    let mut values = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    values.sort();
    values
}

async fn batch(
    failure: Failure,
    capacity: usize,
    separate: bool,
    empty: bool,
) -> Result<(), String> {
    let Some(fixture) = Fixture::new(if empty { 0 } else { 2 }).await? else {
        return Ok(());
    };
    let result = async {
        fixture.insert(true).await?;
        let mut events: VecDeque<Option<Result<PgTaskId, Error>>> = fixture.ids.iter().copied().map(|id|Some(Ok(id))).collect();
        if failure != Failure::Healthy {
            let position = match failure { Failure::Before=>0, Failure::Between=>1, Failure::After=>events.len(), Failure::Healthy=>unreachable!() };
            events.insert(position, Some(Err(Error::NotifyListener("listener failed".into()))));
            if separate {events.insert(if failure==Failure::Before {1} else {position},None);}
        }
        let input=stream::poll_fn(move|cx|match events.pop_front() {
            Some(Some(event))=>Poll::Ready(Some(event)),
            Some(None)=>{cx.waker().wake_by_ref();Poll::Pending},
            None=>Poll::Ready(None),
        });
        let output=queries::batch_ids_into_tasks(fixture.pool.clone(),fixture.queue.clone(),fixture.queue.clone(),capacity,input,Some(Arc::from(TOKEN))).collect::<Vec<_>>().await;
        let mut delivered=Vec::new(); let mut errors=Vec::new();
        for item in output {match item {
            Ok(Some(task))=>delivered.push(task.parts.task_id.map(|id|id.to_string()).unwrap_or_default()),
            Ok(None)=>return Err("batch yielded an empty task".into()),
            Err(Error::NotifyListener(message))=>errors.push(message),
            Err(error)=>return Err(error.to_string()),
        }}
        delivered.sort();
        let rows=fixture.rows().await?;
        let expected_errors=if failure==Failure::Healthy {vec![]} else {vec!["listener failed".to_owned()]};
        if delivered==fixture.expected() && errors==expected_errors
            && rows.iter().map(|row|row.id.clone()).collect::<Vec<_>>()==fixture.expected()
            && rows.iter().all(|row|row.status=="Queued" && row.attempts==0 && row.owned) {Ok(())}
        else {Err(format!("failure={failure:?}, capacity={capacity}, separate={separate}, empty={empty}; expected={:?}; delivered={delivered:?}; errors={errors:?}; rows={rows:?}",fixture.expected()))}
    }.await;
    fixture.cleanup().await?;
    result
}

lets_expect! { #tokio_test
 expect(batch(failure,capacity,separate,empty).await) as batch_retains_accepted_hints {
  let failure=Failure::Healthy;let capacity=3_usize;let separate=false;let empty=false;
  to delivers_every_hint { be_ok }
  when each_event_fills_a_batch {let capacity=1_usize;to delivers_every_hint {be_ok}}
  when the_listener_fails_before_the_hints {let failure=Failure::Before;
   to reports_the_failure_and_delivers_every_hint {be_ok}
   when each_event_fills_a_batch {let capacity=1_usize;to reports_the_failure_and_delivers_every_hint {be_ok}}
   when the_source_waits_after_the_failure { let separate=true;to reports_the_failure_and_delivers_every_hint {be_ok}}
  }
  when the_listener_fails_between_the_hints {let failure=Failure::Between;
   to reports_the_failure_and_delivers_every_hint {be_ok}
   when each_event_fills_a_batch {let capacity=1_usize;to reports_the_failure_and_delivers_every_hint {be_ok}}
  }
  when the_listener_fails_after_the_hints {let failure=Failure::After;
   to reports_the_failure_and_delivers_every_hint {be_ok}
   when each_event_fills_a_batch {let capacity=1_usize;to reports_the_failure_and_delivers_every_hint {be_ok}}
   when the_source_waits_before_the_failure {let separate=true;to reports_the_failure_and_delivers_every_hint {be_ok}}
  }
  when no_hints_are_available {let empty=true;
   to yields_no_tasks_or_errors {be_ok}
   when the_listener_fails {let failure=Failure::Before;to reports_the_failure_without_claiming {be_ok}}
  }
 }
}

#[derive(Debug)]
struct AppName(String);
impl diesel::r2d2::CustomizeConnection<diesel::PgConnection, diesel::r2d2::Error> for AppName {
    fn on_acquire(&self, conn: &mut diesel::PgConnection) -> Result<(), diesel::r2d2::Error> {
        sql_query("SELECT set_config('application_name',$1,false)")
            .bind::<Text, _>(&self.0)
            .execute(conn)
            .map(|_| ())
            .map_err(diesel::r2d2::Error::QueryError)
    }
}
#[derive(QueryableByName)]
struct Pid {
    #[diesel(sql_type=BigInt)]
    pid: i64,
}

enum Source {
    Single(queries::notify::NotifyTaskIds),
    Shared(SharedFetcher),
}
impl Source {
    fn state(&self) -> (usize, bool) {
        match self {
            Self::Single(source) => queries::notify::test_support::state(source),
            Self::Shared(source) => crate::shared::test_support::state(source),
        }
    }
}
impl Stream for Source {
    type Item = Result<PgTaskId, Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Single(source) => Pin::new(source).poll_next(cx),
            Self::Shared(source) => Pin::new(source).poll_next(cx),
        }
    }
}

async fn wait_state(source: &Source, expected: (usize, bool)) -> Result<(), String> {
    let deadline = Instant::now() + WAIT;
    loop {
        let got = source.state();
        if got == expected {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("expected buffered state {expected:?}, got {got:?}"));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn listener_pid(pool: PgPool) -> Result<i64, String> {
    let deadline = Instant::now() + WAIT;
    loop {
        let rows=support::with_conn(pool.clone(),move|conn|sql_query(
            "SELECT pid::bigint AS pid FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND query='LISTEN \"apalis::job::insert\"' AND state='idle'")
            .load::<Pid>(conn).map_err(|e|e.to_string())).await?;
        if rows.len() == 1 {
            return Ok(rows[0].pid);
        }
        if Instant::now() > deadline {
            return Err(format!("expected one own listener, got {}", rows.len()));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_idle(pool: &PgPool) -> Result<(), String> {
    let deadline = Instant::now() + WAIT;
    while pool.state().connections != pool.state().idle_connections {
        if Instant::now() > deadline {
            return Err("listener did not release its pooled connection".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

fn config(queue: &str) -> Config {
    Config::new(queue).set_buffer_size(3).with_poll_interval(
        StrategyBuilder::new()
            .apply(StreamStrategy::new(stream::pending::<()>()))
            .build(),
    )
}

async fn consume(
    fixture: &Fixture,
    source: Source,
    failures: usize,
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut output = crate::fetcher::notify_backed_compact_stream(
        "notification-regression",
        source,
        fixture.pool.clone(),
        config(&fixture.queue),
        WorkerContext::new::<()>(&fixture.queue),
        Arc::from(TOKEN),
        None,
    );
    let deadline = tokio::time::Instant::now() + WAIT;
    let mut delivered = Vec::new();
    let mut errors = Vec::new();
    while delivered.len() < 2 || errors.len() < failures {
        match tokio::time::timeout_at(deadline, output.next()).await {
            Ok(Some(Ok(Some(task)))) => delivered.push(
                task.parts
                    .task_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ),
            Ok(Some(Ok(None))) => {}
            Ok(Some(Err(error))) => errors.push(error.to_string()),
            Ok(None) | Err(_) => break,
        }
    }
    delivered.sort();
    Ok((delivered, errors))
}

async fn live(shared: bool, terminate: bool) -> Result<(), String> {
    let Some(mut fixture) = Fixture::new(2).await? else {
        return Ok(());
    };
    let url = support::database_url_or_skip()?.ok_or("DATABASE_URL disappeared")?;
    fixture.pool = crate::build_pool_with(url, |builder| {
        builder
            .max_size(4)
            .min_idle(Some(0))
            .connection_customizer(Box::new(AppName(fixture.queue.clone())))
    })
    .map_err(|e| e.to_string())?;
    let result=async {
        let mut factory:SharedPostgresStorage=SharedPostgresStorage::new(&fixture.pool);
        let source=if shared {
            let storage=<SharedPostgresStorage as MakeShared<String>>::make_shared_with_config(&mut factory,config(&fixture.queue)).map_err(|e|e.to_string())?;
            Source::Shared(storage.fetcher)
        } else { Source::Single(queries::notify_task_ids(fixture.pool.clone(),fixture.queue.clone(),3)) };
        let pid=listener_pid(fixture.pool.clone()).await?;
        fixture.insert(false).await?;
        // Exactly two committed notifications are the complete expected input.
        wait_state(&source,(2,false)).await?;
        if terminate {
            support::with_conn(fixture.pool.clone(),move|conn|sql_query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND application_name=current_setting('application_name') AND query='LISTEN \"apalis::job::insert\"'")
                .bind::<BigInt,_>(pid).execute(conn).map_err(|e|e.to_string())
                .and_then(|count|if count==1 {Ok(())} else {Err(format!("terminated rows {count}"))})).await?;
            wait_state(&source,(2,true)).await?;
        }
        let (delivered,errors)=consume(&fixture,source,usize::from(terminate)).await?;
        let rows=fixture.rows().await?;
        drop(factory);
        if delivered==fixture.expected() && errors.len()==usize::from(terminate)
            && errors.iter().all(|error|error.contains("notification"))
            && rows.iter().map(|row|row.id.clone()).collect::<Vec<_>>()==fixture.expected()
            && rows.iter().all(|row|row.status=="Queued"&&row.owned&&row.attempts==0) {Ok(())}
        else {Err(format!("shared={shared}, terminate={terminate}; expected={:?}; delivered={delivered:?}; errors={errors:?}; rows={rows:?}",fixture.expected()))}
    }.await;
    wait_idle(&fixture.pool).await?;
    fixture.cleanup().await?;
    result
}

lets_expect! { #tokio_test
    expect(live(shared,terminate).await) as listener_retains_accepted_hints {
        let shared=false; let terminate=false;
        to delivers_the_buffered_tasks_without_polling {be_ok}
        when the_listener_connection_is_terminated {
            let terminate=true;
            to reports_failure_and_delivers_the_buffered_tasks_without_polling {be_ok}
        }
        when consumers_share_the_listener {
            let shared=true;
            to delivers_the_buffered_tasks_without_polling {be_ok}
            when the_listener_connection_is_terminated {
                let terminate=true;
                to reports_failure_and_delivers_the_buffered_tasks_without_polling {be_ok}
            }
        }
    }
}

async fn overflow(shared: bool) -> Result<(), String> {
    let Some(fixture) = Fixture::new(4).await? else {
        return Ok(());
    };
    let result=async {
        fixture.insert(false).await?;
        let (source,accepted)=if shared {
            let (source,accepted)=crate::shared::test_support::saturated(&fixture.ids);
            (Source::Shared(source),accepted)
        } else {
            let (source,accepted)=queries::notify::test_support::saturated(&fixture.ids);
            (Source::Single(source),accepted)
        };
        if accepted.len()!=2 {return Err(format!("expected two accepted hints, got {accepted:?}"));}
        let (delivered,errors)=consume(&fixture,source,0).await?;
        let before=fixture.rows().await?;
        let recovered=queries::fetch_next(fixture.pool.clone(),Config::new(&fixture.queue).set_buffer_size(8),WorkerContext::new::<()>(&fixture.queue),Some(Arc::from(TOKEN)))
            .await.map_err(|e|e.to_string())?;
        let mut all=delivered.clone();
        all.extend(recovered.into_iter().map(|task|task.parts.task_id.map(|id|id.to_string()).unwrap_or_default())); all.sort();
        let after=fixture.rows().await?;
        let mut pending=before.iter().filter(|row|row.status=="Pending"&&!row.owned&&row.attempts==0).map(|row|row.id.clone()).collect::<Vec<_>>(); pending.sort();
        let rejected=sorted_ids(&fixture.ids.iter().copied().filter(|id|!accepted.contains(id)).collect::<Vec<_>>());
        if delivered==sorted_ids(&accepted) && errors.is_empty() && pending==rejected
            && before.iter().filter(|row|row.status=="Queued"&&row.owned&&row.attempts==0).map(|row|row.id.clone()).collect::<Vec<_>>()==delivered
            && all==fixture.expected() && after.iter().map(|row|row.id.clone()).collect::<Vec<_>>()==fixture.expected()
            && after.iter().all(|row|row.status=="Queued"&&row.owned&&row.attempts==0) {Ok(())}
        else {Err(format!("shared={shared}; accepted={accepted:?}; delivered={delivered:?}; errors={errors:?}; all={all:?}; before={before:?}; after={after:?}"))}
    }.await;
    fixture.cleanup().await?;
    result
}
lets_expect! { #tokio_test
    expect(overflow(shared).await) as overflowing_hints_preserve_durable_tasks {
        let shared=false;
        to claims_accepted_ids_and_recovers_dropped_hints_by_polling {be_ok}
        when consumers_share_the_listener {
            let shared=true;
            to claims_accepted_ids_and_recovers_dropped_hints_by_polling {be_ok}
        }
    }
}
