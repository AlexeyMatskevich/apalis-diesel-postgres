//! A successful completion poll must separate consecutive database failures.
#![cfg(feature = "tokio")]

mod support;

use apalis_core::{
    backend::{TaskSink, WaitForCompletion},
    task::{builder::TaskBuilder, status::Status},
};
use apalis_diesel_postgres::{Config, PgTaskId, PostgresStorage, build_pool_with, setup};
use diesel::{
    Connection, PgConnection, RunQueryDsl, connection::InstrumentationEvent,
    connection::SimpleConnection, sql_query, sql_types::Text,
};
use futures::StreamExt;
use lets_expect::*;
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

struct PollFaults {
    control: PgConnection,
    ids: Vec<PgTaskId>,
    partial: bool,
    hidden: bool,
    started: usize,
    // None is success; Some records the actual database diagnostic.
    finished: Vec<Option<String>>,
    fault: Option<String>,
}

impl PollFaults {
    fn before_query(&mut self) -> Result<(), String> {
        let step = self.started;
        self.started += 1;
        let hidden = matches!(step, 0 | 1 | 3 | 4);
        if hidden != self.hidden {
            self.control
                .batch_execute(if hidden {
                    "ALTER TABLE apalis.jobs RENAME TO completion_jobs_hidden"
                } else {
                    "ALTER TABLE apalis.completion_jobs_hidden RENAME TO jobs"
                })
                .map_err(|e| e.to_string())?;
            self.hidden = hidden;
        }
        let completed = match step {
            2 if self.partial => Some(self.ids[0]),
            5 => self.ids.last().copied(),
            _ => None,
        };
        if let Some(id) = completed {
            sql_query("UPDATE apalis.jobs SET status='Done', done_at=clock_timestamp(), last_result='{\"Ok\":\"completed\"}'::jsonb WHERE id=$1")
                .bind::<Text, _>(id.to_string())
                .execute(&mut self.control)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

struct ObservePolls(Arc<Mutex<PollFaults>>);

impl fmt::Debug for ObservePolls {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservePolls")
            .finish_non_exhaustive()
    }
}

impl diesel::r2d2::CustomizeConnection<PgConnection, diesel::r2d2::Error> for ObservePolls {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        let observed = self.0.clone();
        conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            // Only schedule faults around the public completion SELECT, the
            // only statement that reports a terminal verdict. The query itself
            // and its state transitions remain production code.
            match event {
                InstrumentationEvent::StartQuery { query, .. }
                    if format!("{query}").contains(" AS terminal") =>
                {
                    let mut state = observed.lock().unwrap();
                    if let Err(error) = state.before_query() {
                        state.fault = Some(error);
                    }
                }
                InstrumentationEvent::FinishQuery { query, error, .. }
                    if format!("{query}").contains(" AS terminal") =>
                {
                    observed
                        .lock()
                        .unwrap()
                        .finished
                        .push(error.map(ToString::to_string));
                }
                _ => {}
            }
        });
        Ok(())
    }
}

#[derive(Debug)]
struct CompletionRecovery {
    expected_ids: Vec<PgTaskId>,
    completed: Vec<(PgTaskId, Status, Result<String, String>)>,
    query_count: usize,
    query_results: Vec<Option<String>>,
    fixture_error: Option<String>,
}

async fn run_completion_recovery(
    partial: bool,
) -> Result<support::Outcome<CompletionRecovery>, String> {
    support::with_isolated_database(move |url| async move {
        let setup_url = url.clone();
        let setup_pool = tokio::task::spawn_blocking(move || {
            build_pool_with(setup_url, |b| b.max_size(1).min_idle(Some(0)))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())??;
        setup(&setup_pool).await.map_err(|e| e.to_string())?;
        let mut producer = PostgresStorage::<String>::new_with_config(
            &setup_pool,
            &Config::new("completion-recovery"),
        );
        let mut ids = Vec::new();
        for _ in 0..if partial { 2 } else { 1 } {
            let id = PgTaskId::new(ulid::Ulid::new());
            producer
                .push_task(
                    TaskBuilder::new("pending".to_owned())
                        .with_task_id(id)
                        .build(),
                )
                .await
                .map_err(|e| e.to_string())?;
            ids.push(id);
        }
        drop(producer);
        drop(setup_pool);
        let expected_ids = ids.clone();
        let (pool, observed) = tokio::task::spawn_blocking(move || {
            let control = PgConnection::establish(&url).map_err(|e| e.to_string())?;
            let observed = Arc::new(Mutex::new(PollFaults {
                control,
                ids,
                partial,
                hidden: false,
                started: 0,
                finished: Vec::new(),
                fault: None,
            }));
            let customizer = ObservePolls(observed.clone());
            let pool = build_pool_with(url, |b| {
                b.max_size(1)
                    .min_idle(Some(0))
                    .connection_customizer(Box::new(customizer))
            })
            .map_err(|e| e.to_string())?;
            Ok::<_, String>((pool, observed))
        })
        .await
        .map_err(|e| e.to_string())??;
        let storage =
            PostgresStorage::<String>::new_with_config(&pool, &Config::new("completion-observer"));
        let mut results = <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(
            &storage,
            expected_ids.clone(),
        );
        let completed = tokio::time::timeout(Duration::from_secs(10), async {
            let mut completed = Vec::new();
            while let Some(result) = results.next().await {
                let result = result.map_err(|e| e.to_string())?;
                completed.push((result.task_id, result.status, result.result));
            }
            Ok::<_, String>(completed)
        })
        .await
        .map_err(|_| "completion recovery did not reach EOF".to_owned())??;
        let state = observed.lock().map_err(|e| e.to_string())?;
        Ok(CompletionRecovery {
            expected_ids,
            completed,
            query_count: state.started,
            query_results: state.finished.clone(),
            fixture_error: state.fault.clone(),
        })
    })
    .await
}

fn completes_across_separate_failures()
-> impl Fn(&Result<support::Outcome<CompletionRecovery>, String>) -> AssertionResult {
    support::observe("completion recovery", |run: &CompletionRecovery| {
        let expected = run
            .expected_ids
            .iter()
            .copied()
            .map(|id| (id, Status::Done, Ok("completed".to_owned())))
            .collect::<Vec<_>>();
        let errors = run
            .query_results
            .iter()
            .map(Option::is_some)
            .collect::<Vec<_>>();
        let only_missing_table = run
            .query_results
            .iter()
            .flatten()
            .all(|error| error.contains("relation \"apalis.jobs\" does not exist"));
        if run.completed == expected
            && run.query_count == 6
            && errors == [true, true, false, true, true, false]
            && only_missing_table
            && run.fixture_error.is_none()
        {
            Ok(())
        } else {
            Err(format!("unexpected completion sequence: {run:?}"))
        }
    })
}

lets_expect! { #tokio_test
    expect(run_completion_recovery(partial).await) as completion_after_intermittent_database_errors {
        let partial = false;
        when a_successful_empty_poll_separates_two_failure_streaks {
            to completes_the_requested_task_and_ends_the_stream {
                completes_across_separate_failures()
            }
        }
        when a_partial_completion_separates_two_failure_streaks {
            let partial = true;
            to completes_the_remaining_task_and_ends_the_stream {
                completes_across_separate_failures()
            }
        }
    }
}
