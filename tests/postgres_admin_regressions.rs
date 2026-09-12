//! Public administration contracts, with a private database for global queries.
#![cfg(feature = "tokio")]

mod support;

use apalis_core::{
    backend::{Filter, ListAllTasks, ListTasks, Metrics},
    task::status::Status,
};
use apalis_diesel_postgres::{Config, PgPool, PgTask, PostgresStorage, build_pool_with, setup};
use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl,
    connection::{InstrumentationEvent, SimpleConnection},
    r2d2::{CustomizeConnection, Error},
    sql_query,
    sql_types::{Array, Integer, Json, Text},
};
use lets_expect::*;
use std::sync::{Arc, Mutex};
use support::{Outcome, observe, with_conn, with_isolated_database};

#[derive(Debug)]
struct SortPlan;

impl CustomizeConnection<PgConnection, Error> for SortPlan {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), Error> {
        // Both index and explicit sort plans must implement the same total order.
        // Force the sort plan that exposed the original duplicate-page defect.
        conn.batch_execute("SET enable_indexscan=off; SET enable_bitmapscan=off;")
            .map_err(Error::QueryError)
    }
}

fn pool(url: String) -> Result<PgPool, String> {
    build_pool_with(url, |builder| {
        builder
            .max_size(2)
            .min_idle(Some(0))
            .connection_customizer(Box::new(SortPlan))
    })
    .map_err(|error| error.to_string())
}

#[derive(Debug)]
struct Pages {
    expected: Vec<String>,
    actual: Vec<String>,
    beyond: usize,
}

async fn pagination(global: bool, equal_times: bool) -> Result<Outcome<Pages>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(url)?;
        setup(&pool).await.map_err(|error| error.to_string())?;
        let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new("pages"));
        let writer = storage.clone();
        let mut expected = with_conn(pool, move |conn| {
            let tasks = (0..20).map(|i| {
                let mut task = PgTask::new(i.to_string());
                task.parts.run_at = 1_700_000_001 + if equal_times { 0 } else { i };
                task
            });
            writer
                .push_tasks_with_conn(conn, tasks)
                .map(|ids| ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>())
                .map_err(|error| error.to_string())
        })
        .await?;
        // Distinct dates sort in reverse submission order; tied dates sort by ID.
        if equal_times {
            expected.sort_unstable_by(|a, b| b.cmp(a));
        } else {
            expected.reverse();
        }
        let mut actual = Vec::new();
        let mut beyond = 0;
        for page in 1..=11 {
            let filter = Filter {
                status: None,
                page,
                page_size: Some(2),
            };
            let ids = if global {
                storage
                    .list_all_tasks(&filter)
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|task| task.parts.task_id)
                    .collect::<Vec<_>>()
            } else {
                storage
                    .list_tasks(&filter)
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|task| task.parts.task_id)
                    .collect::<Vec<_>>()
            };
            if page == 11 {
                beyond = ids.len();
            }
            for id in ids {
                actual.push(id.ok_or("listed task has no ID")?.to_string());
            }
        }
        Ok(Pages {
            expected,
            actual,
            beyond,
        })
    })
    .await
}

fn complete_pages() -> impl Fn(&Result<Outcome<Pages>, String>) -> AssertionResult {
    observe("ordered task pages", |run: &Pages| {
        if run.actual != run.expected || run.beyond != 0 {
            return Err(format!(
                "expected exact ordered set and empty end page; got {run:?}"
            ));
        }
        Ok(())
    })
}

#[derive(Debug)]
struct ListingPlan {
    expected: Vec<String>,
    actual: Vec<String>,
    expected_index: &'static str,
    plan: serde_json::Value,
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = Json, column_name = "QUERY PLAN")]
    plan: serde_json::Value,
}

async fn limited_listing(global: bool, completed: bool) -> Result<Outcome<ListingPlan>, String> {
    with_isolated_database(move |url| async move {
        // This pool checks that an ordered index path exists. The older
        // SortPlan pool independently checks pagination with explicit sorting.
        let pool = build_pool_with(url, |builder| builder.max_size(1))
            .map_err(|error| error.to_string())?;
        setup(&pool).await.map_err(|error| error.to_string())?;
        let status = if completed {
            Status::Done
        } else {
            Status::Pending
        };
        let status_name = status.to_string();
        let ids: Vec<String> = (1..=20_000_u128)
            .map(|id| ulid::Ulid::from(id).to_string())
            .collect();
        let expected = ids
            .iter()
            .enumerate()
            .rev()
            .filter(|(position, _)| global || position % 2 == 0)
            .take(50)
            .map(|(_, id)| id.clone())
            .collect();
        let captured = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed = captured.clone();
        let seed_status = status_name.clone();
        with_conn(pool.clone(), move |conn| {
            sql_query(
                "INSERT INTO apalis.jobs (id, job_type, job, status, run_at, done_at) \
                 SELECT id, CASE WHEN position % 2 = 1 THEN 'pages-a' ELSE 'pages-b' END, \
                 convert_to('\"payload\"', 'UTF8'), $2, to_timestamp(1700000001), \
                 CASE WHEN $2 = 'Done' THEN to_timestamp(1700000100) ELSE NULL END \
                 FROM unnest($1::text[]) WITH ORDINALITY AS seeded(id, position)",
            )
            .bind::<Array<Text>, _>(ids)
            .bind::<Text, _>(seed_status)
            .execute(conn)
            .map_err(|error| error.to_string())?;
            conn.batch_execute(
                "ANALYZE apalis.jobs; SET enable_seqscan = off; \
                 SET enable_bitmapscan = off; SET max_parallel_workers_per_gather = 0;",
            )
            .map_err(|error| error.to_string())?;
            conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
                if let InstrumentationEvent::StartQuery { query, .. } = event {
                    let rendered = query.to_string();
                    if rendered.starts_with("SELECT * FROM apalis.jobs") {
                        observed.lock().unwrap().push(rendered);
                    }
                }
            });
            Ok(())
        })
        .await?;
        let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new("pages-a"));
        let filter = Filter {
            status: Some(status),
            page: 1,
            page_size: Some(50),
        };
        let returned_ids = if global {
            storage
                .list_all_tasks(&filter)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|task| task.parts.task_id)
                .collect::<Vec<_>>()
        } else {
            storage
                .list_tasks(&filter)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|task| task.parts.task_id)
                .collect::<Vec<_>>()
        };
        let actual = returned_ids
            .into_iter()
            .map(|id| {
                id.map(|id| id.to_string())
                    .ok_or_else(|| "listed task has no ID".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let queries = captured.lock().map_err(|error| error.to_string())?.clone();
        let [rendered] = queries.as_slice() else {
            return Err(format!(
                "expected one public listing query, observed {}",
                queries.len()
            ));
        };
        // Diesel 2.3 renders the real SQL and its bind values in Display. This
        // deliberately fails if that format changes, rather than explaining a
        // separately maintained copy of the production query.
        let (statement, bind_text) = rendered
            .rsplit_once(" -- binds: ")
            .ok_or_else(|| format!("listing query has no captured binds: {rendered}"))?;
        let binds: serde_json::Value = serde_json::from_str(bind_text)
            .map_err(|error| format!("cannot decode captured listing binds: {error}"))?;
        let expected_binds = if global {
            serde_json::json!([status_name, 50, 0])
        } else {
            serde_json::json!([status_name, "pages-a", 50, 0])
        };
        if binds != expected_binds {
            return Err(format!(
                "expected listing binds {expected_binds}, observed {binds}"
            ));
        }
        let explain = format!("EXPLAIN (ANALYZE, FORMAT JSON) {statement}");
        let plan = with_conn(pool, move |conn| {
            let statement = sql_query(explain).bind::<Text, _>(status_name);
            let result = if global {
                statement
                    .bind::<Integer, _>(50)
                    .bind::<Integer, _>(0)
                    .get_result::<ExplainRow>(conn)
            } else {
                statement
                    .bind::<Text, _>("pages-a")
                    .bind::<Integer, _>(50)
                    .bind::<Integer, _>(0)
                    .get_result::<ExplainRow>(conn)
            };
            result
                .map(|row| row.plan)
                .map_err(|error| error.to_string())
        })
        .await?;
        Ok(ListingPlan {
            expected,
            actual,
            expected_index: if global {
                "jobs_list_all_idx"
            } else {
                "jobs_list_by_queue_idx"
            },
            plan,
        })
    })
    .await
}

fn reads_only_the_ordered_page() -> impl Fn(&Result<Outcome<ListingPlan>, String>) -> AssertionResult
{
    observe("bounded ordered listing", |run: &ListingPlan| {
        if run.actual != run.expected {
            return Err(format!(
                "the public API returned the wrong first page: {run:?}"
            ));
        }
        let plan = &run.plan[0]["Plan"];
        let mut pending = vec![plan];
        let mut scans = Vec::new();
        while let Some(node) = pending.pop() {
            let kind = node["Node Type"].as_str().ok_or("plan node has no type")?;
            if kind.contains("Sort") {
                return Err(format!("the limited page requires a {kind}: {}", run.plan));
            }
            if kind.ends_with("Scan") {
                scans.push(node);
            }
            if let Some(children) = node["Plans"].as_array() {
                pending.extend(children);
            }
        }
        if plan["Node Type"] != "Limit" || plan["Actual Rows"].as_f64() != Some(50.0) {
            return Err(format!(
                "the plan did not return exactly one limited page: {}",
                run.plan
            ));
        }
        let [scan] = scans.as_slice() else {
            return Err(format!("expected one ordered index scan: {}", run.plan));
        };
        if scan["Node Type"] != "Index Scan"
            || scan["Index Name"] != run.expected_index
            || scan["Actual Rows"].as_f64() != Some(50.0)
            || scan["Actual Loops"].as_f64() != Some(1.0)
            || scan
                .get("Rows Removed by Filter")
                .is_some_and(|rows| rows.as_f64() != Some(0.0))
            || scan
                .get("Rows Removed by Index Recheck")
                .is_some_and(|rows| rows.as_f64() != Some(0.0))
        {
            return Err(format!(
                "the index read beyond the first page: {}",
                run.plan
            ));
        }
        Ok(())
    })
}

async fn metrics(
    global: bool,
    populated: bool,
) -> Result<Outcome<Vec<apalis_core::backend::Statistic>>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(url)?;
        setup(&pool).await.map_err(|error| error.to_string())?;
        let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new("metrics"));
        if populated {
            let writer = storage.clone();
            with_conn(pool, move |conn| {
                let mut task = PgTask::new("value".to_owned());
                task.parts.run_at = 1_700_000_001;
                writer
                    .push_task_with_conn(conn, task)
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .await?;
        }
        if global {
            storage.global().await
        } else {
            storage.fetch_by_queue().await
        }
        .map_err(|error| error.to_string())
    })
    .await
}

fn exact_metrics(
    populated: bool,
) -> impl Fn(&Result<Outcome<Vec<apalis_core::backend::Statistic>>, String>) -> AssertionResult {
    observe(
        "exact metric values",
        move |stats: &Vec<apalis_core::backend::Statistic>| {
            for (title, expected) in [
                (
                    "MOST_RECENT_JOB",
                    if populated { "1700000001" } else { "0" },
                ),
                (
                    "OLDEST_PENDING_JOB",
                    if populated { "1700000001" } else { "0" },
                ),
                ("TOTAL_JOBS", if populated { "1" } else { "0" }),
            ] {
                let found = stats
                    .iter()
                    .find(|stat| stat.title == title)
                    .ok_or_else(|| format!("missing {title}"))?;
                if found.value != expected {
                    return Err(format!("{title}: expected {expected}, got {}", found.value));
                }
            }
            Ok(())
        },
    )
}

lets_expect! { #tokio_test
    expect(limited_listing(global, completed).await) as limited_listing {
        let global = false;
        let completed = false;
        to reads_only_the_ordered_page { reads_only_the_ordered_page() }
        when tasks_have_completed {
            let completed = true;
            to reads_only_the_ordered_page { reads_only_the_ordered_page() }
        }
        when the_view_covers_all_queues {
            let global = true;
            to reads_only_the_ordered_page { reads_only_the_ordered_page() }
            when tasks_have_completed {
                let completed = true;
                to reads_only_the_ordered_page { reads_only_the_ordered_page() }
            }
        }
    }
    expect(pagination(global, equal_times).await) as task_pages {
        let global = false;
        let equal_times = true;
        when the_view_covers_one_queue {
            when schedule_times_are_equal {
                to returns_every_task_once_in_order { complete_pages() }
            }
            when schedule_times_are_distinct {
                let equal_times = false;
                to returns_every_task_once_in_order { complete_pages() }
            }
        }
        when the_view_covers_all_queues {
            let global = true;
            when schedule_times_are_equal {
                to returns_every_task_once_in_order { complete_pages() }
            }
            when schedule_times_are_distinct {
                let equal_times = false;
                to returns_every_task_once_in_order { complete_pages() }
            }
        }
    }
    expect(metrics(global, populated).await) as queue_statistics {
        let global = false;
        let populated = true;
        when the_view_covers_one_queue {
            when a_task_exists { to preserves_exact_values { exact_metrics(true) } }
            when the_queue_is_empty {
                let populated = false;
                to defaults_absent_values_to_zero { exact_metrics(false) }
            }
        }
        when the_view_covers_all_queues {
            let global = true;
            when a_task_exists { to preserves_exact_values { exact_metrics(true) } }
            when the_queue_is_empty {
                let populated = false;
                to defaults_absent_values_to_zero { exact_metrics(false) }
            }
        }
    }
}
