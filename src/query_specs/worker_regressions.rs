//! Isolated regressions for worker fencing, startup recovery and SQL upgrades.

use crate::{Config, Error, MIGRATIONS, PgPool, queries::worker, test_support as support};
use apalis_core::worker::context::WorkerContext;
use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl, connection::SimpleConnection,
    sql_query, sql_types::Jsonb,
};
use diesel_migrations::MigrationHarness;
use lets_expect::*;
use serde_json::{Value, json};
use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};
use support::{Outcome, observe, with_conn, with_isolated_database};

fn connection(url: &str) -> Result<PgConnection, String> {
    let mut conn = PgConnection::establish(url).map_err(|e| e.to_string())?;
    sql(&mut conn, "SET statement_timeout='10s'")?;
    Ok(conn)
}

fn sql(conn: &mut PgConnection, query: &str) -> Result<(), String> {
    conn.batch_execute(query).map_err(|e| e.to_string())
}

#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type = Jsonb)]
    value: Value,
}

fn read(conn: &mut PgConnection, expression: &str) -> Result<Value, String> {
    sql_query(format!(
        "SELECT COALESCE(({expression})::jsonb, 'null'::jsonb) AS value"
    ))
    .get_result::<JsonRow>(conn)
    .map(|row| row.value)
    .map_err(|e| e.to_string())
}

async fn pool(url: String) -> Result<PgPool, String> {
    let pool = crate::build_pool_with(url, |builder| builder.max_size(2).min_idle(Some(1)))
        .map_err(|e| e.to_string())?;
    crate::setup(&pool).await.map_err(|e| e.to_string())?;
    Ok(pool)
}

fn config() -> Config {
    Config::new("fence-queue").set_reenqueue_orphaned_after(Duration::from_secs(30))
}

fn worker_context() -> WorkerContext {
    WorkerContext::new::<()>("worker")
}

fn worker_fixture(conn: &mut PgConnection) -> Result<(), String> {
    sql(conn, "INSERT INTO apalis.workers(id,worker_type,storage_name,layers,last_seen,started_at,lease_token)
      VALUES('worker','fence-queue','regression','',clock_timestamp(),clock_timestamp(),'current')")
}

fn guard(conn: &mut PgConnection) -> Result<bool, String> {
    worker::lock_current_worker(conn, "worker", "fence-queue", Some("current"))
        .map_err(|e| e.to_string())
}

// The peer either completes while the holder still owns its transaction or is
// observed in pg_blocking_pids. No sleep duration is used as evidence of blocking.
// Every outcome releases the holder and joins its own peer before returning.
fn peer_while_held<F>(
    holder: &mut PgConnection,
    mut peer: PgConnection,
    monitor: &mut PgConnection,
    operation: F,
) -> Result<(bool, bool), String>
where
    F: FnOnce(&mut PgConnection) -> Result<bool, String> + Send + 'static,
{
    let pid = read(&mut peer, "to_jsonb(pg_backend_pid())")?
        .as_i64()
        .ok_or("backend PID was not an integer")?;
    let (send, receive) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let result = operation(&mut peer);
        // Completion notification has no bearing on the captured SQL result.
        let _ = send.send(());
        result
    });
    let observation = (|| {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match receive.try_recv() {
                Ok(()) => return Ok(false),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("peer ended without completion notification".to_owned());
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if read(
                monitor,
                &format!("to_jsonb(cardinality(pg_blocking_pids({pid})) > 0)"),
            )? == json!(true)
            {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Err("peer neither completed nor acquired a visible lock wait".to_owned());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    })();
    let released = sql(holder, "COMMIT");
    let joined = thread.join().map_err(|_| "peer thread panicked".to_owned());
    released?;
    let result = joined??;
    Ok((observation?, result))
}

fn heartbeat(conn: &mut PgConnection) -> Result<bool, String> {
    let before = heartbeat_age(conn)?;
    worker::keep_alive_blocking(conn, &config(), &worker_context(), "current")
        .map_err(|e| e.to_string())?;
    // Compare the stored timestamp, not the affected-row count or elapsed wait.
    // A successful no-op must not count as a renewal.
    let after = heartbeat_age(conn)?;
    Ok(after.1 > before.1)
}

fn heartbeat_age(conn: &mut PgConnection) -> Result<(f64, f64), String> {
    let value = read(
        conn,
        "(SELECT jsonb_build_array(
        EXTRACT(EPOCH FROM(clock_timestamp()-last_seen))::double precision,
        EXTRACT(EPOCH FROM last_seen)::double precision)
        FROM apalis.workers WHERE id='worker' AND worker_type='fence-queue')",
    )?;
    let age = value[0].as_f64().ok_or("worker age is missing")?;
    let timestamp = value[1].as_f64().ok_or("worker heartbeat is missing")?;
    Ok((age, timestamp))
}

#[derive(Clone, Copy)]
enum Peer {
    Heartbeat,
    Takeover,
    Deletion,
}

#[derive(Clone, Copy)]
enum Order {
    ClaimFirst,
    PeerFirst,
    AlreadyReplaced,
}

async fn native_fence(order: Order, competing: Peer) -> Result<Outcome<Value>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(url.clone()).await?;
        with_conn(pool, move |holder| {
            worker_fixture(holder)?;
            let mut peer = connection(&url)?;
            let mut monitor = connection(&url)?;
            if matches!(order, Order::AlreadyReplaced) {
                sql(
                    holder,
                    "UPDATE apalis.workers SET lease_token='replacement'",
                )?;
                return Ok(
                    json!({"blocked":false,"accepted":guard(&mut peer)?,"token":"replacement"}),
                );
            }
            sql(holder, "BEGIN")?;
            match order {
                Order::ClaimFirst => {
                    if !guard(holder)? {
                        return Err("fixture worker did not pass the initial guard".to_owned());
                    }
                }
                Order::PeerFirst => match competing {
                    Peer::Heartbeat => {
                        if !heartbeat(holder)? {
                            return Err("production heartbeat did not advance last_seen".to_owned());
                        }
                    }
                    Peer::Takeover => sql(
                        holder,
                        "SELECT id FROM apalis.workers FOR UPDATE;
                        UPDATE apalis.workers SET lease_token='replacement'",
                    )?,
                    Peer::Deletion => sql(holder, "DELETE FROM apalis.workers WHERE id='worker'")?,
                },
                Order::AlreadyReplaced => unreachable!("handled before BEGIN"),
            }
            let (blocked, accepted) = peer_while_held(holder, peer, &mut monitor, move |conn| {
                if matches!(order, Order::PeerFirst) {
                    return guard(conn);
                }
                match competing {
                    Peer::Heartbeat => heartbeat(conn),
                    Peer::Takeover => worker::register_worker_blocking(
                        conn,
                        "fence-queue",
                        &worker_context(),
                        "regression",
                        "replacement",
                        Duration::ZERO,
                    )
                    .map(|()| true)
                    .map_err(|e| e.to_string()),
                    Peer::Deletion => sql_query("DELETE FROM apalis.workers WHERE id='worker'")
                        .execute(conn)
                        .map(|n| n == 1)
                        .map_err(|e| e.to_string()),
                }
            })?;
            let token = read(
                &mut monitor,
                "to_jsonb((SELECT lease_token FROM apalis.workers WHERE id='worker'))",
            )?;
            Ok(json!({"blocked":blocked,"accepted":accepted,"token":token}))
        })
        .await
    })
    .await
}

#[derive(Clone, Copy)]
enum Incarnation {
    Absent,
    SameFresh,
    SameStale,
    DifferentFresh,
    DifferentStale,
    LegacyFresh,
    LegacyStale,
}

fn startup_snapshot(conn: &mut PgConnection) -> Result<Value, String> {
    read(conn, "jsonb_build_object(
      'token',(SELECT lease_token FROM apalis.workers WHERE id='worker'),
      'jobs',(SELECT count(*) FROM apalis.jobs),
      'pending',(SELECT count(*) FROM apalis.jobs WHERE status='Pending' AND attempts=1 AND lock_by IS NULL AND lock_at IS NULL AND done_at IS NULL),
      'running',(SELECT count(*) FROM apalis.jobs WHERE status='Running' AND attempts=0 AND lock_by IS NOT NULL AND lock_at IS NOT NULL AND done_at IS NULL))")
}

async fn startup(
    incarnation: Incarnation,
    count: i32,
    sweep_fails: bool,
) -> Result<Outcome<Value>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(url).await?;
        with_conn(pool.clone(), move |conn| {
            worker_fixture(conn)?;
            if matches!(incarnation, Incarnation::Absent) {
                sql(conn, "UPDATE apalis.workers SET id='orphan', last_seen=clock_timestamp()-interval '1day'")?;
            } else if matches!(incarnation, Incarnation::LegacyFresh) {
                sql(conn, "UPDATE apalis.workers SET lease_token=NULL")?;
            } else if matches!(incarnation, Incarnation::LegacyStale) {
                sql(conn, "UPDATE apalis.workers SET lease_token=NULL, last_seen=clock_timestamp()-interval '1day'")?;
            } else if !matches!(incarnation, Incarnation::SameFresh | Incarnation::DifferentFresh) {
                sql(conn, "UPDATE apalis.workers SET last_seen=clock_timestamp()-interval '1day'")?;
            }
            sql_query("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,lock_by,lock_at)
                SELECT lpad(i::text,26,'0'),'fence-queue',convert_to('\"payload\"','UTF8'),'Running',0,3,clock_timestamp(),w.id,clock_timestamp()
                FROM generate_series(1,$1) i CROSS JOIN apalis.workers w")
                .bind::<diesel::sql_types::Integer,_>(count).execute(conn).map_err(|e| e.to_string())?;
            if sweep_fails {
                sql(conn, "CREATE FUNCTION public.fail_startup_sweep() RETURNS trigger LANGUAGE plpgsql AS $$
                  BEGIN RAISE EXCEPTION 'owned startup sweep failure'; END $$;
                  CREATE TRIGGER fail_startup_sweep BEFORE UPDATE ON apalis.jobs FOR EACH ROW EXECUTE FUNCTION public.fail_startup_sweep()")?;
            }
            Ok(())
        }).await?;
        let token = if matches!(incarnation, Incarnation::DifferentFresh | Incarnation::DifferentStale | Incarnation::LegacyFresh | Incarnation::LegacyStale) { "replacement" } else { "current" };
        let result = worker::initial_heartbeat(pool.clone(), config(), worker_context(), "regression", Arc::from(token)).await;
        let exact_error = matches!(&result, Err(Error::Database { source: diesel::result::Error::DatabaseError(_, info), .. })
            if info.message() == "owned startup sweep failure");
        let snapshot = with_conn(pool.clone(), startup_snapshot).await?;
        let error = result.as_ref().err().map(ToString::to_string);
        if matches!(incarnation, Incarnation::DifferentFresh | Incarnation::LegacyFresh) {
            let rejected = matches!(&result, Err(Error::AlreadyRegistered { worker_id, queue })
                if worker_id == "worker" && queue == "fence-queue");
            Ok(json!({"exact_registration_error":rejected,"snapshot":snapshot}))
        } else if sweep_fails {
            with_conn(pool.clone(), |conn| sql(conn, "DROP TRIGGER fail_startup_sweep ON apalis.jobs")).await?;
            worker::initial_heartbeat(pool.clone(), config(), worker_context(), "regression", Arc::from("retry-token"))
                .await.map_err(|e| e.to_string())?;
            let retry = with_conn(pool, startup_snapshot).await?;
            Ok(json!({"exact_error":exact_error,"before_retry":snapshot,"after_retry":retry}))
        } else {
            result.map_err(|_| format!("startup failed: {error:?}"))?;
            Ok(snapshot)
        }
    }).await
}

#[derive(Clone, Copy)]
enum RegistrationWait {
    DeadlinePasses,
    HeartbeatRefreshes,
}

async fn registration_wait(schedule: RegistrationWait) -> Result<Outcome<Value>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(url.clone()).await?;
        with_conn(pool, move |monitor| {
            worker_fixture(monitor)?;
            sql(monitor, "INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,lock_by,lock_at)
                VALUES('owned','fence-queue',''::bytea,'Running',0,3,clock_timestamp(),'worker',clock_timestamp())")?;
            if matches!(schedule, RegistrationWait::HeartbeatRefreshes) {
                sql(monitor, "UPDATE apalis.workers SET last_seen=clock_timestamp()-interval '1day'")?;
            }
            let mut holder = connection(&url)?;
            let mut peer = connection(&url)?;
            let pid = read(&mut peer, "to_jsonb(pg_backend_pid())")?.as_i64().ok_or("missing peer PID")?;
            sql(&mut holder, "BEGIN; SELECT id FROM apalis.workers WHERE id='worker' AND worker_type='fence-queue' FOR UPDATE")?;
            let before = heartbeat_age(monitor)?.0;
            let timeout = match schedule {
                RegistrationWait::DeadlinePasses => Duration::from_secs(1),
                RegistrationWait::HeartbeatRefreshes => Duration::from_secs(30),
            };
            let (send, receive) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let result = worker::register_worker_blocking(&mut peer, "fence-queue", &worker_context(), "regression", "replacement", timeout);
                let rejected = matches!(&result, Err(Error::AlreadyRegistered {worker_id,queue})
                    if worker_id == "worker" && queue == "fence-queue");
                let _ = send.send(());
                (result.is_ok(), rejected)
            });
            let observation = (|| {
                let until = Instant::now() + Duration::from_secs(5);
                loop {
                    if receive.try_recv().is_ok() {return Err("registration completed before the worker lock barrier".to_owned());}
                    if read(monitor, &format!("to_jsonb(cardinality(pg_blocking_pids({pid})) > 0)"))? == json!(true) {break;}
                    if Instant::now() >= until {return Err("registration did not reach the worker lock barrier".to_owned());}
                    std::thread::sleep(Duration::from_millis(2));
                }
                let waiting = heartbeat_age(monitor)?.0;
                match schedule {
                    RegistrationWait::DeadlinePasses => {
                        // Observe the server deadline, rather than assuming a sleep crossed it.
                        while heartbeat_age(monitor)?.0 < 1.05 {
                            if Instant::now() >= until {return Err("server deadline did not advance".to_owned());}
                            std::thread::sleep(Duration::from_millis(2));
                        }
                    }
                    RegistrationWait::HeartbeatRefreshes => {
                        if !heartbeat(&mut holder)? {return Err("production heartbeat did not refresh the locked worker".to_owned());}
                    }
                }
                let released = heartbeat_age(&mut holder)?.0;
                let timing_valid = match schedule {
                    RegistrationWait::DeadlinePasses => before < 1.0 && waiting < 1.0 && released >= 1.0,
                    RegistrationWait::HeartbeatRefreshes => before >= 30.0 && waiting >= 30.0 && released < 30.0,
                };
                Ok::<_,String>(timing_valid)
            })();
            // Always release and join our peer before observing the contract, even on error.
            let released = sql(&mut holder, "COMMIT");
            let joined = thread.join().map_err(|_| "registration peer panicked".to_owned());
            released?;
            let (accepted,rejected) = joined?;
            Ok(json!({"timing_valid":observation?,"accepted":accepted,"rejected":rejected,
                "snapshot":startup_snapshot(monitor)?}))
        }).await
    }).await
}

#[derive(Clone, Copy)]
enum Generation {
    Current,
    Downgraded,
    Reapplied,
}

fn downgrade(conn: &mut PgConnection) -> Result<(), String> {
    conn.transaction::<_, Box<dyn std::error::Error + Send + Sync>, _>(|conn| {
        conn.batch_execute("SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('apalis_diesel_postgres'),pg_catalog.hashtext('migrations'));
            SET LOCAL search_path=apalis_diesel_postgres,pg_catalog,pg_temp")?;
        // Restore generation 11, whose SQL claim still owns the SHARE fence.
        // Generation 13 adds the active-owner invariant above generation 12.
        for expected in ["20260912000001", "20260912000000"] {
            let reverted = conn.revert_last_migration(MIGRATIONS)?;
            if reverted.to_string() != expected {
                return Err(format!("expected migration {expected}, reverted {reverted}").into());
            }
        }
        Ok(())
    }).map_err(|e| e.to_string())
}

async fn compatibility(generation: Generation, ready: bool) -> Result<Outcome<Value>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(url.clone()).await?;
        if !matches!(generation, Generation::Current) {
            with_conn(pool.clone(), downgrade).await?;
        }
        if matches!(generation, Generation::Reapplied) {
            crate::setup(&pool).await.map_err(|e| e.to_string())?;
        }
        with_conn(pool, move |holder| {
            worker_fixture(holder)?;
            if ready {
                sql(holder, "INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at)
                  VALUES('01ARZ3NDEKTSV4RRFFQ69G5FAV','fence-queue',convert_to('\"payload\"','UTF8'),'Pending',0,3,clock_timestamp()-interval '1second')")?;
            }
            let peer = connection(&url)?;
            let mut monitor = connection(&url)?;
            sql(holder, "BEGIN")?;
            let returned = read(holder, "to_jsonb((SELECT count(*) FROM apalis.get_jobs('worker','fence-queue',1)))")?;
            let (blocked, renewed) = peer_while_held(holder, peer, &mut monitor, |conn| {
                heartbeat(conn)
            })?;
            let stored = read(&mut monitor, "jsonb_build_object(
              'jobs',(SELECT count(*) FROM apalis.jobs),
              'queued',(SELECT count(*) FROM apalis.jobs WHERE id='01ARZ3NDEKTSV4RRFFQ69G5FAV' AND status='Queued' AND attempts=0 AND lock_by='worker' AND lock_at IS NOT NULL AND done_at IS NULL),
              'migrations',(SELECT count(*) FROM apalis_diesel_postgres.__diesel_schema_migrations))")?;
            Ok(json!({"blocked":blocked,"renewed":renewed,"returned":returned,"stored":stored}))
        }).await
    }).await
}

fn equals(expected: Value) -> impl Fn(&Result<Outcome<Value>, String>) -> AssertionResult {
    observe("worker ownership regression", move |actual| {
        if actual == &expected {
            Ok(())
        } else {
            Err(format!("expected {expected}, observed {actual}"))
        }
    })
}

fn active_snapshot(token: &str, count: i32, fresh: bool) -> Value {
    json!({"token":token,"jobs":count,"pending":if fresh {0} else {count},"running":if fresh {count} else {0}})
}

fn compatibility_result(blocked: bool, ready: bool, migrations: i32) -> Value {
    let count = i32::from(ready);
    json!({"blocked":blocked,"renewed":true,"returned":count,"stored":{"jobs":count,"queued":count,"migrations":migrations}})
}

lets_expect! {
    #tokio_test
    expect(native_fence(order, competing).await) as native_worker_fence {
        let order = Order::ClaimFirst;
        let competing = Peer::Heartbeat;
        when the_token_is_current {
            when the_claim_owns_the_worker_fence_first {
                to allows_heartbeat_while_the_claim_holds_ownership {
                    equals(json!({"blocked":false,"accepted":true,"token":"current"}))
                }
                when another_incarnation_takes_over {
                    let competing = Peer::Takeover;
                    to delays_takeover_until_the_claim_releases_ownership {
                        equals(json!({"blocked":true,"accepted":true,"token":"replacement"}))
                    }
                }
                when the_worker_is_deleted {
                    let competing = Peer::Deletion;
                    to delays_deletion_until_the_claim_releases_ownership {
                        equals(json!({"blocked":true,"accepted":true,"token":null}))
                    }
                }
            }
            when the_peer_transaction_owns_the_row_first {
                let order = Order::PeerFirst;
                to allows_claim_during_heartbeat_renewal {
                    equals(json!({"blocked":false,"accepted":true,"token":"current"}))
                }
                when another_incarnation_takes_over {
                    let competing = Peer::Takeover;
                    to waits_then_rejects_the_previous_token {
                        equals(json!({"blocked":true,"accepted":false,"token":"replacement"}))
                    }
                }
                when the_worker_is_deleted {
                    let competing = Peer::Deletion;
                    to waits_then_rejects_the_removed_worker {
                        equals(json!({"blocked":true,"accepted":false,"token":null}))
                    }
                }
            }
        }
        when the_token_was_already_replaced {
            let order = Order::AlreadyReplaced;
            to rejects_the_previous_token_without_waiting {
                equals(json!({"blocked":false,"accepted":false,"token":"replacement"}))
            }
        }
    }

    expect(startup(incarnation, count, sweep_fails).await) as startup_recovery_boundary {
        let incarnation = Incarnation::Absent;
        let count = 1;
        let sweep_fails = false;
        when the_worker_has_no_registration {
            to recovers_an_existing_orphan_then_registers_the_new_worker {
                equals(active_snapshot("current", 1, false))
            }
            when the_global_sweep_fails {
                let sweep_fails = true;
                to reports_the_error_without_registering_and_allows_a_new_token_to_retry {
                    equals(json!({"exact_error":true,
                        "before_retry":{"token":null,"jobs":1,"pending":0,"running":1},
                        "after_retry":active_snapshot("retry-token",1,false)}))
                }
            }
        }
        when the_same_incarnation_is_fresh {
            let incarnation = Incarnation::SameFresh;
            to preserves_its_active_attempt { equals(active_snapshot("current", 1, true)) }
        }
        when the_same_incarnation_is_stale {
            let incarnation = Incarnation::SameStale;
            to recovers_its_attempt_before_renewal { equals(active_snapshot("current", count, false)) }
            when its_claims_exceed_the_global_sweep_limit {
                let count = 2001;
                to recovers_every_claim_before_renewal { equals(active_snapshot("current", count, false)) }
            }
        }
        when another_stale_incarnation_owns_the_claims {
            let incarnation = Incarnation::DifferentStale;
            to recovers_the_attempt_before_rotating_the_token { equals(active_snapshot("replacement", count, false)) }
            when its_claims_exceed_the_global_sweep_limit {
                let count = 2001;
                to recovers_every_claim_before_rotating_the_token { equals(active_snapshot("replacement", count, false)) }
            }
        }
        when the_incumbent_has_no_lease_token {
            // A token-free registration renews `last_seen` by re-registering;
            // while fresh it holds the name like a heartbeating worker.
            let incarnation = Incarnation::LegacyFresh;
            to rejects_registration_and_preserves_the_incumbent_attempt {
                equals(json!({"exact_registration_error":true,
                    "snapshot":{"token":null,"jobs":1,"pending":0,"running":1}}))
            }
            when it_is_stale {
                let incarnation = Incarnation::LegacyStale;
                to recovers_its_attempt_before_adopting_the_worker {
                    equals(active_snapshot("replacement", count, false))
                }
            }
        }
        when another_fresh_incarnation_owns_the_claims {
            let incarnation = Incarnation::DifferentFresh;
            to rejects_registration_and_preserves_the_incumbent_attempt {
                equals(json!({"exact_registration_error":true,"snapshot":active_snapshot("current",1,true)}))
            }
        }
    }

    expect(registration_wait(schedule).await) as registration_after_worker_lock_wait {
        let schedule = RegistrationWait::DeadlinePasses;
        to recovers_the_expired_claim_before_taking_over {
            equals(json!({"timing_valid":true,"accepted":true,"rejected":false,
                "snapshot":active_snapshot("replacement",1,false)}))
        }
        when the_incumbent_refreshes_its_heartbeat_before_unlock {
            let schedule = RegistrationWait::HeartbeatRefreshes;
            to rejects_takeover_and_preserves_the_refreshed_worker_claim {
                equals(json!({"timing_valid":true,"accepted":false,"rejected":true,
                    "snapshot":active_snapshot("current",1,true)}))
            }
        }
    }

    expect(compatibility(generation, ready).await) as compatibility_worker_fence {
        let generation = Generation::Current;
        let ready = false;
        when the_current_migrations_are_installed {
            to allows_heartbeat_after_an_empty_claim { equals(compatibility_result(false,ready,13)) }
            when a_task_is_ready {
                let ready = true;
                to queues_the_task_and_allows_heartbeat { equals(compatibility_result(false,ready,13)) }
            }
        }
        when the_new_migration_is_reverted {
            let generation = Generation::Downgraded;
            to restores_the_previous_fence_for_an_empty_claim { equals(compatibility_result(true,ready,11)) }
            when a_task_is_ready {
                let ready = true;
                to restores_the_previous_fence_and_preserves_queueing { equals(compatibility_result(true,ready,11)) }
            }
        }
        when the_migration_is_reapplied {
            let generation = Generation::Reapplied;
            to restores_concurrent_heartbeat_for_an_empty_claim { equals(compatibility_result(false,ready,13)) }
            when a_task_is_ready {
                let ready = true;
                to restores_concurrent_heartbeat_and_preserves_queueing { equals(compatibility_result(false,ready,13)) }
            }
        }
    }
}
