//! Server-confirmed COMMIT rejection must remain distinguishable from lost outcome.
use crate::{Config, Error, PgTaskId, PostgresStorage, test_support as support};
use apalis_core::{
    backend::{BackendExt, TaskSink, poll_strategy::IntervalStrategy},
    task::builder::TaskBuilder,
    worker::context::WorkerContext,
};
use diesel::{
    QueryableByName, RunQueryDsl, connection::SimpleConnection, sql_query, sql_types::Jsonb,
};
use futures::StreamExt;
use lets_expect::*;
use serde_json::{Value, json};
use std::time::Duration;
use support::{Outcome, observe, with_conn, with_isolated_database};

#[derive(Clone, Copy)]
enum Rejection {
    Serialization,
    Constraint,
}
#[derive(QueryableByName)]
struct Stored {
    #[diesel(sql_type=Jsonb)]
    value: Value,
}
async fn stored(pool: crate::PgPool) -> Result<Value, String> {
    with_conn(pool, |conn| sql_query("SELECT jsonb_build_object('status',status,'attempts',attempts,'owner',lock_by,'locked',lock_at IS NOT NULL,'done',done_at IS NOT NULL) AS value FROM apalis.jobs")
        .get_result::<Stored>(conn).map(|r|r.value).map_err(|e|e.to_string())).await
}
async fn commit_rejection(rejection: Rejection) -> Result<Outcome<Value>, String> {
    with_isolated_database(move |url| async move {
        let pool=crate::build_pool_with(url,|b|b.max_size(2).min_idle(Some(0))).map_err(|e|e.to_string())?;
        crate::setup(&pool).await.map_err(|e|e.to_string())?;
        let mut storage=PostgresStorage::<String>::new_with_config(&pool,&Config::new("rejected-commit"))
            .with_poll_strategy_factory(||IntervalStrategy::new(Duration::from_millis(1)));
        let id=PgTaskId::new(ulid::Ulid::new());
        storage.push_task(TaskBuilder::new("payload".to_owned()).with_task_id(id).build()).await.map_err(|e|e.to_string())?;
        with_conn(pool.clone(),move |conn| {
            let code=match rejection {Rejection::Serialization=>"40001",Rejection::Constraint=>"23514"};
            conn.batch_execute(&format!("CREATE FUNCTION public.reject_claim_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled server commit rejection' USING ERRCODE='{code}'; END $$;
                CREATE CONSTRAINT TRIGGER reject_claim_commit AFTER UPDATE ON apalis.jobs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.status='Running') EXECUTE FUNCTION public.reject_claim_commit();")).map_err(|e|e.to_string())
        }).await?;
        let worker=WorkerContext::new::<()>("rejection-worker");let mut tasks=storage.poll_compact(&worker);
        let registered=tasks.next().await.ok_or("registration stream ended")?.map_err(|e|e.to_string())?;
        if registered.is_some() {return Err("registration unexpectedly returned a claim".to_owned());}
        let failure=tokio::time::timeout(Duration::from_secs(5),tasks.next()).await.map_err(|e|e.to_string())?
            .ok_or("claim stream ended")?.err().ok_or("COMMIT was unexpectedly accepted")?;
        use diesel::result::{DatabaseErrorKind as Kind, Error as DriverError};
        let known_rejection=matches!((&failure,rejection),
            (Error::Database{source:DriverError::DatabaseError(Kind::SerializationFailure,info),..},Rejection::Serialization)
            |(Error::Database{source:DriverError::DatabaseError(Kind::CheckViolation,info),..},Rejection::Constraint)
            if info.message()=="controlled server commit rejection");
        let rejected=stored(pool.clone()).await?;
        with_conn(pool.clone(),|conn|conn.batch_execute("DROP TRIGGER reject_claim_commit ON apalis.jobs; DROP FUNCTION public.reject_claim_commit()").map_err(|e|e.to_string())).await?;
        let subsequent=tokio::time::timeout(Duration::from_secs(5),tasks.next()).await.map_err(|e|e.to_string())?;
        let same_task=matches!(subsequent,Some(Ok(Some(ref task))) if task.parts.task_id==Some(id));
        let claimed=stored(pool).await?;drop(tasks);
        Ok(json!({"known_rejection":known_rejection,"rejected":rejected,"same_task":same_task,"claimed":claimed}))
    }).await
}
fn preserves_known_outcome() -> impl Fn(&Result<Outcome<Value>, String>) -> AssertionResult {
    observe("known rejected claim commit", |actual: &Value| {
        let expected = json!({"known_rejection":true,"same_task":true,
            "rejected":{"status":"Pending","attempts":0,"owner":null,"locked":false,"done":false},
            "claimed":{"status":"Running","attempts":0,"owner":"rejection-worker","locked":true,"done":false}});
        if actual == &expected {
            Ok(())
        } else {
            Err(format!("expected {expected}, observed {actual}"))
        }
    })
}
lets_expect! { #tokio_test
    expect(commit_rejection(rejection).await) as server_rejected_claim_commit {
        let rejection=Rejection::Serialization;
        to preserves_the_server_rejection_and_allows_the_rolled_back_task_to_be_claimed {preserves_known_outcome()}
        when a_deferred_constraint_rejects_commit {
            let rejection=Rejection::Constraint;
            to preserves_the_server_rejection_and_allows_the_rolled_back_task_to_be_claimed {preserves_known_outcome()}
        }
    }
}
