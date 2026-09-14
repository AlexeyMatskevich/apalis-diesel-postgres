//! Public read/claim boundaries and compatibility ownership regressions.
#![cfg(feature = "tokio")]
mod support;
use apalis_core::{
    backend::{BackendExt, FetchById, Filter, ListAllTasks, ListTasks},
    task::{attempt::Attempt, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use apalis_diesel_postgres::{
    Config, Error, PgAck, PgPool, PgTaskId, PostgresStorage, build_pool_with, setup,
};
use diesel::{
    QueryableByName, RunQueryDsl,
    connection::SimpleConnection,
    sql_query,
    sql_types::{Integer, Nullable, Text},
};
use futures::StreamExt;
use lets_expect::*;
use serde_json::{Value, json};

#[derive(Clone, Copy)]
enum Read {
    ById,
    QueueList,
    GlobalList,
    Claim,
}
#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type=diesel::sql_types::Jsonb)]
    value: Value,
}
#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type=Integer)]
    count: i32,
}
async fn row(pool: PgPool) -> Result<Value, String> {
    support::with_conn(pool, |c| {
        sql_query("SELECT to_jsonb(j) AS value FROM apalis.jobs j")
            .get_result::<JsonRow>(c)
            .map(|r| r.value)
            .map_err(|e| e.to_string())
    })
    .await
}
/// The token-free worker row is fresh for manual acknowledgements. A worker
/// stream registers the name itself, so the row is inserted stale for it:
/// a fresh token-free registration would refuse the stream.
async fn fixture(
    url: String,
    status: &str,
    stream_registers: bool,
) -> Result<(PgPool, PostgresStorage<String>), String> {
    let pool =
        build_pool_with(url, |b| b.max_size(2).min_idle(Some(0))).map_err(|e| e.to_string())?;
    setup(&pool).await.map_err(|e| e.to_string())?;
    let status = status.to_owned();
    let last_seen = if stream_registers {
        "clock_timestamp()-interval '1 day'"
    } else {
        "clock_timestamp()"
    };
    support::with_conn(pool.clone(),move|c|{
 c.batch_execute(&format!("INSERT INTO apalis.workers(id,worker_type,storage_name,layers,last_seen,started_at) VALUES('schema-worker','schema-queue','fixture','',{last_seen},clock_timestamp())")).map_err(|e|e.to_string())?;
 sql_query("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,lock_by,lock_at) VALUES('01ARZ3NDEKTSV4RRFFQ69G5FAV','schema-queue',convert_to('\"payload\"','UTF8'),$1,2,10,clock_timestamp()-interval '1second',CASE WHEN $1='Pending' THEN NULL ELSE 'schema-worker' END,CASE WHEN $1='Pending' THEN NULL ELSE date_trunc('second',clock_timestamp()) END)").bind::<Text,_>(status).execute(c).map(|_|()).map_err(|e|e.to_string())
 }).await?;
    Ok((
        pool.clone(),
        PostgresStorage::new_with_config(&pool, &Config::new("schema-queue")),
    ))
}
async fn read_ack(source: Read, coherent: bool) -> Result<support::Outcome<Value>, String> {
    support::with_isolated_database(move|url|async move{
 let (pool,mut storage)=fixture(url,if matches!(source,Read::Claim){"Failed"}else{"Running"},matches!(source,Read::Claim)).await?;
 let id=PgTaskId::new(ulid::Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap());
 let filter=Filter{status:Some(Status::Running),page:1,page_size:Some(1)};
 let mut held_stream=None;
 let mut parts=match source {
 Read::ById=>storage.fetch_by_id(&id).await.map_err(|e|e.to_string())?.ok_or("missing job")?.parts,
 Read::QueueList=>storage.list_tasks(&filter).await.map_err(|e|e.to_string())?.remove(0).parts,
 Read::GlobalList=>storage.list_all_tasks(&filter).await.map_err(|e|e.to_string())?.remove(0).parts,
 Read::Claim=>{
 let worker=WorkerContext::new::<()>("schema-worker"); let mut stream=storage.poll_compact(&worker);
 let task=tokio::time::timeout(std::time::Duration::from_secs(5),async{loop{match stream.next().await{Some(Ok(Some(task)))=>break Ok(task),Some(Ok(None))=>{},Some(Err(e))=>break Err(e.to_string()),None=>break Err("stream ended".into())}}}).await.map_err(|e|e.to_string())??;
 held_stream=Some(stream);task.parts
 }};
 if !matches!(source,Read::Claim){parts.attempt=Attempt::new_with_value(if coherent{3}else{4});}
 let before=row(pool.clone()).await?;
 let result:Result<(),apalis_core::error::BoxDynError>=Ok(());
 let actual=PgAck::new(&pool).ack(&result,&parts).await;
 let after=row(pool).await?;
 let observation=json!({"ok":actual.is_ok(),"stale":matches!(actual,Err(Error::StaleAcknowledgement{..})),"before":before,"after":after});

 drop(held_stream);Ok(observation)
 }).await
}
fn read_contract(
    coherent: bool,
) -> impl Fn(&Result<support::Outcome<Value>, String>) -> AssertionResult {
    support::observe(
        "admin reads do not manufacture claim history",
        move |r: &Value| {
            let ok = if coherent {
                r["ok"] == true
                    && r["after"]["attempts"] == 3
                    && r["after"]["status"] == "Done"
                    && r["after"]["last_result"] == json!({"Ok": null})
                    && r["after"]["done_at"].is_string()
                    && r["after"]["lock_by"] == r["before"]["lock_by"]
                    && r["after"]["lock_at"] == r["before"]["lock_at"]
            } else {
                r["stale"] == true && r["after"] == r["before"]
            };
            if ok {
                Ok(())
            } else {
                Err(format!("coherent={coherent}, observed {r}"))
            }
        },
    )
}
#[derive(Clone, Copy)]
enum Worker {
    Registered,
    Missing,
    Null,
}
async fn compatibility(worker: Worker) -> Result<support::Outcome<Value>, String> {
    support::with_isolated_database(move|url|async move{
 let (pool,_)=fixture(url,"Pending",false).await?;
 let arg=match worker{Worker::Registered=>Some("schema-worker"),Worker::Missing=>Some("missing-worker"),Worker::Null=>None};
 let before=row(pool.clone()).await?;
 let result=support::with_conn(pool.clone(),move|c|sql_query("SELECT count(*)::integer AS count FROM apalis.get_jobs($1,'schema-queue',1)").bind::<Nullable<Text>,_>(arg).get_result::<Count>(c).map(|r|r.count).map_err(|e|e.to_string())).await;
 let after=row(pool).await?;
 let observation=json!({"returned":result.as_ref().ok(),"error":result.err(),"null_worker":matches!(worker,Worker::Null),"before":before,"after":after});Ok(observation)
 }).await
}
fn compatibility_contract(
    registered: bool,
) -> impl Fn(&Result<support::Outcome<Value>, String>) -> AssertionResult {
    support::observe("compatibility claim requires an owner", move |r: &Value| {
        let ok = if registered {
            r["returned"] == 1
                && r["after"]["status"] == "Queued"
                && r["after"]["lock_by"] == "schema-worker"
        } else {
            r["error"].is_string()
                && (!r["null_worker"].as_bool().unwrap_or(false)
                    || r["error"]
                        .as_str()
                        .is_some_and(|error| error.contains("worker_id must not be null")))
                && r["after"] == r["before"]
        };
        if ok {
            Ok(())
        } else {
            Err(format!("registered={registered}: {r}"))
        }
    })
}
lets_expect! {
 #tokio_test
 expect(read_ack(source,coherent).await) as admin_read_acknowledgement {
 let source=Read::ById; let coherent=true;
 to retains_the_explicit_completed_count {read_contract(coherent)}
 when the_supplied_count_does_not_match_the_running_attempt{let coherent=false;to rejects_the_stale_count_without_writing{read_contract(coherent)}}
 when reading_a_queue_page{let source=Read::QueueList;
 to retains_the_explicit_completed_count {read_contract(coherent)}
 when the_supplied_count_does_not_match_the_running_attempt{let coherent=false;to rejects_the_stale_count_without_writing{read_contract(coherent)}}}
 when reading_a_global_page{let source=Read::GlobalList;
 to retains_the_explicit_completed_count {read_contract(coherent)}
 when the_supplied_count_does_not_match_the_running_attempt{let coherent=false;to rejects_the_stale_count_without_writing{read_contract(coherent)}}}
 }
 expect(read_ack(Read::Claim,true).await) as sql_claim_acknowledgement {to counts_one_execution_without_a_tracker{read_contract(true)}}
 expect(compatibility(worker).await) as compatibility_claim_owner {
 let worker=Worker::Registered;let registered=true;
 to queues_a_job_for_the_registered_owner{compatibility_contract(registered)}
 when the_worker_does_not_exist{let worker=Worker::Missing;let registered=false;to rejects_without_altering_the_job{compatibility_contract(registered)}}
 when the_worker_id_is_null{let worker=Worker::Null;let registered=false;to rejects_without_altering_the_job{compatibility_contract(registered)}}
 }
}
