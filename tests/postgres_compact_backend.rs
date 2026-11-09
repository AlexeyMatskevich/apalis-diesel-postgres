//! A storage typed on `CompactType` hands its consumer the stored bytes.
//!
//! apalis-workflow's `GraphFlow` and `SteppedFlow` run on the raw backend
//! (not behind `Durable`) and push their own encoded payloads straight
//! through the sink. The backend must not test-decode those rows as a
//! JSON-encoded `Vec<u8>`: they are not one, so every workflow task was
//! released as undecodable before it could run.
#![cfg(feature = "tokio")]
mod support;

use std::time::Duration;

use apalis_core::{backend::Backend, task::builder::TaskBuilder, worker::context::WorkerContext};
use apalis_diesel_postgres::{CompactType, Config, PostgresStorage, build_pool_with, setup};
use futures::SinkExt;

#[tokio::test]
async fn a_compact_storage_yields_raw_payloads_undecoded() {
    let outcome = support::with_isolated_database(|url| async move {
        let pool = build_pool_with(&url, |b| b.max_size(3).min_idle(Some(0)))
            .map_err(|e| e.to_string())?;
        setup(&pool).await.map_err(|e| e.to_string())?;
        let config = Config::new(&format!("compact_{}", ulid::Ulid::generate()));

        // What a workflow pushes: already-encoded bytes, sent as-is.
        let payload = br#"{"kind":"real","n":7}"#.to_vec();
        let mut producer = PostgresStorage::<CompactType>::new_with_config(&pool, &config);
        producer
            .send(TaskBuilder::new(payload.clone()).build())
            .await
            .map_err(|e| e.to_string())?;

        let mut consumer = PostgresStorage::<CompactType>::new_with_config(&pool, &config);
        let worker = WorkerContext::new("compact-worker");
        let task = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                futures::future::poll_fn(|cx| consumer.poll_ready(cx, &worker))
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(item) =
                    futures::future::poll_fn(|cx| consumer.poll_next(cx, &worker)).await
                {
                    return item.map_err(|e| e.to_string());
                }
            }
        })
        .await
        .map_err(|_| "no task within 10s".to_string())??;

        Ok::<_, String>(task.args)
    })
    .await
    .unwrap();

    if let support::Outcome::Completed(args) = outcome {
        assert_eq!(args, br#"{"kind":"real","n":7}"#.to_vec());
    }
}
