use diesel::{QueryableByName, RunQueryDsl, sql_query, sql_types::Bool};

use crate::{Error, PgPool, queries::with_conn};

#[derive(QueryableByName)]
struct PopulatedRow {
    #[diesel(sql_type = Bool)]
    populated: bool,
}

/// Refresh the `apalis.queue_stats_snapshot` materialized view. Prefers
/// `CONCURRENTLY` so concurrent readers are not blocked, but falls back to a
/// blocking refresh when the view has not been populated yet — PostgreSQL
/// rejects `REFRESH ... CONCURRENTLY` on an unpopulated materialized view
/// ("CONCURRENTLY cannot be used when the materialized view is not
/// populated"), and the snapshot is created `WITH NO DATA` in migration
/// `20260521000003_queue_stats_snapshot` so the very first refresh must use
/// the blocking form.
pub(crate) async fn refresh_queue_stats_snapshot(pool: PgPool) -> Result<(), Error> {
    with_conn(pool, |conn| {
        let populated = sql_query(
            "SELECT ispopulated AS populated
             FROM pg_matviews
             WHERE schemaname = 'apalis' AND matviewname = 'queue_stats_snapshot'",
        )
        .load::<PopulatedRow>(conn)
        .map_err(Error::database("checking queue stats snapshot population"))?
        .into_iter()
        .next()
        .map(|row| row.populated)
        .unwrap_or(false);
        let stmt = if populated {
            "REFRESH MATERIALIZED VIEW CONCURRENTLY apalis.queue_stats_snapshot"
        } else {
            "REFRESH MATERIALIZED VIEW apalis.queue_stats_snapshot"
        };
        sql_query(stmt)
            .execute(conn)
            .map(|_| ())
            .map_err(Error::database("refreshing queue stats snapshot"))
    })
    .await
}
