//! Pool fixture for tests that must never reach a PostgreSQL server.
//!
//! Shared by the library unit tests and the integration tests, so every test
//! that needs an unusable pool gets the same two guarantees.

use std::time::Duration;

use apalis_diesel_postgres::PgPool;
use diesel::{
    PgConnection,
    r2d2::{Builder, ConnectionManager, ManageConnection, Pool},
};

/// A connection string whose server refuses connections.
///
/// `gssencmode=disable` stops libpq from probing for Kerberos credentials on
/// each attempt. Kerberos builds that run library finalizers at process exit
/// (every macOS build, and MIT krb5 before 1.22 elsewhere) can abort a test
/// process that exits while another thread is still inside the library, so a
/// fixture must not load Kerberos state it does not need.
pub const UNREACHABLE_DATABASE_URL: &str = "postgres://127.0.0.1:1/not-used?gssencmode=disable";

/// A pool that opens no connection until checkout, and whose checkout fails
/// after a short timeout.
///
/// `min_idle(Some(0))` matters: with r2d2's default (`min_idle = max_size`)
/// the pool starts connecting at construction and retries every few
/// milliseconds for as long as it is alive.
/// After the first checkout, this lazy pool also retries while it remains
/// alive: r2d2 caps reconnect backoff at half this fixture's 10ms timeout.
/// Laziness delays connection work; it does not cancel it after a timeout.
pub fn unreachable_pool() -> PgPool {
    pool_with_manager(
        Pool::builder(),
        ConnectionManager::<PgConnection>::new(UNREACHABLE_DATABASE_URL),
    )
}

// Keep the fixture policy shared with the instrumented connection-manager tests.
pub(crate) fn pool_with_manager<M: ManageConnection>(builder: Builder<M>, manager: M) -> Pool<M> {
    builder
        .max_size(1)
        .min_idle(Some(0))
        .connection_timeout(Duration::from_millis(10))
        .build_unchecked(manager)
}
