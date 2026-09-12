# Installation and upgrades

## Upgrading from 0.4.1 to Unreleased

Use this path when moving from the `v0.4.1` tag to the changes under
`Unreleased` in [CHANGELOG](../CHANGELOG.md#unreleased). It covers both the Rust
application and its existing database. The target changes are not yet a new
release: `Cargo.toml` still says `0.4.1`. A registry dependency on `0.4.1`, or the
`v0.4.1` Git tag, does not include them. For pre-release validation, use a path
dependency on the updated checkout; select the actual new version when released.

### 1. Prepare and build the application

Keep Rust 1.88 or later and the existing Apalis RC pins. The minimum dependency
requirements are now Diesel 2.3.10 and, for ntex, ntex-rt 3.15. Update the
application's lockfile as needed and test its selected runtime. Default Tokio,
explicit ntex-only, and the requirement to select a runtime are unchanged.
For ntex-only dependencies, retain `default-features = false, features = ["ntex"]`.
The PostgreSQL prerequisite and tested-version boundary are stated below.

| Application usage in 0.4.1 | Required change or review |
|---|---|
| `PgAck` / `PgMiddleware` constructors, including `with_lease_token`, or `SharedPostgresStorage::new` receive `pool` or `pool.clone()` | Pass `&pool`. Ordinary `PostgresStorage::{new,new_with_config,new_with_notify}` already borrowed the pool in 0.4.1. |
| `lock_task` / `lock_task_in_queue` receive `&Ulid` | Pass `&PgTaskId`. Enqueue already returns this type; wrap an independently stored ULID with `PgTaskId::new(raw_id)`. |
| `Error::AlreadyRegistered(worker_id)` matches or construction | Use `Error::AlreadyRegistered { worker_id, queue }`. Keep the wildcard arm required by the already non-exhaustive `Error` enum. |
| Explicit public `PgSink` imports or type annotations | Use `PostgresStorage`, which implements `Sink`; `PgSink` is now internal. |
| Explicit automatic middleware service type | Replace `AcknowledgeService<LockTaskService<S>, PgAck>` with `LockTaskService<AcknowledgeService<S, PgAck>>`, or use the layer's associated `Service` type. The manual branch remains `LockTaskService<S>`. |
| Several workers reuse a `Config`, or restart by cloning a storage | Supply a poll strategy factory for each stream; construct fresh storage on restart. See the example below and [recovery details](#scheduling-and-cancellation). |
| Code continues using a Sink after a failed flush | Reconcile any unknown write outcome, then retry through a fresh sink with retained IDs/keys. The failed pipeline now returns `SinkFailed`. See [buffered enqueue](../README.md#buffered-enqueue-completion). |
| Direct middleware calls manually adjust attempts, or manual ack rebuilds `Parts` | Review the [acknowledgement contract](#direct-middleware-acknowledgement). Automatic ack counts the claimed execution itself; manually assembled Parts without the claim snapshot still require an explicit inclusive attempt count. |

For example, the constructor calls become:

```rust
use apalis_diesel_postgres::{PgAck, PgMiddleware, PgPool, SharedPostgresStorage};

fn components(pool: &PgPool) -> (PgAck, PgMiddleware, SharedPostgresStorage) {
    (
        PgAck::new(pool),
        PgMiddleware::new(pool, true),
        SharedPostgresStorage::new(pool),
    )
}
```

Create the polling strategy inside the factory, rather than cloning a previously
consumed strategy. Choose the interval appropriate for the application:

```rust
use std::time::Duration;
use apalis_core::backend::poll_strategy::IntervalStrategy;
use apalis_diesel_postgres::{Config, PgPool, PostgresStorage};

fn storage(pool: &PgPool) -> PostgresStorage<String> {
    PostgresStorage::new_with_config(pool, &Config::new("emails"))
        .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_secs(1)))
}
```

`PostgresStorage::new` already provides a factory for its default strategy.
The custom-factory guidance also applies to notification and shared storage.
Handling `PollStrategyExhausted`, `WorkerRetired`, or `ClaimOutcomeUnknown` by
cloning the same storage is not a restart procedure.

Check enqueue inputs against the new 1 MiB encoded task-payload limit and the
stricter scheduling validation. Enqueue idempotency does not deduplicate an
external effect performed by a handler; keep that effect independently idempotent.

### 2. Rehearse the cutover on a restored database

Use a copy of the existing database and the exact application build intended for
deployment. Apply the sequence below, then test enqueue, execution, retry,
acknowledgement and restart using the application's real codecs and handlers.
`verify_schema` checks the backend's required catalog, not application behavior
or every stored task. Custom DDL and legacy-schema cases are covered in
[supported existing schemas](#supported-existing-schemas).

### 3. Stop the old actors and retain a recovery point

Pause producers and schedulers, stop old workers, and let their handlers and
already-submitted SQL finish. Dropping an async waiter alone does not stop SQL.
Retain a consistent backup of the application data, queue data and schema before
the upgrade, plus the old application build. Record job counts by queue and state
and identify any unfinished active jobs for post-upgrade review.

Keep these actors stopped during migration. Old and new workers do not have a
supported mixed-version rolling-upgrade protocol for this transition. Index
rebuilds and constraint validation can block reads and writes; reserve the
[maintenance window](#schema-installation-and-upgrades) before starting setup.

### 4. Run setup with the new build

Use a migration-owner connection pool and call this from the selected async
runtime, before starting the new workers:

```rust
use apalis_diesel_postgres::{Error, PgPool, setup, verify_schema};

async fn migrate(pool: &PgPool) -> Result<(), Error> {
    setup(pool).await?;
    verify_schema(pool).await
}
```

For the complete 0.4.1 schema, setup adopts eight known versions into its private
journal and executes the five additional embedded migrations. The resulting
current series has thirteen versions. Schema recognition determines this path;
changing the application's crate version alone does not migrate the database.

Do not copy or move `public.__diesel_schema_migrations`, manually insert version
stamps, or run the old raw `run_pending_migrations(MIGRATIONS)` bootstrap against
an existing unadopted schema. `setup` performs adoption while preserving the
application's public journal. If setup refuses the schema, inspect the reported
objects and use the supported-schema guidance instead of forcing version stamps.

### 5. Verify and resume

Require successful `setup` and `verify_schema` before starting the new workers.
Compare the queue/state snapshot with the expected ownership repairs described
below: lost active work may become `Pending` or `Killed` according to its retry
budget. Review those jobs; do not reset every attempt counter to make counts match.

Start workers with fresh storage and fresh configuration or a strategy factory.
Reusing a worker name can be refused until its previous database registration
becomes stale. Let the configured recovery interval expire rather than forcing
the registration stale while old handlers or SQL could still be active. Confirm
a controlled application task reaches its expected final state, then resume
normal producers and schedulers. Retain polling fallback for notifications.

### 6. Distinguish retrying setup from rolling back the deployment

A failure before migration commit rolls back the complete pending series; fix
the cause and rerun setup. If the connection or COMMIT response was lost, or only
the async waiter was cancelled, do not infer rollback from that client outcome.
Let the original SQL finish, then rerun setup with the new build and verify the
schema to establish the resulting state.

After a committed upgrade, restarting the old binary or blindly running every
down migration is not a supported rollback. Ownership repairs can consume attempts
and change job history; down SQL cannot reconstruct that history. Use a separately
validated downgrade or restore the consistent pre-upgrade recovery point together
with the old application, accounting for any writes and external effects since
the backup. The individual down-migration limits are documented below.

## Schema installation and upgrades

The backend's SQL requires PostgreSQL 14 or later; CI currently tests PostgreSQL
18. PostgreSQL 12/13 cannot execute the metrics queries. PostgreSQL 14–17 are not
validated by the current CI matrix. The lower SQL prerequisite was previously
undocumented; this is not evidence that all behavior on those versions is tested.

Run `apalis_diesel_postgres::setup(&pool).await?` as the migration owner before
starting workers. Back up existing data and stop old workers for an upgrade that
changes worker identity or recovers legacy ownership. Mixed old/new workers
are not a supported rolling-upgrade protocol during these changes.

The private journal is now
`apalis_diesel_postgres.__diesel_schema_migrations`; runtime tables stay in
`apalis`. An application's `public.__diesel_schema_migrations` is never read or
modified. Both schemas must be writable only by trusted migration owners. The
journal is an operational record, not protection against a schema administrator.

Creating the private schema requires `CREATE` on the database. If the schema
already exists, `setup` checks for it under the migration lock and does not issue
`CREATE SCHEMA`; the migration role still needs the privileges required for its
journal and any pending DDL. A repeat setup of a complete installation therefore
does not require database-wide `CREATE` solely to reuse its private schema.

The complete migration series runs in one transaction under the same advisory
lock key as earlier versions. Nested Diesel transactions become savepoints.
Commit, rollback, connection loss, and disposal of a panicking pooled connection
release that transaction lock; the original `search_path` is restored. Setup
does not release session locks acquired by unrelated code. Finish or terminate
an older migration session before upgrading: cancelling its async waiter does
not stop its SQL. Future nontransactional migration metadata is rejected by setup.

DDL locks remain held until the whole series commits. Index builds and constraint
validation on large tables require a maintenance window and sufficient disk/WAL
capacity. A failure before commit rolls back the entire new series; resolve its
cause and retry. A lost COMMIT response or cancelled waiter does not establish
whether the series committed; reconcile it as described in the cutover sequence.

### Listing index update

Migration `20260910000001_listing_id_tie_breaker` rebuilds
`jobs_list_by_queue_idx` and `jobs_list_all_idx`, adding `id DESC` as the last
ordering key. Run `setup` to apply it to existing installations. The listing API's
stable order stays the same; small pages no longer require sorting a whole batch
whose completion and schedule timestamps tie when the optimizer uses these indexes.

This is a transactional maintenance migration. `DROP INDEX` takes an
`ACCESS EXCLUSIVE` lock on `apalis.jobs`, blocking both reads and writes until
the complete setup transaction commits. Both indexes are built over the full
table. Schedule downtime and allow disk/WAL headroom; the added key increases
index size, especially where repeated timestamps previously allowed B-tree
deduplication. A failure rolls back both index replacements and their journal
entry, so the same setup can be retried. The down migration restores the prior
index definitions, which do not cover the ID tie-breaker.

### Worker lock update

Migration `20260912000000_worker_key_share` replaces the worker-row guard in
`apalis.get_jobs` with `FOR KEY SHARE`, matching the native Rust claim paths.
Heartbeat updates no longer conflict with that guard; supported identity takeover
and deletion still acquire `FOR UPDATE` and remain fenced. Run `setup` even if
the preceding eleven migrations were applied through an external Diesel harness.
The down migration restores the previous `FOR SHARE` guard. This changes function
locking behavior, not its signature or the meaning of a claimed job.

### Active ownership update

Migration `20260912000001_require_active_owner` adds the invariant that every
`Queued` or `Running` row has a non-null `lock_by`. Before validating the new
constraint, it repairs active rows without owners: one lost attempt is consumed
within the retry budget, and the row becomes `Pending` or `Killed`. Valid owners
and terminal history are preserved. The compatibility function `apalis.get_jobs`
now rejects a null worker argument explicitly with SQLSTATE `22004`.

Run `setup` to apply the migration to existing installations. Constraint
validation scans `jobs`, and the DDL lock lasts until the complete setup
transaction commits; plan a maintenance window for large tables. A rollback
restores the pre-upgrade state. The down migration removes the new constraint
and function guard, but cannot reconstruct ownership or history repaired by a
previously committed upgrade.

## Direct middleware acknowledgement

`PgMiddleware` now uses the attempt history returned by each SQL claim for
automatic acknowledgement, whether invoked by an Apalis worker or as a direct
Tower service. A fallback claim refreshes the input counter from the database;
callers do not need to pre-increment it. The handler's counter starts with the
previous completed count. Apalis's Tracker increments it on the first poll;
a direct service without Tracker leaves that observable counter unchanged.
Acknowledgement records exactly one additional completed attempt in either case.

The claim snapshot stays in `Parts.data` across clones. Preserve the original
Parts for explicit `PgAck` calls and retries. Serialization or rebuilding Parts
without their extensions drops this in-memory snapshot. Such manually assembled
acknowledgements retain the existing contract: provide an `Attempt` that includes
the execution being acknowledged. Administrative reads, including `fetch_by_id`
and task listings, do not create a claim snapshot; reading a row does not prove
a new execution. Their Parts therefore follow that explicit-attempt contract.
This acknowledgement change requires no schema migration.

## Supported existing schemas

- Empty database: apply all embedded migrations.
- Complete `0.4.1` schema from [tag `v0.4.1`](https://github.com/AlexeyMatskevich/apalis-diesel-postgres/tree/v0.4.1)
  (commit `8181c4391b8828029c0e1e6e04439f74a671786c`): check its required catalog
  structure, record the eight known migration versions privately, then apply new migrations.
  Historical hardening is not replayed. External composite worker foreign keys
  are preserved.
- Complete catalog through `20260910000001_listing_id_tie_breaker`, including an
  installation made by the formerly supported raw public Diesel harness: validate
  its required structure and adopt the fixed eleven known versions privately.
  Preserve application data and any public journal, then execute later migrations.
  This also works without a public journal. Even if a raw harness already applied
  `20260912000000_worker_key_share` or
  `20260912000001_require_active_owner`, setup adopts only the eleven-version
  generation and safely reapplies both later, idempotent migrations. A missing
  active-owner constraint without a journal is structurally indistinguishable
  from the supported previous generation; adoption does not prove provenance.
  An existing incorrect or unvalidated constraint is rejected. With a current
  private journal, a missing constraint is also rejected.
- That crate's initial schema, or the bytea/JSONB schema of
  `apalis-postgres 1.0.0-rc.8` after all 19 upstream migrations: run the guarded
  legacy transition. Older upstream JSONB job/`last_error` generations must first
  be upgraded through upstream migrations.
- Unknown partial schema or incomplete current generation: refuse with the
  mismatched objects. Restore a supported generation or perform an explicit
  reviewed migration. Copying version rows cannot repair schema damage.

Legacy worker uniqueness changes from `id` to `(id, worker_type)`. External
foreign keys requiring the old id-only key prevent this conversion. Setup fails
without dropping those dependencies or changing data. Decide how the application
will identify queue-specific registrations, migrate its FK explicitly, and retry.
There is no `CASCADE` on worker key conversion.

Legacy jobs with no valid owner in their queue are repaired by state. Lost
`Running`/`Queued` executions consume one remaining attempt: budget remaining
means `Pending`; exhaustion means `Killed`, an error result, and completion time.
`Done`, `Killed`, and exhausted `Failed` retain terminal state and history.
Non-active states do not consume another attempt. Invalid ownership fields are
cleared; valid ownership is retained. Exhausted `Pending` stays unclaimable.
A trusted administrator handling such a row must either restore a valid retry
budget before rescheduling it or choose the appropriate terminal state.

Damage already caused by an older migration cannot be reconstructed reliably.
Recover deleted registrations, reanimated terminal jobs, or dropped application
foreign keys from backups and application records as appropriate.

Downgrading the worker key refuses when an id occurs in multiple queues, instead
of deleting registrations. Resolve the registrations explicitly before retrying.
Reverting the original create-schema migration is an intentional destructive
uninstall: review down SQL and retain a backup.

## Out-of-band runners and verification

Prefer the public `setup` in a migration executable. It performs schema recognition
and history adoption; applications can then call `verify_schema` on boot.

A custom Diesel harness may use `MIGRATIONS` for a fresh database or a database
whose private journal setup already initialized. Inside one Diesel transaction,
execute the following SQL, then `conn.run_pending_migrations(MIGRATIONS)`, and
commit only on success:

```sql
SELECT pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('apalis_diesel_postgres'), pg_catalog.hashtext('migrations'));
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace
                   WHERE nspname = 'apalis_diesel_postgres') THEN
        CREATE SCHEMA apalis_diesel_postgres;
    END IF;
END $$;
SET LOCAL search_path = apalis_diesel_postgres, pg_catalog, pg_temp;
```

The explicit last position of `pg_temp` prevents a temporary journal from
shadowing the private one. Do not run the raw harness against an existing schema
with no private journal: use setup once for validated adoption. Do not add
nontransactional migrations to this transaction.

`verify_schema` checks private versions and required table/view columns, types,
nullability, primary/foreign keys, check constraints, indexes, trigger registration,
and function signatures/security/search paths. Stamps alone do not pass. It does
not checksum arbitrary function/view bodies, prove privileges for every caller,
or validate every stored job. Control out-of-band DDL and retain behavior tests.
Catalog recognition does not prove which runner created the schema. In particular,
adoption does not infer that future data or function-body migrations ran merely
because their signatures match. The fixed known generation is adopted; subsequent
migrations execute normally, including replacement of the `get_jobs` body.

## Scheduling and cancellation

Enqueue returns `Error::InvalidArgument` for a schedule outside the representable
UTC timestamp range, currently through 8,210,266,876,799 seconds after the Unix
epoch. Earlier versions silently converted some larger values to the epoch.
A rejected mixed batch writes no jobs; the outbox caller's outer transaction
remains usable.

Cancellation does not imply SQL rollback. A blocking operation may commit after
its waiter disappears. Resolve unknown enqueue outcomes with stable identities
or idempotency keys; independently make external handler effects idempotent.

Automatic acknowledgement failure and an uncertain claim commit retire the local
worker registration. With Apalis `Worker::run` or a monitored worker, its terminal
stream error also cancels that worker's other pending service futures. This
fail-stop policy permits orphan recovery after the last committed heartbeat; it
does not provide a graceful drain of healthy siblings. Blocking SQL already
submitted can still finish, and external effects may repeat after recovery.

Restart factories must construct fresh storage and either fresh `Config` or a
poll strategy factory. A storage clone retains the retired local registration;
a cloned, consumed strategy does not become reusable. A fresh storage token may
be refused until the old database registration becomes stale. Do not force it
stale while prior handlers or SQL can still be active. Low-level `lock_task` and
`lock_task_in_queue` callers must resolve `ClaimOutcomeUnknown` or stop their own
heartbeat and allow orphan recovery; these APIs do not manage local retirement.
