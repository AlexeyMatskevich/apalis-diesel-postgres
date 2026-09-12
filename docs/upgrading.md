# Installation and upgrades

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
whether the series committed; reconnect, rerun setup, and verify the schema before resuming workers.

### Listing index update

Migration `20260910000001_listing_id_tie_breaker` rebuilds
`jobs_list_by_queue_idx` and `jobs_list_all_idx`, adding `id DESC` as the last
ordering key. Run `setup` to apply it to existing installations. The indexes
support ordered pagination with an ID tie-breaker after the completion and
schedule timestamps, without sorting a whole timestamp tie group.

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
