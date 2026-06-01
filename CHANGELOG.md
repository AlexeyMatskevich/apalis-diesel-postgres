# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While
the crate is pre-1.0, a minor version bump may carry breaking changes.

## [0.3.0]

### Changed (breaking)

- Idempotency-key conflicts on enqueue now return a dedicated, typed error
  variant instead of the stringly-typed `Error::InvalidArgument(
  "idempotency_key conflict: …")`. The new variant is:

  ```rust
  Error::IdempotencyConflict { job_type: String, conflicting_keys: Vec<String>, total: usize }
  ```

  `conflicting_keys` lists exactly which keys collided, so a batch caller can
  drop them and re-enqueue the rest.

  Match the variant to tell a benign duplicate apart from a real failure,
  rather than matching the message text (which could change in any release):

  ```rust
  match storage.push_task_with_conn(conn, task) {
      Ok(id) => { /* enqueued */ }
      Err(Error::IdempotencyConflict { .. }) => { /* duplicate — swallow it */ }
      Err(other) => return Err(other),
  }
  ```

  Storage behavior is unchanged: the conflict still rolls back the whole
  enqueue batch via SAVEPOINT — one duplicate undoes *every* row in the
  batch, not just the colliding one — while a surrounding transaction stays
  alive so business writes can still commit. Every other
  `Error::InvalidArgument` case (queue-name / metadata / idempotency-key
  length caps, unreachable `run_at`) is unchanged.

### Notes

- `Error` is `#[non_exhaustive]`, so future variants are not a breaking change
  for downstreams that already include a wildcard match arm.

## [0.2.0] and earlier

See the git history for changes before this changelog was introduced.
