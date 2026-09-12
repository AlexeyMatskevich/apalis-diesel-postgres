# Contributing

Use the Nix development shell before running project commands:

```sh
nix develop
```

In another terminal, start the local PostgreSQL service before running the
DB-backed test suite:

```sh
nix run .#services
```

The dev shell exports:

```sh
DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres
```

Before opening a pull request, run:

```sh
cargo fmt --all -- --check
cargo check --locked --features tokio
cargo check --locked --no-default-features --features ntex
cargo check --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
env -u DATABASE_URL cargo test --locked --features tokio --lib
env -u DATABASE_URL cargo test --locked --no-default-features --features ntex --lib
env -u DATABASE_URL cargo test --locked --all-features --lib
APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 cargo test --locked --all-features \
  --tests -- --test-threads=1
APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 cargo test --locked \
  --no-default-features --features ntex --tests -- --test-threads=1
APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 cargo test --locked --all-features \
  --tests -- --test-threads=8
cargo test --locked --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
```

`--features ntex` alone keeps the default Tokio feature. Use
`--no-default-features --features ntex` to verify ntex alone. The worker
integration suite runs real handlers and acknowledgements on each enabled
runtime; with both features it also enters ntex without a Tokio runtime.
`cargo check --locked --no-default-features` must fail with the explicit
missing-runtime diagnostic. CI checks that negative contract separately.

CI repeats the stable/both database suite with eight test threads after the
sequential run. Tests that assert exact global populations or change shared
schema objects must own a separate database; unique queue names alone do not
isolate global metrics or paginated global listings. Keep both execution modes
when changing database fixtures.

Use a disposable test cluster. Schema/global-query scenarios create and remove
their own databases; migration privilege scenarios also create, assume, and
drop their own roles. The test connection needs all of these permissions (a
superuser in the disposable cluster is suitable). Once DATABASE_URL is supplied,
connection, privilege and isolation failures fail the test. Required mode also
rejects a skipped DB scenario. Test discovery, compilation and execution are
different checks; a binary reporting zero tests does not verify a runtime.

The development shell and the password-authenticated CI matrix set
`PGGSSENCMODE=disable`. Keep this setting for tests that use neither GSS
authentication nor GSS encryption: libpq credential probing can trigger the
[native shutdown issue](README.md#operational-boundaries) while a pool is still
connecting. This does not verify GSS behavior. Outside the development shell,
set it yourself when running the database tests.

Tests must not load Kerberos on their own. A fixture that needs a pool which
never reaches a server uses `tests/support/unreachable.rs`; any other
connection string a test builds keeps the options of `DATABASE_URL`.
`scripts/check-kerberos-isolation.sh` checks this for the whole suite against a
reachable database, and CI runs it. It accepts no arguments: it requires DB
scenarios and runs all library and integration tests with both features,
serially. It compares discovered and completed test identities and rejects
empty, partial, ignored or filtered runs. Use `psql` from the same native
installation as the Rust tests so that the trace control is representative.
This diagnostic does not establish native shutdown correctness. Its process
contract can be checked without a database using
`python3 scripts/test-check-kerberos-isolation.py`.

When testing GSS, record native stderr as well as the process exit code.
A native assertion is a failure even if Rust assertions pass or the process
returns zero; require both checks for a clean native shutdown.

Run the matrix on Rust 1.88 and current stable before release. Keep `Cargo.lock`
and the native libpq/PostgreSQL packages current; review RustSec advisories with
`cargo audit --deny warnings`. A version match needs a reachability assessment,
but compatible soundness patches should not be deferred because a particular
feature is currently unused. Run `cargo package --locked` from a clean checkout
after the other CI steps to verify the consumer package and embedded migrations.
Store diagnostic logs outside the checkout, for example in `RUNNER_TEMP` in CI.
A local `--allow-dirty` build can check package contents during editing, but it
does not verify the clean-checkout requirement used by CI. Do not publish as
part of local verification.

If you only need to run the non-database unit tests, unset `DATABASE_URL`:

```sh
env -u DATABASE_URL cargo test --locked --all-features --lib
```
