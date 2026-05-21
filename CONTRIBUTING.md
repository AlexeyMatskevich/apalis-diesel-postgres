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
cargo check --locked --no-default-features
cargo check --locked --features tokio
cargo check --locked --features ntex
cargo check --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
env -u DATABASE_URL cargo test --locked --no-default-features --lib
env -u DATABASE_URL cargo test --locked --features tokio --lib
env -u DATABASE_URL cargo test --locked --features ntex --lib
env -u DATABASE_URL cargo test --locked --all-features --lib
APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 cargo test --locked --all-features \
  --test postgres_lifecycle \
  --test postgres_notify_shared \
  --test postgres_queries \
  -- --test-threads=1
cargo test --locked --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
```

If you only need to run the non-database unit tests, unset `DATABASE_URL`:

```sh
env -u DATABASE_URL cargo test --locked --all-features --lib
```
