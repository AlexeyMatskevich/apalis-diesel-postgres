# apalis-diesel-postgres

PostgreSQL storage backend for [Apalis](https://github.com/apalis-dev/apalis)
implemented with Diesel.

This repository is currently initialized as a Rust library crate with the same
local development runtime style as `leptos_ntex`.

## Development

Enter the development shell:

```sh
nix develop
```

Run the baseline checks:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```
