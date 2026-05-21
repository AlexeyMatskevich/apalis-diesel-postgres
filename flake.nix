{
  description = "apalis-diesel-postgres development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    services-flake.url = "github:juspay/services-flake";
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      process-compose-flake,
      services-flake,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Local PostgreSQL connection details, shared by the dev shell and the
        # process-compose service below.
        dbName = "apalis_diesel_postgres";
        dbPort = 5432;
        databaseUrl = "postgres://127.0.0.1:${toString dbPort}/${dbName}";

        # `nix run .#services` starts a project-local PostgreSQL cluster with
        # its data directory in ./.pgdata (gitignored).
        services = (import process-compose-flake.lib { inherit pkgs; }).makeProcessCompose {
          modules = [
            services-flake.processComposeModules.default
            {
              services.postgres."pg" = {
                enable = true;
                package = pkgs.postgresql_18;
                dataDir = "./.pgdata";
                listen_addresses = "127.0.0.1";
                port = dbPort;
                initialDatabases = [ { name = dbName; } ];
              };
            }
          ];
        };
      in
      {
        packages.services = services;

        devShells.default =
          with pkgs;
          mkShell {
            buildInputs = [
              openssl
              pkg-config
              git
              taplo
              rbw
              (rust-bin.stable."1.88.0".default.override {
                extensions = [
                  "clippy"
                  "llvm-tools-preview"
                  "rust-analyzer"
                  "rust-src"
                  "rustfmt"
                ];
              })

              clang

              nixd
              nodejs_22
              bun

              postgresql_18
              diesel-cli
            ];

            nativeBuildInputs = [
              clang
            ];

            DATABASE_URL = databaseUrl;
            LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

            shellHook = ''
              source ./scripts/dev-setup.sh
            '';
          };
      }
    );
}
