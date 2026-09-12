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

        # Patch OpenSSL throughout libpq's runtime dependencies, including the
        # optional OAuth and Kerberos plugins, without updating all of nixpkgs.
        opensslVersion = "3.6.4";
        trustStorePlatform = if pkgs.stdenv.hostPlatform.isDarwin then "darwin" else "linux";
        upstreamTrustStorePatch =
          if pkgs.stdenv.hostPlatform.isDarwin then
            "use-etc-ssl-certs-darwin.patch"
          else
            "use-etc-ssl-certs.patch";
        openssl = pkgs.openssl.overrideAttrs (old:
          assert pkgs.lib.assertMsg (pkgs.lib.versionAtLeast opensslVersion old.version)
            "The OpenSSL override must not downgrade nixpkgs; update or remove it.";
          assert pkgs.lib.assertMsg
            (builtins.length (builtins.filter
              (patch: builtins.baseNameOf (toString patch) == upstreamTrustStorePatch)
              old.patches) == 1)
            "Expected exactly one OpenSSL system trust-store patch; review the nixpkgs update.";
          {
            version = opensslVersion;
            src = pkgs.fetchurl {
              url = "https://github.com/openssl/openssl/releases/download/openssl-${opensslVersion}/openssl-${opensslVersion}.tar.gz";
              hash = "sha256-m/+qGtHgezVMIb0zJOwC+hVXn0Wn0ElLPnS8RJtzM+8=";
            };
            # Upstream reformatted common.h; keep the existing certificate paths.
            patches = map (patch:
              if builtins.baseNameOf (toString patch) == upstreamTrustStorePatch then
                ./nix/patches + "/openssl-${opensslVersion}-trust-store-${trustStorePlatform}.patch"
              else patch
            ) old.patches;
          });
        postgresKrb5 = pkgs.libkrb5.override { inherit openssl; };
        postgresCurl = pkgs.curl.override {
          inherit openssl;
          libkrb5 = postgresKrb5;
          libssh2 = pkgs.libssh2.override { inherit openssl; };
          ngtcp2 = pkgs.ngtcp2.override { inherit openssl; };
        };

        # Keep the PostgreSQL minor current without changing the rest of the
        # pinned development toolchain. The source is the official release tag.
        postgres = (pkgs.postgresql_18.override {
          inherit openssl;
          libkrb5 = postgresKrb5;
          curl = postgresCurl;
        }).overrideAttrs (_: {
          version = "18.6";
          src = pkgs.fetchFromGitHub {
            owner = "postgres";
            repo = "postgres";
            rev = "refs/tags/REL_18_6";
            hash = "sha256-ySffxlG7jlNyzx++BmIN+WuaQ9TMAJt/qER9wIjd6B8=";
          };
        });

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
                package = postgres;
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
        packages.postgres = postgres;
        packages.openssl = openssl;

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

              postgres
              diesel-cli
            ];

            nativeBuildInputs = [
              clang
            ];

            DATABASE_URL = databaseUrl;
            # libpq otherwise probes for Kerberos credentials on every TCP
            # connection. Kerberos builds that run library finalizers at
            # process exit (every macOS build, and MIT krb5 before 1.22
            # elsewhere) can abort a test process that exits while a pool is
            # still connecting. Matches the CI setting.
            PGGSSENCMODE = "disable";
            # Make a PostgreSQL update visible to Cargo's native build cache.
            # pq-sys/pkg-config watches this path, unlike Nix wrapper inputs.
            PKG_CONFIG_PATH = "${postgres.dev}/lib/pkgconfig";
            LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

            shellHook = ''
              source ./scripts/dev-setup.sh
            '';
          };
      }
    );
}
