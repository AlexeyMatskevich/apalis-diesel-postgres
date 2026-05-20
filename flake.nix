{
  description = "apalis-diesel-postgres development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in
      {
        devShells.default = with pkgs; mkShell {
          buildInputs = [
            openssl
            pkg-config
            git
            taplo
            rbw
            (rust-bin.beta.latest.default.override {
              extensions = [ "rust-analyzer" "rust-src" ];
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

          DATABASE_URL = "postgres://postgres:postgres@localhost:5432/apalis_diesel_postgres";
          LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

          shellHook = ''
            source ./scripts/dev-setup.sh
          '';
        };
      }
    );
}
