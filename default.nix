# gate as a Nix package: `pkgs.callPackage "${gate}/default.nix" {}`.
#
# Needs Rust 1.92 or newer (see `rust-version` in Cargo.toml), so nixos-25.05's
# rustPlatform is too old; use one from nixos-unstable or a newer release.
{
  lib,
  rustPlatform,
  pkg-config,
  aws-lc,
}: let
  cargoToml = lib.importTOML ./Cargo.toml;
  # The executable is named after the package, unless Cargo.toml names it with [[bin]]
  binary = (builtins.head (cargoToml.bin or [{inherit (cargoToml.package) name;}])).name;
in
  rustPlatform.buildRustPackage {
    pname = cargoToml.package.name;
    inherit (cargoToml.package) version;

    src = lib.fileset.toSource {
      root = ./.;
      fileset = lib.fileset.unions [
        ./Cargo.toml
        ./Cargo.lock
        ./src
      ];
    };

    cargoLock.lockFile = ./Cargo.lock;

    # nixpkgs' cargo setup hook sets AWS_LC_SYS_USE_SYSTEM=1, so aws-lc-sys links
    # nixpkgs' AWS-LC instead of building its own
    nativeBuildInputs = [pkg-config];
    buildInputs = [aws-lc];

    # The tests start servers and child processes, and live in tests/, which isn't part of
    # src. CI runs them on every pull request.
    doCheck = false;

    meta = {
      description = "MCP gateway: one MCP server that searches, inspects and calls the tools of many others";
      homepage = "https://github.com/tembo/gate";
      license = lib.licenses.mit;
      mainProgram = binary;
      platforms = lib.platforms.linux ++ lib.platforms.darwin;
    };
  }
