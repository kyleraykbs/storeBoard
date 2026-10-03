{
  description = "Storeboard — replicated folders across your own devices";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs =
    inputs@{ self, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      flake = {
        nixosModules = {
          storeboardd =
            {
              config,
              lib,
              pkgs,
              ...
            }:
            import ./nix/storeboardd.nix {
              inherit config lib pkgs;
              defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.storeboard;
            };
          storeboard =
            {
              config,
              lib,
              pkgs,
              ...
            }:
            import ./nix/storeboard-nixos.nix {
              inherit config lib pkgs;
              defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.storeboard;
            };
          default = self.nixosModules.storeboardd;
        };

        homeManagerModules = {
          storeboard =
            {
              config,
              lib,
              pkgs,
              ...
            }:
            import ./nix/storeboard-hm.nix {
              inherit config lib pkgs;
              defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.storeboard;
            };
          default = self.homeManagerModules.storeboard;
        };
      };

      perSystem =
        {
          pkgs,
          lib,
          self',
          ...
        }:
        let
          version = "0.1.0";

          # One derivation, both binaries: nothing to wrap (the daemon speaks
          # its unix socket and nothing else so far), so the package is what
          # `go build` produced, plus the version stamped in.
          storeboard = pkgs.buildGoModule {
            pname = "storeboard";
            inherit version;
            src = lib.cleanSource ./.;
            # No module dependencies — the stdlib covers the whole
            # foundation, so there is no vendor tree to hash.
            vendorHash = null;
            subPackages = [
              "cmd/storeboard"
              "cmd/storeboardd"
            ];
            ldflags = [
              "-X codeberg.org/kyleraykbs/storeboard/internal/version.Version=${version}"
            ];
            # The tests start a real daemon on a real socket and drive the
            # CLI against it.
            checkPhase = ''
              runHook preCheck
              go test ./...
              runHook postCheck
            '';
            meta = {
              description = "Replicated folders across your own devices";
              homepage = "https://codeberg.org/kyleraykbs/storeboard";
              mainProgram = "storeboard";
            };
          };
        in
        {
          packages = {
            inherit storeboard;
            default = storeboard;
          };

          apps = {
            storeboard = {
              type = "app";
              program = "${storeboard}/bin/storeboard";
              meta.description = "Storeboard CLI";
            };
            storeboardd = {
              type = "app";
              program = "${storeboard}/bin/storeboardd";
              meta.description = "Storeboard daemon";
            };
            default = self'.apps.storeboard;
          };

          checks = {
            inherit storeboard;

            # The example config must stay valid against the real validator —
            # it is what the README tells people to copy.
            config-example = pkgs.runCommand "check-config-example" { nativeBuildInputs = [ storeboard ]; } ''
              storeboard --config ${./config.example.json} config check
              touch $out
            '';
          };

          formatter = pkgs.nixfmt;

          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              go
              gopls
              gotools
              go-tools
              golangci-lint
              delve

              gcc

              # Every JSON surface is jq-shaped from day one.
              jq
            ];

            shellHook = ''
              export GOPATH="$PWD/.go"
              export PATH="$GOPATH/bin:$PATH"
            '';
          };
        };
    };
}
