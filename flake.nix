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
          storeboard =
            {
              config,
              lib,
              pkgs,
              ...
            }:
            import ./nix/storeboard.nix {
              inherit config lib pkgs;
              defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.storeboard;
            };
          default = self.nixosModules.storeboard;
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

            # The module's contract: enabling services.storeboard runs the
            # daemon AND installs the CLI pointed at the daemon's config.
            # Evaluated against stubbed NixOS options (the patch pattern),
            # because module files are otherwise invisible to flake check.
            module-service =
              let
                eval = lib.evalModules {
                  specialArgs = { inherit pkgs; };
                  modules = [
                    # The NixOS options storeboard.nix writes, stubbed:
                    # enough to evaluate it without a whole system.
                    (
                      { lib, ... }:
                      {
                        options = {
                          assertions = lib.mkOption {
                            type = lib.types.listOf lib.types.anything;
                            default = [ ];
                          };
                          environment.etc = lib.mkOption {
                            type = lib.types.attrsOf lib.types.anything;
                            default = { };
                          };
                          environment.systemPackages = lib.mkOption {
                            type = lib.types.listOf lib.types.package;
                            default = [ ];
                          };
                          environment.variables = lib.mkOption {
                            type = lib.types.attrsOf lib.types.str;
                            default = { };
                          };
                          systemd.services = lib.mkOption {
                            type = lib.types.attrsOf lib.types.anything;
                            default = { };
                          };
                          users.groups = lib.mkOption {
                            type = lib.types.attrsOf lib.types.anything;
                            default = { };
                          };
                          users.users = lib.mkOption {
                            type = lib.types.attrsOf lib.types.anything;
                            default = { };
                          };
                        };
                      }
                    )
                    (
                      {
                        config,
                        lib,
                        pkgs,
                        ...
                      }:
                      import ./nix/storeboard.nix {
                        inherit config lib pkgs;
                        defaultPackage = storeboard;
                      }
                    )
                    {
                      config.services.storeboard = {
                        enable = true;
                        peer = "laptop";
                        peers = {
                          laptop.endpoints = [ "192.168.1.10:4242" ];
                          desktop.endpoints = [ "192.168.1.11:4242" ];
                        };
                        groups.family = {
                          peers = [
                            "laptop"
                            "desktop"
                          ];
                          folders = {
                            photos.mount = "/srv/photos";
                            archive = { };
                          };
                        };
                        # Stream one folder instead of replicating it.
                        keepOffline.archive = false;
                      };
                    }
                  ];
                };

                cfg = eval.config;

                problems = lib.filter (p: p != null) [
                  (
                    if lib.elem storeboard cfg.environment.systemPackages then
                      null
                    else
                      "the CLI is not installed by the module"
                  )
                  (
                    if cfg.environment.variables.STOREBOARD_CONFIG or null == "/etc/storeboard/config.json" then
                      null
                    else
                      "STOREBOARD_CONFIG does not point at the daemon's config"
                  )
                  (
                    if cfg.systemd.services.storeboard.serviceConfig.ExecStart or null != null then
                      null
                    else
                      "no daemon unit"
                  )
                  (if lib.all (a: a.assertion) cfg.assertions then null else "an assertion failed")
                ];
              in
              if problems == [ ] then
                pkgs.runCommand "check-module-service" { } "touch $out"
              else
                throw "services.storeboard: ${lib.concatStringsSep "; " problems}";
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
