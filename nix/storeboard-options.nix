# Options and config generation shared by the NixOS service module and the
# two CLI modules.
#
# Storeboard has exactly one strict JSON config file, but which keys it
# carries depends on the scope: the daemon's file has everything, a CLI file
# needs only the socket it should dial. `checkedConfig` generates either and
# runs it through the real `storeboard config check` at build time, so a bad
# option value fails nixos-rebuild instead of failing at daemon startup.
{ lib, pkgs }:
rec {
  # The socket the generated configs point at. The service owns this path
  # (RuntimeDirectory=storeboard); the CLI modules default to the same one so
  # an unconfigured client finds an unconfigured daemon.
  defaultSocket = "/run/storeboard/storeboard.sock";

  socket = lib.mkOption {
    type = lib.types.str;
    default = defaultSocket;
    example = "/run/user/1000/storeboard.sock";
    description = "Unix socket the daemon serves /v1 on and the CLI dials.";
  };

  dataDir = lib.mkOption {
    type = lib.types.str;
    default = "/var/lib/storeboard";
    example = "/var/lib/storeboard";
    description = "Directory the daemon keeps its state in.";
  };

  folders = lib.mkOption {
    type = lib.types.listOf (
      lib.types.submodule {
        options = {
          name = lib.mkOption {
            type = lib.types.str;
            example = "photos";
            description = "Stable name of the folder inside the mesh; must be unique.";
          };
          path = lib.mkOption {
            type = lib.types.str;
            example = "/srv/photos";
            description = "Absolute path the folder is bound to on this machine.";
          };
        };
      }
    );
    default = [ ];
    example = [
      {
        name = "photos";
        path = "/srv/photos";
      }
    ];
    description = "Folders this machine syncs.";
  };

  # The configuration file both binaries read: strict JSON, configVersion 1,
  # unknown keys an error. Sections appear only when the calling scope has
  # the options — a CLI file carries the socket, the daemon file all of it.
  configJSON =
    cfg:
    {
      configVersion = 1;
    }
    // lib.optionalAttrs (cfg ? socket) { socket = cfg.socket; }
    // lib.optionalAttrs (cfg ? dataDir) { dataDir = cfg.dataDir; }
    // lib.optionalAttrs (cfg ? folders) {
      folders = map (f: { inherit (f) name path; }) cfg.folders;
    };

  # The config file, validated by the real `storeboard config check` during
  # the build: nixos-rebuild fails with the validator's message, and the file
  # that lands in /etc (or a home) is one the daemon is guaranteed to accept.
  checkedConfig =
    package: cfg: name:
    let
      raw = (pkgs.formats.json { }).generate "${name}.json" (configJSON cfg);
    in
    pkgs.runCommand "${name}-checked" { nativeBuildInputs = [ package ]; } ''
      storeboard --config ${raw} config check
      cp ${raw} $out
    '';
}
