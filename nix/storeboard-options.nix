# Options and config generation shared by the NixOS service module and the
# home-manager CLI module.
#
# Storeboard has exactly one strict JSON config file, but which keys it
# carries depends on the scope: the daemon's file carries the whole model —
# this machine's peer name, declared peers, groups, local mount overrides —
# while a CLI file needs only the socket it should dial. `checkedConfig`
# generates either and runs it through the real `storeboard config check`
# at build time, so a bad option value fails nixos-rebuild instead of
# failing at daemon startup.
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
    description = "The one storage directory: store, journal, index, default mounts.";
  };

  peer = lib.mkOption {
    type = lib.types.str;
    default = "";
    example = "laptop";
    description = ''
      This machine's own peer name: must be a key of `peers` and a member
      of every group. Empty lets the daemon fall back to the hostname,
      which only works while no groups are declared.
    '';
  };

  peers = lib.mkOption {
    type = lib.types.attrsOf (
      lib.types.submodule {
        options = {
          endpoints = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            default = [ ];
            example = [ "192.168.1.10:4242" ];
            description = "Declared host:port endpoints other peers dial.";
          };
        };
      }
    );
    default = { };
    example = {
      laptop.endpoints = [ "192.168.1.10:4242" ];
      phone.endpoints = [ ];
    };
    description = "Devices this machine may sync with, by name.";
  };

  groups = lib.mkOption {
    type = lib.types.attrsOf (
      lib.types.submodule {
        options = {
          peers = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            description = "Member peers; must include this machine's `peer`.";
          };

          folders = lib.mkOption {
            type = lib.types.attrsOf (
              lib.types.submodule {
                options = {
                  mount = lib.mkOption {
                    type = lib.types.nullOr lib.types.str;
                    default = null;
                    example = "/srv/photos";
                    description = ''
                      Default mount point for this folder on every peer;
                      each peer overrides it locally through `mounts`.
                    '';
                  };
                };
              }
            );
            default = { };
            example = {
              photos.mount = "/srv/photos";
            };
            description = "Folders shared by the group; names are global across groups.";
          };
        };
      }
    );
    default = { };
    example = {
      family = {
        peers = [
          "laptop"
          "desktop"
        ];
        folders.photos.mount = "/srv/photos";
      };
    };
    description = "Groups of peers that share folders.";
  };

  mounts = lib.mkOption {
    type = lib.types.attrsOf lib.types.str;
    default = { };
    example = {
      photos = "/mnt/photos";
    };
    description = ''
      This machine's mount-point overrides for group folders. Effective
      mount: local override, else the group's default, else inside dataDir.
    '';
  };

  keepOffline = lib.mkOption {
    type = lib.types.attrsOf lib.types.bool;
    default = { };
    example = {
      projectStorage = false;
    };
    description = ''
      Per-folder switch on THIS machine: true (default for anything
      undeclared) keeps a full local replica that works offline; false
      streams the folder from a holder — mounted live while a holder is
      reachable, an empty dummy dir when none is, remerged on reconnect.
    '';
  };

  # The configuration file both binaries read: strict JSON, configVersion 1,
  # unknown keys an error. Sections appear only when the calling scope has
  # the options — a CLI file carries the socket, the daemon file the model.
  configJSON =
    cfg:
    {
      configVersion = 1;
    }
    // lib.optionalAttrs (cfg ? socket) { socket = cfg.socket; }
    // lib.optionalAttrs (cfg ? dataDir) { dataDir = cfg.dataDir; }
    // lib.optionalAttrs (cfg ? peer && cfg.peer != "") { peer = cfg.peer; }
    // lib.optionalAttrs (cfg ? peers) {
      peers = lib.mapAttrs (_: p: { endpoints = p.endpoints; }) cfg.peers;
    }
    // lib.optionalAttrs (cfg ? groups) {
      groups = lib.mapAttrs (_: g: {
        peers = g.peers;
        folders = lib.mapAttrs (_: f: lib.optionalAttrs (f.mount != null) { mount = f.mount; }) g.folders;
      }) cfg.groups;
    }
    // lib.optionalAttrs (cfg ? mounts) { mounts = cfg.mounts; }
    // lib.optionalAttrs (cfg ? keepOffline) { keepOffline = cfg.keepOffline; };

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
