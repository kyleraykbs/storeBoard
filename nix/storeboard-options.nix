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

          permissions = lib.mkOption {
            type = lib.types.nullOr permSubmodule;
            default = null;
            example = {
              user = "kyle";
              mode = "0755";
            };
            description = ''
              Default ownership and mode for this group's folders on every
              peer; each machine's `permissions` overrides it field by
              field, and what neither declares is inherited from the
              directory the mount lives in.
            '';
          };

          storageDir = lib.mkOption {
            type = lib.types.nullOr lib.types.str;
            default = null;
            example = "/mainpool";
            description = ''
              Storage directory for this group's folders, overriding the
              top-level `storageDir`. Content lives in
              `<storageDir>/<folder>` and the mount path is a symlink to it.
            '';
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

  storageDir = lib.mkOption {
    type = lib.types.nullOr lib.types.str;
    default = null;
    example = "/srv/storeboard";
    description = ''
      Where folder content lives on this machine: each folder's root becomes
      `<storageDir>/<folder>` and its mount path is a symlink to it, so
      content and mount point are independent — the mount may sit anywhere
      and no two folders can index the same bytes. Null (the default) means
      `<dataDir>/storage`; the empty string restores the original layout
      (a replica's content at the mount path itself, a streamed folder's
      under the data directory). Content already at a mount path is migrated
      into the storage directory on the next transition. A group may override
      it with `groups.<name>.storageDir`.
    '';
  };

  backup = lib.mkOption {
    type = lib.types.nullOr (
      lib.types.submodule {
        options = {
          enable = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Keep a thinned history of every file's versions under the data directory.";
          };
          keep = lib.mkOption {
            type = lib.types.nullOr lib.types.str;
            default = null;
            example = "1y";
            description = ''
              How long a version may live: a duration with h/d/w/y units
              ("1y", "90d", "4w", "36h"). Null means one year. Nothing older
              than this is ever kept.
            '';
          };
          folders = lib.mkOption {
            type = lib.types.attrsOf lib.types.bool;
            default = { };
            example = {
              Videos = false;
            };
            description = "Per-folder opt-out by name: false keeps no history for that folder.";
          };
        };
      }
    );
    default = null;
    description = ''
      Version history for this machine's folders, thinned the standard way:
      the newest version of each hour for a day, of each day for a week, of
      each week for a month, of each month for a year — and nothing older
      than `keep`. The current version of a file is never dropped, and the
      retention policy is the only thing the daemon ever deletes on its own.
    '';
  };

  # One Permissions block, shared by the group default and the per-folder
  # override, so the two can never drift apart.
  permSubmodule = lib.types.submodule {
    options = {
      user = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "kyle";
        description = "Owner of the entries the daemon creates: a user name or numeric uid.";
      };
      group = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "kyle";
        description = "Group of the entries the daemon creates: a group name or numeric gid.";
      };
      mode = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "0755";
        description = "Octal directory mode for the directories the daemon creates.";
      };
    };
  };

  permissions = lib.mkOption {
    type = lib.types.attrsOf permSubmodule;
    default = { };
    example = {
      photos.user = "kyle";
    };
    description = ''
      Per-folder ownership and mode for the entries the daemon creates on
      THIS machine; a set field beats the group's `permissions`, and
      anything neither declares is inherited from the directory the mount
      lives in — so by default a folder is permissioned exactly like the
      directory containing it. Directory modes only: files keep the mode
      they were synced with.
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
      groups = lib.mapAttrs (
        _: g:
        {
          peers = g.peers;
          folders = lib.mapAttrs (_: f: lib.optionalAttrs (f.mount != null) { mount = f.mount; }) g.folders;
        }
        // lib.optionalAttrs (g.permissions != null) {
          permissions = lib.filterAttrs (_: v: v != null) g.permissions;
        }
        // lib.optionalAttrs (g.storageDir != null) { storageDir = g.storageDir; }
      ) cfg.groups;
    }
    // lib.optionalAttrs (cfg ? mounts) { mounts = cfg.mounts; }
    // lib.optionalAttrs (cfg ? keepOffline) { keepOffline = cfg.keepOffline; }
    // lib.optionalAttrs (cfg ? permissions && cfg.permissions != { }) {
      permissions = lib.mapAttrs (_: p: lib.filterAttrs (_: v: v != null) p) cfg.permissions;
    }
    // lib.optionalAttrs (cfg ? storageDir && cfg.storageDir != null) {
      storageDir = cfg.storageDir;
    }
    // lib.optionalAttrs (cfg ? backup && cfg.backup != null) {
      backup = {
        enable = cfg.backup.enable;
      }
      // lib.optionalAttrs (cfg.backup.keep != null) { keep = cfg.backup.keep; }
      // lib.optionalAttrs (cfg.backup.folders != { }) { folders = cfg.backup.folders; };
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
