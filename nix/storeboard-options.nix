# Options and config generation shared by the NixOS service module and the
# home-manager CLI module.
#
# Storeboard has exactly one strict JSON config file, but which keys it
# carries depends on the scope: the daemon's file carries the whole model —
# this machine's root, peer name, declared peers, groups and store tree —
# while a CLI file needs only the socket it should dial. `checkedConfig`
# generates either and runs it through the real `storeboard config check` at
# build time, so a bad option value fails nixos-rebuild instead of failing at
# daemon startup.
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
    description = "The one storage directory for daemon state: blobs, journals, history.";
  };

  root = lib.mkOption {
    type = lib.types.str;
    default = "/srv/storeboard";
    example = "/home/kyle/Storeboard";
    description = ''
      The one directory the bytes live in. Every store's canonical dir is
      `<root>/<store>`, and the tree below it is made of symlinks. A plain
      container, not a store.
    '';
  };

  peer = lib.mkOption {
    type = lib.types.str;
    default = "";
    example = "laptop";
    description = ''
      This machine's own peer name: must be a key of `peers` and a member of
      every group. Empty lets the daemon fall back to the hostname, which only
      works while no groups are declared.
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

          permissions = lib.mkOption {
            type = lib.types.nullOr permSubmodule;
            default = null;
            example = {
              user = "kyle";
              mode = "0755";
            };
            description = ''
              Default ownership and mode for this group's stores; a store's
              own declaration overrides it field by field, and what nobody
              declares is inherited from the directory the root lives in.
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
      };
    };
    description = "Groups of peers that share stores.";
  };

  # One store's declaration. Everything but `path` and `symlinks` is
  # inherited from the parent when unset: a child only says what makes it
  # different (its group, its mount type, its permissions).
  storeModule =
    { name, ... }:
    {
      options = {
        path = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          example = "Bulk";
          description = ''
            Relpath inside the parent, and the name it appears under there.
            Null means the store's key, at the parent's top level. A
            top-level store's canonical dir is its key, so this is
            meaningless at the top level.
          '';
        };
        group = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          example = "family";
          description = "Group that shares this store. Null inherits the parent's; a top-level store must declare one.";
        };
        sync = lib.mkOption {
          type = lib.types.nullOr (lib.types.enum [ "local" "remote" ]);
          default = null;
          example = "remote";
          description = ''
            `local` holds a replica that works offline; `remote` streams the
            store from a holder. Null inherits the parent's; the top-level
            default is local.
          '';
        };
        symlinks = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [ ];
          example = [ "/home/kyle/Documents" ];
          description = ''
            Absolute paths that become symlinks to this store's canonical
            dir. Never inherited: a path belongs to one machine.
          '';
        };
        permissions = lib.mkOption {
          type = lib.types.nullOr permSubmodule;
          default = null;
          example = {
            user = "kyle";
          };
          description = "Ownership and mode for this store; overrides the parent field by field.";
        };
        stores = lib.mkOption {
          type = lib.types.attrsOf (lib.types.submodule storeModule);
          default = { };
          description = "Child stores, presented as symlinks inside this one.";
        };
      };
    };

  stores = lib.mkOption {
    type = lib.types.attrsOf (lib.types.submodule storeModule);
    default = { };
    example = {
      pictures = {
        group = "family";
        stores.bulk = {
          group = "media";
          sync = "remote";
        };
      };
      documents = {
        group = "family";
        symlinks = [ "/home/kyle/Documents" ];
      };
    };
    description = "The store tree, by id. Ids are globally unique and name the canonical dir under `root`.";
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
          stores = lib.mkOption {
            type = lib.types.attrsOf lib.types.bool;
            default = { };
            example = {
              Videos = false;
            };
            description = "Per-store opt-out by id: false keeps no history for that store.";
          };
        };
      }
    );
    default = null;
    description = ''
      Version history for this machine's stores, thinned the standard way:
      the newest version of each hour for a day, of each day for a week, of
      each week for a month, of each month for a year — and nothing older
      than `keep`. The current version of a file is never dropped. A store
      that is `remote` on this machine keeps no history.
    '';
  };

  # One Permissions block, shared by the group default, the store
  # declaration and the per-machine override, so they cannot drift apart.
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
      Per-store ownership and mode for the entries the daemon creates on THIS
      machine; a set field beats the store's declaration and its group's
      default, and anything nobody declares is inherited from the directory
      the root lives in. Directory modes only: files keep the mode they were
      synced with.
    '';
  };

  # The store tree as it goes into the JSON file: only what is declared.
  storesJSON =
    tree:
    lib.mapAttrs (
      _: s:
      lib.optionalAttrs (s.path != null) { path = s.path; }
      // lib.optionalAttrs (s.group != null) { group = s.group; }
      // lib.optionalAttrs (s.sync != null) { sync = s.sync; }
      // lib.optionalAttrs (s.symlinks != [ ]) { symlinks = s.symlinks; }
      // lib.optionalAttrs (s.permissions != null) {
        permissions = lib.filterAttrs (_: v: v != null) s.permissions;
      }
      // lib.optionalAttrs (s.stores != { }) { stores = storesJSON s.stores; }
    ) tree;

  # The configuration file both binaries read: strict JSON, configVersion 2,
  # unknown keys an error. Sections appear only when the calling scope has the
  # options — a CLI file carries the socket, the daemon file the model.
  configJSON =
    cfg:
    {
      configVersion = 2;
    }
    // lib.optionalAttrs (cfg ? socket) { socket = cfg.socket; }
    // lib.optionalAttrs (cfg ? dataDir) { dataDir = cfg.dataDir; }
    // lib.optionalAttrs (cfg ? root) { root = cfg.root; }
    // lib.optionalAttrs (cfg ? peer && cfg.peer != "") { peer = cfg.peer; }
    // lib.optionalAttrs (cfg ? peers) {
      peers = lib.mapAttrs (_: p: { endpoints = p.endpoints; }) cfg.peers;
    }
    // lib.optionalAttrs (cfg ? groups) {
      groups = lib.mapAttrs (
        _: g:
        { peers = g.peers; }
        // lib.optionalAttrs (g.permissions != null) {
          permissions = lib.filterAttrs (_: v: v != null) g.permissions;
        }
      ) cfg.groups;
    }
    // lib.optionalAttrs (cfg ? stores) { stores = storesJSON cfg.stores; }
    // lib.optionalAttrs (cfg ? permissions && cfg.permissions != { }) {
      permissions = lib.mapAttrs (_: p: lib.filterAttrs (_: v: v != null) p) cfg.permissions;
    }
    // lib.optionalAttrs (cfg ? backup && cfg.backup != null) {
      backup = { enable = cfg.backup.enable; }
      // lib.optionalAttrs (cfg.backup.keep != null) { keep = cfg.backup.keep; }
      // lib.optionalAttrs (cfg.backup.stores != { }) { stores = cfg.backup.stores; };
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