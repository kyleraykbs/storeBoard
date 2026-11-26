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
          signIn = lib.mkOption {
            type = lib.types.nullOr (
              lib.types.submodule {
                options = {
                  gatehouse = lib.mkOption {
                    type = lib.types.str;
                    example = "http://amanita.friends:4849";
                    description = "That grid's GateHouse, as this machine reaches it.";
                  };
                  credentials = lib.mkOption {
                    type = lib.types.str;
                    default = "/var/lib/storeboard/signin.json";
                    description = "Where `storeboard login <peer>` keeps the sign-in (root's).";
                  };
                };
              }
            );
            default = null;
            description = ''
              The peer belongs to someone else's grid, whose owner shares
              stores with this machine's person: connect to it signed in
              with that grid's GateHouse (`storeboard login <peer>` once).
            '';
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

  learn = lib.mkOption {
    type = lib.types.nullOr (
      lib.types.submodule {
        options.subscribe = lib.mkOption {
          type = lib.types.enum [
            "off"
            "stream"
            "local"
          ];
          default = "off";
          description = ''
            What a store a peer offers this machine (one in a group with it
            that this config does not declare) becomes until someone chooses:
            off (listed by `storeboard stores`, carried only once someone runs
            `storeboard subscribe`), streamed, or kept in full.
          '';
        };
      }
    );
    default = null;
    description = "Stores learned from peers (DESIGN: learned stores). Declared stores always win.";
  };

  friends = lib.mkOption {
    type = lib.types.nullOr (
      lib.types.submodule {
        options = {
          listen = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            example = [ "[fd00::1]:4243" ];
            description = "Where friends' devices connect (only friends are accepted there).";
          };
          gatehouse = lib.mkOption {
            type = lib.types.nullOr lib.types.str;
            default = null;
            description = "This grid's GateHouse as people reach it, for `storeboard share`.";
          };
        };
      }
    );
    default = null;
    description = ''
      Let people this grid's GateHouse knows sync, from their own devices,
      the stores they are given (`storeboard share`, or the grants
      storeboard.read and storeboard.write scoped to a store). Needs the
      GateHouse agent on this machine.
    '';
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

  storeModule =
    { name, ... }:
    {
      options = {
        group = lib.mkOption { type = lib.types.nullOr lib.types.str; default = null; };
        sync = lib.mkOption { type = lib.types.nullOr (lib.types.enum [ "local" "remote" "stream" ]); default = null; };
        preferred = lib.mkOption { type = lib.types.nullOr lib.types.str; default = null; description = "Preferred peer for streamed reads. Must share the store's group."; };
        offline = lib.mkOption {
          type = lib.types.nullOr (lib.types.submodule {
            options = {
              policy = lib.mkOption { type = lib.types.enum [ "unbounded" "capped" "readonly" ]; default = "unbounded"; };
              maxBytes = lib.mkOption { type = lib.types.nullOr lib.types.ints.positive; default = null; };
            };
          });
          default = null;
        };
        symlinks = lib.mkOption { type = lib.types.listOf lib.types.str; default = [ ]; };
        permissions = lib.mkOption { type = lib.types.nullOr permSubmodule; default = null; };
        links = lib.mkOption { type = lib.types.attrsOf lib.types.str; default = { }; };
      };
    };

  stores = lib.mkOption {
    type = lib.types.attrsOf (lib.types.submodule storeModule);
    default = { };
    description = "Flat top-level stores; links reference stores by name.";
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

  trash = lib.mkOption {
    type = lib.types.nullOr (
      lib.types.submodule {
        options.keep = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          example = "1w";
          description = ''
            How long a deleted or replaced file stays in the trash, counted
            from when it got there: a duration with h/d/w/y units. Null
            follows backup.keep, or one year without backup.
          '';
        };
      }
    );
    default = null;
    description = "Trash retention for this machine's stores.";
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

  # The flat store namespace as it goes into JSON: only declared fields.
  storesJSON =
    stores:
    lib.mapAttrs (
	  _: s:
	  lib.optionalAttrs (s.group != null) { group = s.group; }
	  // lib.optionalAttrs (s.sync != null) { sync = s.sync; }
	  // lib.optionalAttrs (s.preferred != null) { preferred = s.preferred; }
	  // lib.optionalAttrs (s.offline != null) {
	    offline = { policy = s.offline.policy; } // lib.optionalAttrs (s.offline.maxBytes != null) { maxBytes = s.offline.maxBytes; };
	  }
	  // lib.optionalAttrs (s.symlinks != [ ]) { symlinks = s.symlinks; }
	  // lib.optionalAttrs (s.links != { }) { links = s.links; }
	  // lib.optionalAttrs (s.permissions != null) {
	    permissions = lib.filterAttrs (_: v: v != null) s.permissions;
	  }
	) stores;

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
      peers = lib.mapAttrs (
        _: p: { endpoints = p.endpoints; } // lib.optionalAttrs (p.signIn != null) { signIn = p.signIn; }
      ) cfg.peers;
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
    // lib.optionalAttrs (cfg ? learn && cfg.learn != null) {
      learn = { subscribe = cfg.learn.subscribe; };
    }
    // lib.optionalAttrs (cfg ? friends && cfg.friends != null) {
      friends = { listen = cfg.friends.listen; } // lib.optionalAttrs (cfg.friends.gatehouse != null) { gatehouse = cfg.friends.gatehouse; };
    }
    # Both were options the daemon module exposed and this file never wrote,
    # so `backup.enable = true` and per-store permissions silently did
    # nothing. Null fields are dropped: the config is strict, and absent is
    # what means "inherit" or "the default".
    // lib.optionalAttrs (cfg ? permissions && cfg.permissions != { }) {
      permissions = lib.mapAttrs (_: p: lib.filterAttrs (_: v: v != null) p) cfg.permissions;
    }
    // lib.optionalAttrs (cfg ? backup && cfg.backup != null) {
      backup =
        { enable = cfg.backup.enable; }
        // lib.optionalAttrs (cfg.backup.keep != null) { keep = cfg.backup.keep; }
        // lib.optionalAttrs (cfg.backup.stores != { }) { stores = cfg.backup.stores; };
    }
    // lib.optionalAttrs (cfg ? trash && cfg.trash != null && cfg.trash.keep != null) {
      trash = { keep = cfg.trash.keep; };
    };
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