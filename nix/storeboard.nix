# NixOS module for storeboard — one options set for the daemon and its CLI.
#
# Stores are flat top-level declarations. A store can reference another store
# by name with `links.<store> = "relative/path"`; the referenced store may be
# linked by any number of parents.
#
#   services.storeboard = {
#     enable = true;
#     peer = "laptop";
#     groups.family.peers = [ "laptop" "desktop" ];
#     stores.pictures = {
#       group = "family";
#       links.bulk = "Bulk";
#     };
#     stores.bulk = {
#       group = "family";
#       sync = "remote";
#     };
#   };
#
# Enabling it runs the daemon and installs `storeboard` for every user, with
# STOREBOARD_CONFIG pointed at the config file the daemon reads — there is no
# separate CLI module or option set to also enable.
{
  config,
  lib,
  pkgs,
  defaultPackage,
}:
let
  opts = import ./storeboard-options.nix { inherit lib pkgs; };

  cfg = config.services.storeboard;

  # The configuration the daemon reads: an explicit file when the user has
  # one (useful for secrets or externally managed config), the generated and
  # build-time-validated JSON otherwise.
  #
  # ExecStart embeds the STORE path of the generated file, not the /etc
  # symlink: a config change produces a new store path, which changes the
  # unit file, which makes nixos-rebuild switch RESTART the daemon. Without
  # that the daemon would keep running with yesterday's model until
  # somebody remembered to restart it by hand. The /etc copy stays for the
  # CLI fallback and humans; it is the same store path.
  generatedConfig = opts.checkedConfig cfg.package cfg "storeboard-config";

  configFile = if cfg.configFile != null then cfg.configFile else generatedConfig;

  # All stores are top-level; links are references and do not create owned
  # nested declarations.
  allStores = lib.attrValues cfg.stores;

  # A streamed store is a FUSE mount, which the hardened sandbox forbids:
  # PrivateDevices hides /dev/fuse, RestrictNamespaces blocks what fusermount
  # does, and the capability bounding set drops CAP_SYS_ADMIN, without which
  # mount(2) is EPERM even for root. The relaxation is applied only when a
  # store actually streams on this machine, so every other host keeps the
  # hardened unit unchanged.
  anyStreamed = lib.any (s: s.sync == "stream") allStores;

  # What the engine may write: the root, the data directory, and every
  # declared symlink path (and its parent — a store's link is created where
  # the user asked for it). "-" tolerates a path that does not exist yet
  # rather than failing the unit.
  storeReadWrite = lib.concatMap (
    s: lib.concatMap (p: [ "-${p}" "-${builtins.dirOf p}" ]) s.symlinks
  ) allStores;
in
{
  options.services.storeboard = {
    enable = lib.mkEnableOption "the storeboard sync daemon";

    package = lib.mkOption {
      type = lib.types.package;
      default = defaultPackage;
      defaultText = lib.literalExpression "storeboard.packages.\${system}.storeboard";
      description = "The storeboard package to run.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "root";
      description = ''
        User the service runs as. Defaults to root because the engine writes
        into people's home directories and materialises symlinks there; the
        syncthing tag made the same call for the same reason.
      '';
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "root";
      description = "Group the service runs as.";
    };

    configFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = "/run/secrets/storeboard.json";
      description = ''
        Configuration file to use instead of the generated one. Use this when
        the file is managed elsewhere or must not sit in the world-readable
        Nix store.
      '';
    };

    environment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      description = "Extra environment variables for the service.";
    };

    hardening = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Apply the systemd sandboxing options.";
    };

    # Daemon state lives with the stores, not on the system disk: blobs,
    # journals, history and trash are all part of the store's data, so the
    # default puts them inside the store location. A store root on a pool then
    # keeps everything on that pool.
    dataDir = lib.mkOption {
      type = lib.types.str;
      default = "${config.services.storeboard.root}/.storeboard";
      defaultText = lib.literalExpression ''"\${config.services.storeboard.root}/.storeboard"'';
      description = "The one storage directory for daemon state: blobs, journals, history.";
    };

    inherit (opts)
      socket
      root
      peer
      peers
      groups
      stores
      permissions
      backup
      ;
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.groups == { } || cfg.peer != "";
        message = "services.storeboard.peer: required while groups are declared — name this machine";
      }
      {
        assertion = cfg.peer == "" || lib.all (g: lib.elem cfg.peer g.peers) (lib.attrValues cfg.groups);
        message = "services.storeboard.peer must be a member of every group";
      }
    ];

    # The CLI rides with the daemon: one package holds both binaries, and the
    # env var points every user's CLI at the file this module generates from
    # the same options — the CLI and the daemon can never disagree about the
    # socket. A home-manager config still wins when one exists: the CLI
    # prefers an existing per-user file.
    environment.systemPackages = [ cfg.package ];
    environment.variables.STOREBOARD_CONFIG = "${configFile}";

    users.users = lib.optionalAttrs (cfg.user != "root") {
      ${cfg.user} = {
        isSystemUser = true;
        group = cfg.group;
        home = cfg.dataDir;
        description = "storeboard daemon";
      };
    };
    users.groups = lib.optionalAttrs (cfg.group != "root") {
      ${cfg.group} = { };
    };

	systemd.tmpfiles.rules = [
	  # Preserve an existing store-root owner; the daemon repairs it from
	  # the parent directory on startup.
	  "d ${cfg.root} 0755 - - -"
	  "d ${cfg.dataDir} 0750 ${cfg.user} ${cfg.group} -"
	];

    systemd.services.storeboard = {
      description = "Storeboard sync daemon";
      wantedBy = [ "multi-user.target" ];

      environment = cfg.environment;

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/storeboardd --config ${configFile}";
        User = cfg.user;
        Group = cfg.group;
        Restart = "on-failure";
        RestartSec = 2;
        # No StateDirectory: the state directory is cfg.dataDir, which lives
        # under the store root. tmpfiles creates both before the unit starts,
        # so WorkingDirectory and ReadWritePaths resolve.

        # The CLI runs as an arbitrary user, so the socket's directory is
        # traversable; the socket file's own mode is the daemon's to set.
        RuntimeDirectory = "storeboard";
        RuntimeDirectoryMode = "0755";
        WorkingDirectory = cfg.dataDir;
        # Stores live in the root; symlinks can be declared anywhere.
        ReadWritePaths = [
          cfg.dataDir
          cfg.root
        ] ++ storeReadWrite;
        # Content has to land owned by the human, not by root.
        AmbientCapabilities = [
          "CAP_CHOWN"
          "CAP_FOWNER"
          "CAP_DAC_OVERRIDE"
        ];
        CapabilityBoundingSet = [
          "CAP_CHOWN"
          "CAP_FOWNER"
          "CAP_DAC_OVERRIDE"
        ];
      }
      // lib.optionalAttrs cfg.hardening {
        NoNewPrivileges = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectSystem = "strict";
        # The engine syncs and mounts inside home directories.
        ProtectHome = false;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictNamespaces = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        LockPersonality = true;
        # Pure Go, nothing to JIT.
        MemoryDenyWriteExecute = true;
        SystemCallArchitectures = "native";
        # The wire is TCP over the nebula overlay: the sandbox must allow
        # the same families the daemon serves (its local socket plus the
        # declared IPv4/IPv6 endpoints). This list bit once — leaving only
        # AF_UNIX made every bind and dial fail with "address family not
        # supported by protocol", invisible to any test that runs outside
        # systemd's sandbox.
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
      }
      // lib.optionalAttrs anyStreamed {
        # A streamed store is a FUSE mount. This block is after the hardening
        # one on purpose: these keys merge last, so they win. The device and
        # the capability are only granted where a store actually streams.
        #
        # ProtectSystem and PrivateTmp are implemented through a private mount
        # namespace, which would make the mount invisible to every process
        # outside the unit - the daemon reported mount=true while the host saw
        # nothing. Mounts have to land in the host's namespace, so those three
        # are dropped on a streaming host and the rest of the sandbox stays.
        PrivateDevices = false;
        PrivateMounts = false;
        PrivateTmp = false;
        ProtectSystem = false;
        # These three are implemented with ReadOnlyPaths on /proc/sys and
        # /sys, which implies a private mount namespace just as ProtectSystem
        # does - the daemon still had a namespace of its own after the three
        # above were dropped.
        ProtectKernelTunables = false;
        ProtectKernelModules = false;
        ProtectControlGroups = false;
        RestrictNamespaces = false;
        AmbientCapabilities = [
          "CAP_CHOWN"
          "CAP_FOWNER"
          "CAP_DAC_OVERRIDE"
          "CAP_SYS_ADMIN"
        ];
        CapabilityBoundingSet = [
          "CAP_CHOWN"
          "CAP_FOWNER"
          "CAP_DAC_OVERRIDE"
          "CAP_SYS_ADMIN"
        ];
      };
    };

    # The generated configuration is written to /etc so it is obvious and
    # inspectable; a user-provided file is used exactly as it is.
    environment.etc = lib.mkIf (cfg.configFile == null) {
      "storeboard/config.json".source = generatedConfig;
    };
  };
}
