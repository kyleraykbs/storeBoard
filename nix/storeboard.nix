# NixOS module for storeboard — one options set for the daemon and its CLI.
#
#   services.storeboard = {
#     enable = true;
#     peer = "laptop";
#     peers = {
#       laptop.endpoints = [ "192.168.1.10:4242" ];
#       desktop.endpoints = [ "192.168.1.11:4242" ];
#     };
#     groups.family.peers = [ "laptop" "desktop" ];
#     stores.pictures = {
#       group = "family";
#       stores.bulk = { group = "media"; sync = "remote"; };
#     };
#     stores.documents = {
#       group = "family";
#       symlinks = [ "/home/kyle/Documents" ];
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

  # Every store in the tree, flattened: the ids are global, the placement is
  # a tree. Used only to collect the paths the service may write.
  flattenStores =
    tree: lib.concatLists (lib.mapAttrsToList (_: s: [ s ] ++ flattenStores (s.stores or { })) tree);

  allStores = flattenStores cfg.stores;

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

    inherit (opts)
      socket
      dataDir
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
        StateDirectory = "storeboard";
        StateDirectoryMode = "0750";
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
      };
    };

    # The generated configuration is written to /etc so it is obvious and
    # inspectable; a user-provided file is used exactly as it is.
    environment.etc = lib.mkIf (cfg.configFile == null) {
      "storeboard/config.json".source = generatedConfig;
    };
  };
}
