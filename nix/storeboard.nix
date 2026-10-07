# NixOS module for storeboard — one options set for the daemon and its CLI.
#
#   services.storeboard = {
#     enable = true;
#     peer = "laptop";
#     peers = {
#       laptop.endpoints = [ "192.168.1.10:4242" ];
#       desktop.endpoints = [ "192.168.1.11:4242" ];
#     };
#     groups.family = {
#       peers = [ "laptop" "desktop" ];
#       folders.photos.mount = "/home/kyle/Pictures";
#     };
#     mounts = { };   # this machine's local mount overrides
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
      default = "storeboard";
      description = "User the service runs as.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "storeboard";
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
      peer
      peers
      groups
      mounts
      keepOffline
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
      {
        assertion =
          let
            names = lib.concatMap (g: lib.attrNames g.folders) (lib.attrValues cfg.groups);
          in
          names == lib.unique names;
        message = "services.storeboard.groups: folder names must be unique across groups";
      }
      {
        assertion =
          let
            groupMounts = lib.concatMap (g: lib.mapAttrsToList (_: f: f.mount) g.folders) (
              lib.attrValues cfg.groups
            );
          in
          lib.all (m: m == null || lib.hasPrefix "/" m) (groupMounts ++ lib.attrValues cfg.mounts);
        message = "services.storeboard: mount points must be absolute";
      }
    ];

    # The CLI rides with the daemon: one package holds both binaries, and the
    # env var points every user's CLI at the file this module generates from
    # the same options — the CLI and the daemon can never disagree about the
    # socket. A home-manager config still wins when one exists: the CLI
    # prefers an existing per-user file.
    environment.systemPackages = [ cfg.package ];
    environment.variables.STOREBOARD_CONFIG = "${configFile}";

    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      home = cfg.dataDir;
      description = "storeboard daemon";
    };
    users.groups.${cfg.group} = { };

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
        # The daemon owns its state dir today. When the sync engine lands,
        # every configured folder path joins this list (and ProtectHome with
        # it) — see agents/TODO.md.
        ReadWritePaths = [ cfg.dataDir ];
      }
      // lib.optionalAttrs cfg.hardening {
        NoNewPrivileges = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectSystem = "strict";
        ProtectHome = true;
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
