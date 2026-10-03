# NixOS module for the storeboard CLI.
#
# It installs the CLI for every user and drops a system-wide default
# configuration at /etc/storeboard/config.json, pointed at by
# STOREBOARD_CONFIG. A home-manager configuration still wins when it exists:
# the CLI prefers an existing per-user file over the environment.
{
  config,
  lib,
  pkgs,
  defaultPackage,
}:
let
  opts = import ./storeboard-options.nix { inherit lib pkgs; };

  cfg = config.programs.storeboard;
in
{
  options.programs.storeboard = {
    enable = lib.mkEnableOption "the storeboard CLI";

    package = lib.mkOption {
      type = lib.types.package;
      default = defaultPackage;
      defaultText = lib.literalExpression "storeboard.packages.\${system}.storeboard";
      description = "The storeboard package to install.";
    };

    inherit (opts) socket;
  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];

    environment.etc."storeboard/config.json".source =
      opts.checkedConfig cfg.package cfg
        "storeboard-config";

    environment.variables.STOREBOARD_CONFIG = "/etc/storeboard/config.json";
  };
}
