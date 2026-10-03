# home-manager module for the storeboard CLI.
#
# It installs storeboard for one user and writes that user's configuration to
# $XDG_CONFIG_HOME/storeboard/config.json, which the CLI prefers over any
# system-wide file.
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
    home.packages = [ cfg.package ];

    xdg.configFile."storeboard/config.json".source =
      opts.checkedConfig cfg.package cfg
        "storeboard-config";
  };
}
