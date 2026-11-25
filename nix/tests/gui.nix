# The window, on a real daemon: stores with files, a peer that is away, the
# health report, space. One screenshot per page.
{ module, gui }:
{ pkgs, ... }:
let
  page = force: p: {
    services.cage = {
      enable = true;
      user = "alice";
      program = "${gui}/bin/storeboard-gui";
      environment = {
        STOREBOARD_GUI_PAGE = if force then pkgs.lib.mkForce p else p;
        STOREBOARD_GUI_EXPAND = "1";
        STOREBOARD_GUI_TRACE = "1";
      };
    };
  };
in
{
  name = "storeboard-gui";
  nodes.machine =
    { lib, ... }:
    {
      imports = [
        module
        (page false "overview")
      ];
      users.users.alice = {
        isNormalUser = true;
        password = "x";
      };
      services.storeboard = {
        enable = true;
        peer = "laptop";
        root = "/var/lib/sb";
        peers = {
          laptop.endpoints = [ "127.0.0.1:4242" ];
          desktop.endpoints = [ "192.0.2.10:4242" ];
        };
        groups.home.peers = [
          "laptop"
          "desktop"
        ];
        stores.Documents = {
          group = "home";
          sync = "local";
        };
        stores.Pictures = {
          group = "home";
          sync = "local";
        };
        permissions.Documents.user = "alice";
        permissions.Pictures.user = "alice";
        backup.enable = true;
      };
      virtualisation.memorySize = 2048;
      virtualisation.resolution = {
        x = 1280;
        y = 800;
      };
      fonts.packages = [ pkgs.cantarell-fonts ];
      environment.systemPackages = [ pkgs.adwaita-icon-theme ];
      specialisation = lib.genAttrs [ "stores" "peers" "space" ] (p: {
        configuration = page true p;
      });
    };
  testScript = ''
    machine.wait_for_unit("storeboard.service")
    machine.wait_until_succeeds("test -d /var/lib/sb/Documents")
    machine.succeed("for i in 1 2 3; do echo note $i > /var/lib/sb/Documents/note$i.txt; done; echo pic > /var/lib/sb/Pictures/cat.jpg; chown -R alice /var/lib/sb/Documents /var/lib/sb/Pictures")
    machine.succeed("curl -s --unix-socket /run/storeboard/storeboard.sock -X POST http://x/v1/stores/Documents/scan")
    machine.wait_for_unit("cage-tty1.service")
    machine.sleep(8)
    machine.screenshot("overview")
    base = machine.succeed("readlink -f /run/current-system").strip()
    for p in ["stores", "peers", "space"]:
        machine.succeed(f"{base}/specialisation/{p}/bin/switch-to-configuration test")
        machine.succeed("systemctl reset-failed cage-tty1.service; systemctl restart cage-tty1.service")
        machine.wait_for_unit("cage-tty1.service")
        # Stores: long enough for the data to change several times (scans
        # move the times); the page must update in place, not be rebuilt.
        machine.sleep(40 if p == "stores" else 8)
        machine.screenshot(p)
        if p == "stores":
            since = machine.succeed("systemctl show -p ActiveEnterTimestamp --value cage-tty1.service").strip()
            rebuilt = machine.succeed(f"journalctl -b --since '{since}' -o cat | grep -c 'rebuild Stores (data)' || true").strip()
            assert rebuilt == "1", f"the Stores page was built from data {rebuilt} times in 40 s"
  '';
}
