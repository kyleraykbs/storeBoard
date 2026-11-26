package e2e

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type offersDoc struct {
	Policy string `json:"policy"`
	Offers []struct {
		Name    string   `json:"name"`
		Group   string   `json:"group"`
		From    []string `json:"from"`
		Mode    string   `json:"mode"`
		Carried string   `json:"carried"`
	} `json:"offers"`
}

func offersOf(t *testing.T, p *peer) offersDoc {
	t.Helper()
	var d offersDoc
	if err := json.Unmarshal([]byte(cliRun(t, p, "stores", "--json")), &d); err != nil {
		t.Fatal(err)
	}
	return d
}

// TestLearnedStores (DESIGN: learned stores): a device that declares its
// peers but no stores learns them from a peer that has it in their group,
// carries them by its policy (restarting itself to), keeps the offer's tree
// shape, and syncs.
func TestLearnedStores(t *testing.T) {
	n := startNetworkWith(t, map[string]map[string]storeDecl{
		"alpha": {
			"Docs":     {Group: "g"},
			"Pictures": {Group: "g", Links: map[string]string{"Bulk": "Bulk"}},
			"Bulk":     {Group: "g"},
		},
		"phone": {},
	}, nil, "alpha")
	alpha, phone := n.peer("alpha"), n.peer("phone")
	// The phone: no groups, no stores, and a policy of carrying everything
	// offered in full.
	cfg := map[string]any{}
	raw, _ := os.ReadFile(phone.cfgPath)
	json.Unmarshal(raw, &cfg)
	cfg["groups"] = map[string]any{}
	cfg["stores"] = map[string]any{}
	cfg["learn"] = map[string]any{"subscribe": "local"}
	raw, _ = json.Marshal(cfg)
	os.WriteFile(phone.cfgPath, raw, 0o644)
	write(t, filepath.Join(alpha.storeDir("Docs"), "hello.txt"), "from alpha")

	n.start("phone")
	waitFor(t, "phone learns alpha's stores", func() bool {
		d := offersOf(t, phone)
		return len(d.Offers) == 3 && d.Offers[0].From[0] == "alpha" && d.Policy == "local"
	})
	waitFor(t, "phone starts again carrying them", func() bool {
		for _, o := range offersOf(t, phone).Offers {
			if o.Carried != "learned" {
				return false
			}
		}
		return true
	})
	waitForContent(t, "alpha's file on the phone", filepath.Join(phone.storeDir("Docs"), "hello.txt"), "from alpha")
	write(t, filepath.Join(phone.storeDir("Pictures"), "cat.jpg"), "meow")
	waitForContent(t, "the phone's file on alpha", filepath.Join(alpha.storeDir("Pictures"), "cat.jpg"), "meow")
	if target, err := os.Readlink(filepath.Join(phone.storeDir("Pictures"), "Bulk")); err != nil || filepath.Base(target) != "Bulk" {
		t.Fatalf("Pictures/Bulk on the phone: %q %v", target, err)
	}

	// On alpha, the same stores are its own: offered back, they stay declared.
	waitFor(t, "alpha hears the phone's offers", func() bool { return len(offersOf(t, alpha).Offers) == 3 })
	for _, o := range offersOf(t, alpha).Offers {
		if o.Carried != "declared" {
			t.Fatalf("alpha: %+v", o)
		}
	}
}

// TestLearnedStoresOffByDefault: with no policy, offers are listed and
// nothing is carried until someone subscribes, which only root may.
func TestLearnedStoresOffByDefault(t *testing.T) {
	n := startNetwork(t, map[string]map[string]storeDecl{
		"alpha": {"Docs": {Group: "g"}},
		"bravo": {"Own": {Group: "g"}},
	})
	bravo := n.peer("bravo")
	waitFor(t, "bravo learns Docs", func() bool {
		for _, o := range offersOf(t, bravo).Offers {
			if o.Name == "Docs" {
				return o.Mode == "off" && o.Carried == ""
			}
		}
		return false
	})
	if _, err := os.Stat(bravo.storeDir("Docs")); err == nil {
		t.Fatal("bravo carries Docs without asking")
	}
	if os.Getuid() != 0 {
		cli, _ := binaries(t)
		out, err := exec.Command(cli, "-config", bravo.cfgPath, "subscribe", "Docs").CombinedOutput()
		if err == nil {
			t.Fatalf("a user subscribed: %s", out)
		}
	}
}
