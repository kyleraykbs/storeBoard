package config

import (
	"slices"
	"testing"
)

func phoneConfig(t *testing.T, policy string) *Config {
	t.Helper()
	raw := `{"configVersion": 2, "root": "/r", "dataDir": "` + t.TempDir() + `", "peer": "phone",
	  "peers": {"phone": {}, "amanita": {"endpoints": ["amanita.grid:4242"]}},
	  "groups": {"own": {"peers": ["phone"]}}, "stores": {"Notes": {"group": "own"}}`
	if policy != "" {
		raw += `, "learn": {"subscribe": "` + policy + `"}`
	}
	c, err := Parse([]byte(raw + "}"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestWithLearned(t *testing.T) {
	offers := map[string]Offer{
		"Pictures":     {Group: "kyle", From: []string{"amanita"}, Links: map[string]string{"BulkPictures": "Bulk"}},
		"BulkPictures": {Group: "kyle", From: []string{"amanita"}},
		"Videos":       {Group: "kyle", From: []string{"amanita"}},
		"Notes":        {Group: "kyle", From: []string{"amanita"}},      // declared here: the config wins
		"Stranger":     {Group: "kyle", From: []string{"someone-else"}}, // not a declared peer
	}

	// The default policy is off: only what is subscribed.
	c := phoneConfig(t, "")
	got, names := c.WithLearned(offers, map[string]Subscription{"Videos": {Sync: SyncLocal}})
	if !slices.Equal(names, []string{"Videos"}) || got.StoreTree["Videos"].Sync != SyncLocal {
		t.Fatalf("off policy: %v %+v", names, got.StoreTree)
	}
	if g := got.Groups["kyle"]; !slices.Equal(g.Peers, []string{"phone", "amanita"}) || !got.LearnedGroups["kyle"] {
		t.Fatalf("learned group %+v", g)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}

	// Stream by policy; one store turned off; links follow the offer.
	c = phoneConfig(t, SyncStream)
	got, names = c.WithLearned(offers, map[string]Subscription{"Videos": {Sync: SubscribeOff}})
	if !slices.Equal(names, []string{"BulkPictures", "Pictures"}) {
		t.Fatalf("stream policy: %v", names)
	}
	if got.StoreTree["Pictures"].Sync != SyncStream || got.StoreTree["Pictures"].Links["BulkPictures"] != "Bulk" {
		t.Fatalf("pictures %+v", got.StoreTree["Pictures"])
	}
	if got.StoreTree["Notes"].Group != "own" || got.LearnedFrom["Notes"] != nil {
		t.Fatal("a learned store replaced a declared one")
	}
	if _, ok := got.StoreTree["Stranger"]; ok {
		t.Fatal("an offer from an undeclared peer counted")
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}

	// Unlearned takes it all back out; the same offers are the same stores.
	if back := got.Unlearned(); len(back.StoreTree) != 1 || len(back.Groups) != 1 {
		t.Fatalf("unlearned %+v %+v", back.StoreTree, back.Groups)
	}
	again, _ := got.Unlearned().WithLearned(offers, map[string]Subscription{"Videos": {Sync: SubscribeOff}})
	if !SameLearned(got, again) {
		t.Fatal("the same offers differ")
	}
	more, _ := got.Unlearned().WithLearned(offers, nil)
	if SameLearned(got, more) {
		t.Fatal("subscribing to Videos changed nothing")
	}

	// A group declared here without this machine is not this machine's.
	c = phoneConfig(t, SyncStream)
	c.Groups["kyle"] = Group{Peers: []string{"amanita"}}
	if _, names := c.WithLearned(offers, nil); len(names) != 0 {
		t.Fatalf("joined someone else's group: %v", names)
	}
}

func TestSubscribe(t *testing.T) {
	c := phoneConfig(t, "")
	if err := SaveOffers(c.DataDir, map[string]Offer{"Videos": {Group: "kyle", From: []string{"amanita"}}, "Notes": {Group: "kyle", From: []string{"amanita"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe("Nothing", SyncStream, "test"); err == nil {
		t.Fatal("subscribed to a store nobody offers")
	}
	if _, err := c.Subscribe("Notes", SyncStream, "test"); err == nil {
		t.Fatal("subscribed over the config")
	}
	if _, err := c.Subscribe("Videos", "sideways", "test"); err == nil {
		t.Fatal("a mode that isn't one")
	}
	changed, err := c.Subscribe("Videos", SyncStream, "test")
	if err != nil || !changed {
		t.Fatalf("subscribe: %v %v", changed, err)
	}
	subs, _ := LoadSubscriptions(c.DataDir)
	if subs["Videos"].Sync != SyncStream {
		t.Fatalf("saved %+v", subs)
	}
	list := c.OfferList(map[string]Offer{"Videos": {Group: "kyle", From: []string{"amanita"}}}, subs)
	if len(list) != 1 || list[0].Mode != SyncStream || !list[0].Subscribed {
		t.Fatalf("list %+v", list)
	}
}
