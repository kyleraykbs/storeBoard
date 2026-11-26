package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"codeberg.org/kyleraykbs/storeboard/pkg/api"
)

// Learned stores (DESIGN: learned stores, decided 2026-11-25). The tree of
// stores is shared by every peer; what a machine chooses is whether it
// carries a store and how. So a declared peer tells this machine which of
// its stores this machine is in a group for (an Offer), and this machine
// subscribes to the ones it wants (a Subscription) or follows its Learn
// policy. Machines are never learned: an offer only counts from a declared
// peer, and a learned group holds only declared peers. The config wins over
// both, and so does a requested store.

// Sync modes a subscription takes, besides SyncLocal and SyncStream.
const SubscribeOff = "off"

// Learn is a machine's policy for offered stores it has not chosen for.
type Learn struct {
	// Subscribe is what an offered store becomes until someone says
	// otherwise: "off" (the default: only what is subscribed by hand),
	// "stream" or "local".
	Subscribe string `json:"subscribe,omitempty"`
}

// Offer is a store peers said this machine may carry.
type Offer struct {
	// Group is the store's group on the offering peer, and the group the
	// store joins here.
	Group string `json:"group"`
	// Links are the store's children, by id, at their relative paths.
	Links map[string]string `json:"links,omitempty"`
	// From are the peers that offered it.
	From []string  `json:"from"`
	Seen time.Time `json:"seen"`
}

// Subscription is what this machine does with an offered store.
type Subscription struct {
	// Sync is "stream", "local" or "off".
	Sync string    `json:"sync"`
	By   string    `json:"by,omitempty"`
	At   time.Time `json:"at"`
}

// OffersFile and SubscriptionsFile are where they are kept, in the data dir.
func OffersFile(dataDir string) string        { return filepath.Join(dataDir, "offers.json") }
func SubscriptionsFile(dataDir string) string { return filepath.Join(dataDir, "subscribed.json") }

// LoadOffers reads the offers; none is an empty map.
func LoadOffers(dataDir string) (map[string]Offer, error) {
	return loadJSONMap[Offer](OffersFile(dataDir))
}

// SaveOffers writes the offers atomically.
func SaveOffers(dataDir string, o map[string]Offer) error {
	return saveJSON(dataDir, OffersFile(dataDir), o)
}

// LoadSubscriptions reads the subscriptions; none is an empty map.
func LoadSubscriptions(dataDir string) (map[string]Subscription, error) {
	return loadJSONMap[Subscription](SubscriptionsFile(dataDir))
}

// SaveSubscriptions writes the subscriptions atomically.
func SaveSubscriptions(dataDir string, s map[string]Subscription) error {
	return saveJSON(dataDir, SubscriptionsFile(dataDir), s)
}

func loadJSONMap[T any](path string) (map[string]T, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]T{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]T{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return out, nil
}

func saveJSON(dataDir, path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ValidSubscribe reports whether s is a mode a subscription or policy takes.
func ValidSubscribe(s string) bool {
	return s == SubscribeOff || s == SyncStream || s == SyncLocal
}

// SubscribeDefault is the machine's policy for offered stores.
func (c *Config) SubscribeDefault() string {
	if c.Learn == nil || c.Learn.Subscribe == "" {
		return SubscribeOff
	}
	return c.Learn.Subscribe
}

// LearnedMode is what this machine does with an offered store: its
// subscription, else the policy.
func (c *Config) LearnedMode(name string, subs map[string]Subscription) string {
	if s, ok := subs[name]; ok && ValidSubscribe(s.Sync) {
		return s.Sync
	}
	return c.SubscribeDefault()
}

// WithLearned returns a copy of the config with the offered stores this
// machine subscribes to merged in, and their names. A store the config (or
// a request) already has is left alone. A learned store's group is the
// offer's: the one declared here when it includes this machine, else a
// group of this machine and the declared peers that offered it. Links
// between learned stores follow the offers, for children this machine
// also carries.
func (c *Config) WithLearned(offers map[string]Offer, subs map[string]Subscription) (*Config, []string) {
	out := *c
	out.StoreTree = maps.Clone(c.StoreTree)
	if out.StoreTree == nil {
		out.StoreTree = map[string]Store{}
	}
	out.Groups = maps.Clone(c.Groups)
	if out.Groups == nil {
		out.Groups = map[string]Group{}
	}
	out.LearnedFrom = map[string][]string{}
	out.LearnedGroups = map[string]bool{}

	names := slices.Sorted(maps.Keys(offers))
	var merged []string
	for _, name := range names {
		o := offers[name]
		if _, taken := out.StoreTree[name]; taken || checkName("store", name) != nil {
			continue
		}
		mode := c.LearnedMode(name, subs)
		if mode == SubscribeOff {
			continue
		}
		var from []string
		for _, p := range o.From {
			if _, declared := c.Peers[p]; declared && p != c.Peer {
				from = append(from, p)
			}
		}
		if len(from) == 0 || checkName("group", o.Group) != nil {
			continue
		}
		g, ok := out.Groups[o.Group]
		switch {
		case ok && !slices.Contains(g.Peers, c.Peer):
			continue // that group is not this machine's
		case ok:
		default:
			sort.Strings(from)
			out.Groups[o.Group] = Group{Peers: append([]string{c.Peer}, from...)}
			out.LearnedGroups[o.Group] = true
		}
		out.StoreTree[name] = Store{Group: o.Group, Sync: mode}
		out.LearnedFrom[name] = from
		merged = append(merged, name)
	}
	// Links: a learned parent shows its learned or declared children where
	// the offering peer has them, unless something here already links them.
	linked := map[string]bool{}
	for _, s := range c.StoreTree {
		for child := range s.Links {
			linked[child] = true
		}
	}
	for _, name := range merged {
		var links map[string]string
		for child, rel := range offers[name].Links {
			if _, carried := out.StoreTree[child]; !carried || linked[child] || child == name {
				continue
			}
			if links == nil {
				links = map[string]string{}
			}
			links[child] = rel
			linked[child] = true
		}
		if links != nil {
			s := out.StoreTree[name]
			s.Links = links
			out.StoreTree[name] = s
		}
	}
	// A link that makes the tree invalid (a cycle, a clash) is dropped
	// rather than costing the stores themselves.
	if out.checkLinkCycles() != nil {
		for _, name := range merged {
			s := out.StoreTree[name]
			s.Links = nil
			out.StoreTree[name] = s
		}
	}
	return &out, merged
}

// SameLearned reports whether two configs carry the same learned stores the
// same way, so a new offer that changes nothing needs no rebuild.
func SameLearned(a, b *Config) bool {
	if len(a.LearnedFrom) != len(b.LearnedFrom) {
		return false
	}
	for name := range a.LearnedFrom {
		if _, ok := b.LearnedFrom[name]; !ok {
			return false
		}
		sa, sb := a.StoreTree[name], b.StoreTree[name]
		if sa.Group != sb.Group || sa.Sync != sb.Sync || !maps.Equal(sa.Links, sb.Links) ||
			!slices.Equal(a.Groups[sa.Group].Peers, b.Groups[sb.Group].Peers) {
			return false
		}
	}
	return true
}

// Unlearned is the config without its learned stores (and the groups made
// for them), for working out what a change in offers would carry.
func (c *Config) Unlearned() *Config {
	if len(c.LearnedFrom) == 0 {
		return c
	}
	out := *c
	out.StoreTree = maps.Clone(c.StoreTree)
	out.Groups = maps.Clone(c.Groups)
	for name := range c.LearnedFrom {
		g := c.StoreTree[name].Group
		delete(out.StoreTree, name)
		if c.LearnedGroups[g] {
			delete(out.Groups, g)
		}
	}
	out.LearnedFrom, out.LearnedGroups = nil, nil
	return &out
}

// OfferList is every offered store with what this machine does with it, by
// name.
func (c *Config) OfferList(offers map[string]Offer, subs map[string]Subscription) []api.Offered {
	out := make([]api.Offered, 0, len(offers))
	for _, name := range slices.Sorted(maps.Keys(offers)) {
		o := offers[name]
		_, chosen := subs[name]
		a := api.Offered{Name: name, Group: o.Group, From: o.From, Seen: o.Seen, Mode: c.LearnedMode(name, subs), Subscribed: chosen}
		switch {
		case c.LearnedFrom[name] != nil:
			a.Carried = "learned"
		case c.RequestedBy[name] != "":
			a.Carried = "requested"
		case c.Declared(name):
			a.Carried = "declared"
		}
		out = append(out, a)
	}
	return out
}

// Subscribe records a choice for an offered store: sync "stream", "local"
// or "off", or "" to follow the policy again. It reports whether the
// carried stores change, so the caller rebuilds.
func (c *Config) Subscribe(name, sync, by string) (bool, error) {
	if sync != "" && !ValidSubscribe(sync) {
		return false, fmt.Errorf("sync %q: want stream, local or off", sync)
	}
	offersMu.Lock()
	defer offersMu.Unlock()
	offers, err := LoadOffers(c.DataDir)
	if err != nil {
		return false, err
	}
	if _, ok := offers[name]; !ok {
		return false, fmt.Errorf("no peer offers a store %q", name)
	}
	if c.Declared(name) && c.LearnedFrom[name] == nil {
		return false, fmt.Errorf("%q is in this machine's config, which decides how it is carried", name)
	}
	subs, err := LoadSubscriptions(c.DataDir)
	if err != nil {
		return false, err
	}
	if sync == "" {
		delete(subs, name)
	} else {
		subs[name] = Subscription{Sync: sync, By: by, At: time.Now().UTC()}
	}
	next, _ := c.Unlearned().WithLearned(offers, subs)
	if err := next.Validate(); err != nil {
		return false, err
	}
	if err := SaveSubscriptions(c.DataDir, subs); err != nil {
		return false, err
	}
	return !SameLearned(c, next), nil
}

// offersMu serializes the read-change-write of the offer and subscription
// files within a process.
var offersMu sync.Mutex

// UpdateOffers applies change to the offers file under the lock.
func UpdateOffers(dataDir string, change func(map[string]Offer) bool) (map[string]Offer, error) {
	offersMu.Lock()
	defer offersMu.Unlock()
	offers, err := LoadOffers(dataDir)
	if err != nil {
		return nil, err
	}
	if !change(offers) {
		return offers, nil
	}
	return offers, SaveOffers(dataDir, offers)
}
