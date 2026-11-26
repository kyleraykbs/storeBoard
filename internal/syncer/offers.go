package syncer

import (
	"maps"
	"slices"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
)

// Offers (DESIGN: learned stores): on connecting, each side tells a declared
// peer which of its stores that peer is in a group for, so a device can
// carry stores nobody declared on it. The receiver keeps the offers in its
// data dir; what it carries is decided when the daemon builds itself
// (config.WithLearned), and a change in offers that changes that asks for a
// rebuild (Engine.SetLearnedChanged).

const featureOffers = "offers"

// storeOffer is one store in an "offers" frame.
type storeOffer struct {
	Name    string            `json:"name"`
	Group   string            `json:"group"`
	Holders []string          `json:"holders,omitempty"`
	Links   map[string]string `json:"links,omitempty"`
}

// SetLearnedChanged sets what to call when offers change what this machine
// would carry: the daemon rebuilds itself with them.
func (e *Engine) SetLearnedChanged(f func()) {
	e.mu.Lock()
	e.learnedChanged = f
	e.mu.Unlock()
}

// sendOffers tells a declared peer the stores it may carry with this
// machine: those whose group includes it.
func (e *Engine) sendOffers(peer string) {
	if _, declared := e.cfg.Peers[peer]; !declared {
		return
	}
	var out []storeOffer
	for _, name := range slices.Sorted(maps.Keys(e.cfg.StoreTree)) {
		st := e.cfg.StoreTree[name]
		v, ok := e.cfg.StoreView(name)
		if !ok {
			continue
		}
		if !slices.Contains(e.cfg.Groups[v.Group].Peers, peer) {
			continue
		}
		out = append(out, storeOffer{Name: name, Group: v.Group, Holders: v.Holders, Links: st.Links})
	}
	if err := e.sendTo(peer, frame{Type: "offers", Offers: out}); err != nil {
		e.logger.Debug("send offers", "peer", peer, "error", err)
	}
}

// handleOffers records what a declared peer offers now, replacing what it
// offered before.
func (e *Engine) handleOffers(peer string, fr *frame) {
	if _, declared := e.cfg.Peers[peer]; !declared || peer == e.cfg.Peer {
		return
	}
	now := time.Now().UTC()
	offers, err := config.UpdateOffers(e.cfg.DataDir, func(offers map[string]config.Offer) bool {
		changed := false
		got := map[string]bool{}
		for _, o := range fr.Offers {
			got[o.Name] = true
			cur, ok := offers[o.Name]
			next := cur
			next.Group, next.Links, next.Holders = o.Group, o.Links, o.Holders
			if !slices.Contains(next.From, peer) {
				next.From = append(slices.Clone(next.From), peer)
				slices.Sort(next.From)
			}
			differs := !ok || cur.Group != next.Group || !maps.Equal(cur.Links, next.Links) || !slices.Equal(cur.From, next.From) ||
				!slices.Equal(cur.Holders, next.Holders)
			// Seen moves at most hourly, so a reconnect is not a write.
			if differs || now.Sub(cur.Seen) > time.Hour {
				next.Seen = now
				changed = true
			}
			offers[o.Name] = next
		}
		for name, o := range offers {
			if got[name] || !slices.Contains(o.From, peer) {
				continue
			}
			o.From = slices.DeleteFunc(slices.Clone(o.From), func(p string) bool { return p == peer })
			if len(o.From) == 0 {
				delete(offers, name)
			} else {
				offers[name] = o
			}
			changed = true
		}
		return changed
	})
	if err != nil {
		e.logger.Warn("offers", "error", err)
		return
	}
	e.checkLearned(offers)
}

// checkLearned asks for a rebuild when the offers now change what this
// machine carries.
func (e *Engine) checkLearned(offers map[string]config.Offer) {
	subs, err := config.LoadSubscriptions(e.cfg.DataDir)
	if err != nil {
		e.logger.Warn("subscriptions", "error", err)
		return
	}
	next, _ := e.cfg.Unlearned().WithLearned(offers, subs)
	if config.SameLearned(e.cfg, next) || next.Validate() != nil {
		return
	}
	e.mu.Lock()
	f := e.learnedChanged
	e.mu.Unlock()
	if f != nil {
		e.logger.Info("offered stores changed what this machine carries")
		f()
	}
}
