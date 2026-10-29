package syncer

import (
	"encoding/json"
	"fmt"
	"time"
)

// A streamed store has no history and no bytes of its own, so the verbs that
// read or restore versions are answered by a holder: the machine that keeps
// both. The request is peer-to-peer - the config already authorizes every peer
// in the store's group - and it travels on the same correlated channel as a
// ranged read.

// verbsRequest is one forwarded verb.
type verbsRequest struct {
	Action string `json:"action"`
	Path   string `json:"path"`
	Blob   string `json:"blob,omitempty"`
	At     int64  `json:"at,omitempty"`
	To     string `json:"to,omitempty"`
}

// verbRoundTrip sends one verbs request to a holder and returns its payload.
func (e *Engine) verbRoundTrip(peer string, fs *StoreState, req verbsRequest) ([]byte, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ch := make(chan readReply, 1)
	e.mu.Lock()
	e.readSeq++
	id := e.readSeq
	e.reads[id] = ch
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.reads, id)
		e.mu.Unlock()
	}()

	if err := e.sendTo(peer, frame{Type: "verbs", ID: id, Store: fs.name, Action: req.Action, Data: payload}); err != nil {
		return nil, err
	}
	select {
	case <-e.closed:
		return nil, fmt.Errorf("engine closed")
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.data, nil
	}
}

// forwardVerbs asks the fastest reachable holder to answer a verb, failing
// over to the next one. It never invents an answer when no holder responds.
func (e *Engine) forwardVerbs(fs *StoreState, req verbsRequest, out any) error {
	peers := e.holdersFor(fs)
	if len(peers) == 0 {
		return fmt.Errorf("store %q is streamed on this machine and no holder is reachable", fs.name)
	}
	var firstErr error
	for _, peer := range peers {
		data, err := e.verbRoundTrip(peer, fs, req)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := json.Unmarshal(data, out); err != nil {
			return err
		}
		return nil
	}
	return firstErr
}

// handleVerbs answers a peer's verb on this machine's own history.
func (e *Engine) handleVerbs(peer string, fr *frame) {
	fs := e.byName[fr.Store]
	if fs == nil {
		e.replyVerbs(peer, fr, nil, "unknown store")
		return
	}
	if !e.storeSharedWith(fs, peer) {
		e.replyVerbs(peer, fr, nil, "peer is not in the store's group")
		return
	}
	if fs.stream {
		// Only a holder keeps history; a peer asking another streamed peer
		// would just pass the request along.
		e.replyVerbs(peer, fr, nil, "store has no local history")
		return
	}
	var req verbsRequest
	if err := json.Unmarshal(fr.Data, &req); err != nil {
		e.replyVerbs(peer, fr, nil, "bad request")
		return
	}

	switch req.Action {
	case "versions":
		if req.Path != "" && !validRel(req.Path) {
			e.replyVerbs(peer, fr, nil, "unsafe path")
			return
		}
		v, err := e.Versions(fr.Store, req.Path)
		if err != nil {
			e.replyVerbs(peer, fr, nil, err.Error())
			return
		}
		e.replyVerbs(peer, fr, v, "")
	case "restore":
		if !validRel(req.Path) {
			e.replyVerbs(peer, fr, nil, "unsafe path")
			return
		}
		// The holder acts as the store's owner: it is the machine that keeps
		// the bytes, and the peer is already authorized by the group.
		uid := fs.perm.uid
		var at time.Time
		if req.At != 0 {
			at = time.Unix(0, req.At)
		}
		res, err := e.Restore(fr.Store, uid, req.Path, req.Blob, at, req.To)
		if err != nil {
			e.replyVerbs(peer, fr, nil, err.Error())
			return
		}
		e.replyVerbs(peer, fr, res, "")
	default:
		e.replyVerbs(peer, fr, nil, "unknown action")
	}
}

func (e *Engine) replyVerbs(peer string, fr *frame, payload any, errMsg string) {
	out := frame{Type: "verbsResult", ID: fr.ID, Store: fr.Store, Path: fr.Path, Error: errMsg}
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			out.Error = "cannot encode the answer"
		} else {
			out.Data = b
		}
	}
	if err := e.sendTo(peer, out); err != nil {
		e.logger.Debug("verbs reply", "peer", peer, "store", fr.Store, "error", err)
	}
}
