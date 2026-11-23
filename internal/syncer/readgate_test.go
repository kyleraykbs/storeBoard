package syncer

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

// A read on a slow link that is still delivering is not given up: the
// holder keeps sending (other replies, here), so the read waits past
// readTimeout and gets its bytes. This used to fail half way through photos.
func TestSlowButLiveHolderDoesNotFailTheRead(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})
	old := readTimeout
	readTimeout = 200 * time.Millisecond
	defer func() { readTimeout = old }()
	s := &slowSender{e: n.eng, after: 3 * readTimeout}
	n.eng.mu.Lock()
	n.eng.sends["b"] = s
	n.eng.mu.Unlock()
	got, err := (&streamFetcher{e: n.eng, fs: n.store("docs")}).ReadRange(context.Background(), streamfs.Entry{Path: "x", Blob: "b", Size: 4}, 0, 4)
	if err != nil || string(got) != "data" {
		t.Fatalf("slow read = %q, %v", got, err)
	}
}

// slowSender answers a read after a delay, while the link keeps carrying
// other traffic the whole time.
type slowSender struct {
	e     *Engine
	after time.Duration
}

func (s *slowSender) Send(b []byte) error {
	var fr frame
	if err := json.Unmarshal(b, &fr); err != nil || fr.Type != "read" {
		return err
	}
	go func() {
		end := time.Now().Add(s.after)
		for time.Now().Before(end) {
			s.e.heard("b")
			time.Sleep(20 * time.Millisecond)
		}
		s.e.deliverRead(&frame{Type: "readData", ID: fr.ID, Data: []byte("data")})
	}()
	return nil
}
func (*slowSender) SendBulk([]byte) error { return nil }

// When every slot to a holder is taken, the next one goes to a read someone
// is blocked on, ahead of readahead that queued earlier.
func TestReadGateServesUrgentFirst(t *testing.T) {
	g := newReadGate(1)
	if err := g.acquire(context.Background(), func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	var order []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := func(name string, urgent bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.acquire(context.Background(), func() bool { return urgent }); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			g.release()
		}()
	}
	start("readahead", false)
	waitFor(t, time.Second, "the first waiter", func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.waiters) == 1 })
	start("blocked", true)
	waitFor(t, time.Second, "the second waiter", func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.waiters) == 2 })
	g.release()
	wg.Wait()
	if len(order) != 2 || order[0] != "blocked" {
		t.Fatalf("served %v, want the blocked read first", order)
	}
}

// A waiter that gives up leaves the queue and never holds a slot.
func TestReadGateCancelledWaiterLeaves(t *testing.T) {
	g := newReadGate(1)
	_ = g.acquire(context.Background(), func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- g.acquire(ctx, func() bool { return false }) }()
	waitFor(t, time.Second, "the waiter", func() bool { g.mu.Lock(); defer g.mu.Unlock(); return len(g.waiters) == 1 })
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled acquire succeeded")
	}
	g.release()
	if err := g.acquire(context.Background(), func() bool { return true }); err != nil {
		t.Fatal(err)
	}
}

// A read the reader cancelled is not served: the holder spends nothing on
// bytes nobody waits for.
func TestCancelledReadIsNotServed(t *testing.T) {
	n := newNode(t, "a", "b", "127.0.0.1:1", "127.0.0.1:2", "", map[string]config.Store{"docs": {Group: "g"}})
	var sent atomic.Int32
	n.eng.mu.Lock()
	n.eng.sends["b"] = countingSender{&sent}
	n.eng.mu.Unlock()
	n.eng.noteReadCancel("b", 7)
	n.eng.handleRead("b", &frame{Type: "read", ID: 7, Store: "docs", Path: "x", Offset: 0, Length: 4})
	if sent.Load() != 0 {
		t.Fatalf("cancelled read sent %d frames", sent.Load())
	}
	n.eng.handleRead("b", &frame{Type: "read", ID: 8, Store: "docs", Path: "x", Offset: 0, Length: 4})
	if sent.Load() != 1 {
		t.Fatalf("live read sent %d frames, want its reply", sent.Load())
	}
}

type countingSender struct{ n *atomic.Int32 }

func (c countingSender) Send([]byte) error     { c.n.Add(1); return nil }
func (c countingSender) SendBulk([]byte) error { c.n.Add(1); return nil }
