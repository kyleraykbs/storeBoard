package syncer

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/kyleraykbs/storeboard/internal/config"
	"codeberg.org/kyleraykbs/storeboard/internal/streamfs"
)

func TestCreditWindow(t *testing.T) {
	c := &credit{wake: make(chan struct{}), last: time.Now()}
	ctx := context.Background()
	if err := c.take(ctx, blobWindow-10); err != nil {
		t.Fatal(err)
	}
	took := make(chan error, 1)
	go func() { took <- c.take(ctx, 100) }()
	select {
	case <-took:
		t.Fatal("a frame past the window went out")
	case <-time.After(50 * time.Millisecond):
	}
	c.grant(1 << 20)
	if err := <-took; err != nil {
		t.Fatal(err)
	}
	// One frame always fits an empty window, however large.
	empty := &credit{wake: make(chan struct{}), last: time.Now()}
	if err := empty.take(ctx, 2*blobWindow); err != nil {
		t.Fatal(err)
	}
	// A cancelled transfer stops waiting.
	cctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if err := empty.take(cctx, 1); err == nil {
		t.Fatal("take ignored a cancelled transfer")
	}
}

func TestCreditStallFallsBackToUnmetered(t *testing.T) {
	prev := creditStall
	creditStall = 80 * time.Millisecond
	defer func() { creditStall = prev }()
	c := &credit{wake: make(chan struct{}), last: time.Now()}
	_ = c.take(context.Background(), blobWindow)
	start := time.Now()
	if err := c.take(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("a window nobody acknowledges held the transfer forever")
	}
}

// The scenario from the field: a holder whose disk falls behind an upload
// stopped reading the link, and a streamed read - a video someone pressed
// play on - waited behind the upload for minutes. With a window, the read
// gets through while the upload waits.
func TestReadGetsThroughWhileReceiverIsBusy(t *testing.T) {
	epA, epB := twoEndpoints(t)
	a := newNode(t, "a", "b", epA, epB, "", map[string]config.Store{"docs": {Group: "g"}})
	b := newNode(t, "b", "a", epB, epA, "", map[string]config.Store{"docs": {Group: "g", Sync: config.SyncStream}})

	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	var stuck atomic.Int64
	hook := func(e *Engine, pf peerFrame) {
		if e == a.eng && pf.peer == "b" {
			stuck.Add(1)
			<-gate
		}
	}
	blobWorkHook.Store(&hook)
	defer blobWorkHook.Store(nil)

	startNode(t, a)
	startNode(t, b)
	waitPrepared(t, b.store("docs"))
	writeFile(t, a.root("docs")+"/small.txt", "small", time.Now())
	waitFor(t, testWait, "small.txt on the streamed side", func() bool {
		e := b.store("docs").db.Get("small.txt")
		return e != nil && !e.Deleted
	})

	// An upload far larger than every buffer between the two.
	m := &storeMutator{e: b.eng, fs: b.store("docs")}
	pf, err := m.Create("big.bin", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("v"), 1<<20)
	for i := 0; i < 96; i++ {
		if _, err := pf.WriteAt(chunk, int64(i)<<20); err != nil {
			t.Fatal(err)
		}
	}
	if err := pf.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, testWait, "the upload to reach the busy holder", func() bool { return stuck.Load() > 0 })
	time.Sleep(time.Second) // let everything that will queue, queue

	ent := b.store("docs").db.Get("small.txt")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := (&streamFetcher{e: b.eng, fs: b.store("docs")}).ReadRange(ctx, streamfs.Entry{Path: ent.Path, Blob: ent.Blob, Size: ent.Size}, 0, ent.Size)
		done <- result{data, err}
	}()
	var data []byte
	select {
	case r := <-done:
		data, err = r.data, r.err
	case <-time.After(15 * time.Second):
		release()
		t.Fatal("the read never got through while the holder was busy")
	}
	if err != nil || string(data) != "small" {
		t.Fatalf("read while the holder was busy = %q, %v after %v", data, err, time.Since(start))
	}
	t.Logf("read answered in %v with the holder's blob workers stuck", time.Since(start))
	release()
}
