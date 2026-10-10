package eventsourcing

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"ListenLedger/internal/messaging"
)

func setupTestJSStore(t *testing.T) (*JetStreamStore, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	storeDir := filepath.Join(t.TempDir(), "nats-store")
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: storeDir,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := messaging.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	if err := messaging.EnsureDomainEventsStream(ctx, js); err != nil {
		t.Fatalf("EnsureDomainEventsStream: %v", err)
	}
	return NewJetStreamStore(js), ctx
}

func mustTestEvent(t *testing.T, streamID string, version int64) Event {
	t.Helper()
	evt, err := NewEvent(streamID, "artist", version, "ArtistMonthlyListenersScraped",
		map[string]any{"monthly_listeners": 100}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	return evt
}

func TestJetStreamStore_RoundTrip(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_js_1", 0, mustTestEvent(t, "ar_js_1", 1)); err != nil {
		t.Fatalf("Append v1: %v", err)
	}
	if err := store.Append(ctx, "ar_js_1", 1, mustTestEvent(t, "ar_js_1", 2)); err != nil {
		t.Fatalf("Append v2: %v", err)
	}

	loaded, err := store.Load(ctx, "ar_js_1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 || loaded[0].Version != 1 || loaded[1].Version != 2 {
		t.Fatalf("Load = %d events, want [v1 v2]", len(loaded))
	}
	var pl struct {
		MonthlyListeners int `json:"monthly_listeners"`
	}
	if err := DecodeEventPayload(loaded[0], &pl); err != nil || pl.MonthlyListeners != 100 {
		t.Fatalf("payload decode = %+v, %v; want 100", pl, err)
	}

	if got, err := store.Load(ctx, "ar_missing"); err != nil || len(got) != 0 {
		t.Fatalf("Load missing = %d, %v; want 0, nil", len(got), err)
	}
}

func TestJetStreamStore_VersionConflict(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_js_2", 5, mustTestEvent(t, "ar_js_2", 1)); !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("Append wrong version = %v; want ErrConcurrencyConflict", err)
	}
	if err := store.Append(ctx, "ar_js_2", 0); err != nil {
		t.Fatalf("Append empty = %v; want nil", err)
	}
}

func TestJetStreamStore_BatchPrevalidationNoPartialWrite(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	v1 := mustTestEvent(t, "ar_js_4", 1)
	v3 := mustTestEvent(t, "ar_js_4", 3) // skew: forces pre-validation failure
	if err := store.Append(ctx, "ar_js_4", 0, v1, v3); !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("Append skewed batch = %v; want ErrConcurrencyConflict", err)
	}
	if got, err := store.Load(ctx, "ar_js_4"); err != nil || len(got) != 0 {
		t.Fatalf("Load after rejected batch = %d, %v; want 0, nil (no partial write)", len(got), err)
	}
}

func TestJetStreamStore_ConcurrentAppendConflict(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	// Two writers read the same empty stream, both append v1. The loser's
	// stream-wide CAS must surface ErrConcurrencyConflict.
	w1 := mustTestEvent(t, "ar_js_5", 1)
	w2, err := NewEvent("ar_js_5", "artist", 1, "ArtistMonthlyListenersScraped",
		map[string]any{"monthly_listeners": 200}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, "ar_js_5", 0, w1); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if err := store.Append(ctx, "ar_js_5", 0, w2); !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("stale Append = %v; want ErrConcurrencyConflict", err)
	}
	loaded, err := store.Load(ctx, "ar_js_5")
	if err != nil || len(loaded) != 1 {
		t.Fatalf("Load = %d, %v; want exactly the winner", len(loaded), err)
	}
}

func TestUnmarshalEvent_RoundTrip(t *testing.T) {
	orig := mustTestEvent(t, "ar_js_3", 7)
	data, err := orig.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	got, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatalf("UnmarshalEvent: %v", err)
	}
	if got.ID != orig.ID || got.StreamID != "ar_js_3" || got.Version != 7 || got.EventType != orig.EventType {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}
