package eventsourcing

import (
	"context"
	"errors"
	"fmt"
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

func TestJetStreamStore_LoadFiltersByStream(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_filter_a", 0, mustTestEvent(t, "ar_filter_a", 1)); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if err := store.Append(ctx, "ar_filter_b", 0, mustTestEvent(t, "ar_filter_b", 1)); err != nil {
		t.Fatalf("Append b: %v", err)
	}
	if err := store.Append(ctx, "ar_filter_a", 1, mustTestEvent(t, "ar_filter_a", 2)); err != nil {
		t.Fatalf("Append a v2: %v", err)
	}

	loaded, err := store.Load(ctx, "ar_filter_a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("Load = %d events, want 2", len(loaded))
	}
	for _, evt := range loaded {
		if evt.StreamID != "ar_filter_a" {
			t.Fatalf("Load leaked event from stream %q", evt.StreamID)
		}
	}
	if loaded[0].Version != 1 || loaded[1].Version != 2 {
		t.Fatalf("Load order = v%d,v%d; want v1,v2", loaded[0].Version, loaded[1].Version)
	}
}

func TestJetStreamStore_NoCrossAggregateContention(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	// Interleaved appends to different aggregates must not conflict: the
	// OCC scope is per-aggregate, not stream-wide.
	if err := store.Append(ctx, "ar_x_1", 0, mustTestEvent(t, "ar_x_1", 1)); err != nil {
		t.Fatalf("Append x1: %v", err)
	}
	if err := store.Append(ctx, "ar_x_2", 0, mustTestEvent(t, "ar_x_2", 1)); err != nil {
		t.Fatalf("Append x2: %v", err)
	}
	if err := store.Append(ctx, "ar_x_1", 1, mustTestEvent(t, "ar_x_1", 2)); err != nil {
		t.Fatalf("Append x1 v2: %v", err)
	}
}

func TestJetStreamStore_MultiEventBatchWithInterleaving(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	// Seed another aggregate first so scope sequences are non-consecutive
	// within the target aggregate: base+i anchoring would false-conflict on
	// the second event after the first persisted.
	if err := store.Append(ctx, "ar_m_other", 0, mustTestEvent(t, "ar_m_other", 1)); err != nil {
		t.Fatalf("Append other: %v", err)
	}
	v1 := mustTestEvent(t, "ar_m_main", 1)
	v2 := mustTestEvent(t, "ar_m_main", 2)
	if err := store.Append(ctx, "ar_m_main", 0, v1, v2); err != nil {
		t.Fatalf("multi-event Append with interleaving: %v", err)
	}
	loaded, err := store.Load(ctx, "ar_m_main")
	if err != nil || len(loaded) != 2 {
		t.Fatalf("Load = %d, %v; want 2", len(loaded), err)
	}
	if loaded[0].Version != 1 || loaded[1].Version != 2 {
		t.Fatalf("Load order = v%d,v%d; want v1,v2", loaded[0].Version, loaded[1].Version)
	}
}

func TestJetStreamStore_SameIDAcrossTypesRejected(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_shared", 0, mustTestEvent(t, "ar_shared", 1)); err != nil {
		t.Fatalf("Append artist v1: %v", err)
	}
	// Same ID, different aggregate type, same version: the OCC scope covers
	// the ID across types (like Load), so the double-mint must conflict.
	albumV1, err := NewEvent("ar_shared", "album", 1, "AlbumCreated",
		map[string]any{"title": "X"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, "ar_shared", 0, albumV1); !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("cross-type Append = %v; want ErrConcurrencyConflict", err)
	}
	// Correct version but wrong type: the aggregate-type guard fires.
	albumV2, err := NewEvent("ar_shared", "album", 2, "AlbumCreated",
		map[string]any{"title": "X"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, "ar_shared", 1, albumV2); err == nil || errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("wrong-type Append = %v; want non-conflict type error", err)
	}
}

// partialFailStore simulates a mid-batch transport failure: it persists the
// first failAfter events via the wrapped store, then reports a conflict as
// if the batch's remaining publishes never reached the server.
type partialFailStore struct {
	Store
	failAfter int
}

func (s *partialFailStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	if len(events) > s.failAfter {
		if err := s.Store.Append(ctx, streamID, expectedVersion, events[:s.failAfter]...); err != nil {
			return err
		}
		return fmt.Errorf("%w: injected mid-batch failure", ErrConcurrencyConflict)
	}
	return s.Store.Append(ctx, streamID, expectedVersion, events...)
}

func TestAppendWithRetry_ResumesUnwrittenTail(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	flaky := &partialFailStore{Store: store, failAfter: 1}

	v1 := mustTestEvent(t, "ar_retry_1", 1)
	v2 := mustTestEvent(t, "ar_retry_1", 2)
	v3 := mustTestEvent(t, "ar_retry_1", 3)
	if err := AppendWithRetry(ctx, flaky, "ar_retry_1", 0, v1, v2, v3); err != nil {
		t.Fatalf("AppendWithRetry = %v; want nil", err)
	}
	loaded, err := store.Load(ctx, "ar_retry_1")
	if err != nil || len(loaded) != 3 {
		t.Fatalf("Load = %d, %v; want 3", len(loaded), err)
	}
	for i, want := range []string{v1.ID, v2.ID, v3.ID} {
		if loaded[i].ID != want {
			t.Fatalf("loaded[%d].ID = %s, want %s", i, loaded[i].ID, want)
		}
	}
}

func TestAppendWithRetry_AllPersistedIsSuccess(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	v1 := mustTestEvent(t, "ar_retry_2", 1)
	v2 := mustTestEvent(t, "ar_retry_2", 2)
	if err := AppendWithRetry(ctx, store, "ar_retry_2", 0, v1, v2); err != nil {
		t.Fatalf("first AppendWithRetry: %v", err)
	}
	// Same batch again (e.g. ack lost after success): IDs verify present.
	if err := AppendWithRetry(ctx, store, "ar_retry_2", 0, v1, v2); err != nil {
		t.Fatalf("redundant AppendWithRetry = %v; want nil", err)
	}
}

func TestAppendWithRetry_ForeignWriterWins(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_retry_3", 0, mustTestEvent(t, "ar_retry_3", 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	mine := mustTestEvent(t, "ar_retry_3", 1)
	if err := AppendWithRetry(ctx, store, "ar_retry_3", 0, mine); !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("AppendWithRetry = %v; want ErrConcurrencyConflict", err)
	}
}

func TestAppendWithRetry_ValidationPassesThrough(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	other := mustTestEvent(t, "ar_retry_other", 1)
	if err := AppendWithRetry(ctx, store, "ar_retry_4", 0, other); err == nil || errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("AppendWithRetry = %v; want plain validation error", err)
	}
}
