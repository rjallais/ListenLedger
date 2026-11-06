package eventsourcing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/db"
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

func TestJetStreamStore_AppendBatchMultiStreamRoundTrip(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	// Seed a third aggregate so scope sequences interleave: each stream's
	// first event anchors its own scope and the tail rides the batch's
	// atomic commit.
	if err := store.Append(ctx, "ar_bat_seed", 0, mustTestEvent(t, "ar_bat_seed", 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	a1 := mustTestEvent(t, "ar_bat_a", 1)
	a2 := mustTestEvent(t, "ar_bat_a", 2)
	b1 := mustTestEvent(t, "ar_bat_b", 1)
	b2 := mustTestEvent(t, "ar_bat_b", 2)
	err := store.AppendBatch(ctx, []StreamAppend{
		{StreamID: "ar_bat_a", ExpectedVersion: 0, Events: []Event{a1, a2}},
		{StreamID: "ar_bat_b", ExpectedVersion: 0, Events: []Event{b1, b2}},
	})
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	for _, streamID := range []string{"ar_bat_a", "ar_bat_b"} {
		loaded, err := store.Load(ctx, streamID)
		if err != nil || len(loaded) != 2 {
			t.Fatalf("Load %s = %d events, %v; want 2", streamID, len(loaded), err)
		}
		if loaded[0].Version != 1 || loaded[1].Version != 2 {
			t.Fatalf("Load %s order = v%d,v%d; want v1,v2", streamID, loaded[0].Version, loaded[1].Version)
		}
	}
}

func TestJetStreamStore_AppendBatchAtomicRollbackOnServerRejection(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	// Both streams pass client-side pre-flight; the server rejects the
	// commit because two events share one MsgID. The whole batch must roll
	// back server-side: the innocent stream stays empty too.
	a1 := mustTestEvent(t, "ar_bat_c", 1)
	b1 := mustTestEvent(t, "ar_bat_d", 1)
	b1.ID = a1.ID
	err := store.AppendBatch(ctx, []StreamAppend{
		{StreamID: "ar_bat_c", ExpectedVersion: 0, Events: []Event{a1}},
		{StreamID: "ar_bat_d", ExpectedVersion: 0, Events: []Event{b1}},
	})
	if err == nil {
		t.Fatal("AppendBatch with duplicate MsgID = nil; want rejection")
	}
	if errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("duplicate MsgID misclassified as conflict: %v", err)
	}
	for _, streamID := range []string{"ar_bat_c", "ar_bat_d"} {
		loaded, err := store.Load(ctx, streamID)
		if err != nil || len(loaded) != 0 {
			t.Fatalf("Load %s after rejected batch = %d events, %v; want 0, nil (atomic rollback)",
				streamID, len(loaded), err)
		}
	}
}

func TestJetStreamStore_AppendBatchConflictLeavesInnocentStreamUntouched(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	// One stream in the batch is already at v1: the server rejects the
	// anchor and the innocent stream stays empty.
	if err := store.Append(ctx, "ar_bat_e", 0, mustTestEvent(t, "ar_bat_e", 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fresh := mustTestEvent(t, "ar_bat_f", 1)
	// Version 1 at ExpectedVersion 0 passes client validation; the server
	// rejects the anchor (subject already holds v1) and rolls back the batch.
	stale := mustTestEvent(t, "ar_bat_e", 1)
	err := store.AppendBatch(ctx, []StreamAppend{
		{StreamID: "ar_bat_f", ExpectedVersion: 0, Events: []Event{fresh}},
		{StreamID: "ar_bat_e", ExpectedVersion: 0, Events: []Event{stale}},
	})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("AppendBatch with stale stream = %v; want ErrConcurrencyConflict", err)
	}
	loaded, err := store.Load(ctx, "ar_bat_f")
	if err != nil || len(loaded) != 0 {
		t.Fatalf("Load innocent stream = %d, %v; want 0, nil", len(loaded), err)
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

// partialFailStore simulates a mid-batch transport failure with unknown
// outcome (timeout / lost ack): it persists the first failAfter events via
// the wrapped store, then reports indeterminate as if the remaining
// publishes never answered.
type partialFailStore struct {
	Store
	failAfter int
}

func (s *partialFailStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	if len(events) > s.failAfter {
		if err := s.Store.Append(ctx, streamID, expectedVersion, events[:s.failAfter]...); err != nil {
			return err
		}
		return fmt.Errorf("%w: injected mid-batch failure", ErrAppendIndeterminate)
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

func TestCheckJetStreamParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

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

	sqliteStore := NewSQLiteStore(sqliteDB)
	if err := CheckJetStreamParity(ctx, sqliteDB, js); err != nil {
		t.Fatalf("empty parity = %v; want nil", err)
	}
	if pos, err := sqliteStore.GetCheckpoint(ctx, CheckpointJetStreamAuthority); err != nil || pos == 0 {
		t.Fatalf("authority marker = %d, %v; want recorded after passing gate", pos, err)
	}
	// Test-only reset between scenarios (no DeleteCheckpoint API): a zero
	// marker re-arms full verification for the phases below.
	if err := sqliteStore.SaveCheckpoint(ctx, CheckpointJetStreamAuthority, 0); err != nil {
		t.Fatalf("resetting marker: %v", err)
	}

	v1 := mustTestEvent(t, "ar_par_1", 1)
	v2 := mustTestEvent(t, "ar_par_1", 2)
	if err := sqliteStore.Append(ctx, "ar_par_1", 0, v1, v2); err != nil {
		t.Fatalf("SQLite Append: %v", err)
	}
	if err := CheckJetStreamParity(ctx, sqliteDB, js); err == nil {
		t.Fatal("parity with unpublished outbox rows should fail")
	}

	// Mirror the outbox set to JetStream and mark published: parity holds.
	jsStore := NewJetStreamStore(js)
	if err := jsStore.Append(ctx, "ar_par_1", 0, v1, v2); err != nil {
		t.Fatalf("JS Append: %v", err)
	}
	if err := sqliteStore.MarkPublished(ctx, []string{v1.ID, v2.ID}); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := CheckJetStreamParity(ctx, sqliteDB, js); err != nil {
		t.Fatalf("mirrored parity = %v; want nil", err)
	}
	if pos, err := sqliteStore.GetCheckpoint(ctx, CheckpointJetStreamAuthority); err != nil || pos == 0 {
		t.Fatalf("authority marker = %d, %v; want recorded after passing gate", pos, err)
	}

	// A foreign event in JetStream breaks count equality — but the recorded
	// flip short-circuits the gate now (steady state: JetStream ahead by
	// design, SQLite frozen). Boot must not brick on it.
	if _, err := js.Publish(ctx, messaging.SubjectDomainEvent("artist", "ar_par_x", "ArtistCreated"), []byte(`{}`)); err != nil {
		t.Fatalf("foreign Publish: %v", err)
	}
	if err := CheckJetStreamParity(ctx, sqliteDB, js); err != nil {
		t.Fatalf("post-flip parity = %v; want nil (skip)", err)
	}
}

// TestCheckJetStreamParity_RefusesBehindFrozenLog covers the steady-state
// guard's refusal path: marker recorded, but JetStream holds fewer events
// than the frozen SQLite log (nats/ store lost or restored without its
// tree). Boot must refuse instead of minting v1 events over missing history.
func TestCheckJetStreamParity_RefusesBehindFrozenLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

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

	// Frozen log with one event, authority marker recorded — the post-flip
	// world. The JetStream store is empty (0 msgs < 1 logged event).
	sqliteStore := NewSQLiteStore(sqliteDB)
	v1 := mustTestEvent(t, "ar_frozen_1", 1)
	if err := sqliteStore.Append(ctx, "ar_frozen_1", 0, v1); err != nil {
		t.Fatalf("SQLite Append: %v", err)
	}
	if err := sqliteStore.SaveCheckpoint(ctx, CheckpointJetStreamAuthority, 1); err != nil {
		t.Fatalf("recording marker: %v", err)
	}
	if err := CheckJetStreamParity(ctx, sqliteDB, js); err == nil {
		t.Fatal("post-flip boot with JetStream behind the frozen log should refuse")
	}
}

func TestCheckJetStreamParity_TipSubstitution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

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

	// Balanced substitution: same count, zero lag, but v2's identity differs
	// per store. Counts pass; the tip check must refuse.
	v1 := mustTestEvent(t, "ar_sub_1", 1)
	v2 := mustTestEvent(t, "ar_sub_1", 2)
	v2prime, err := NewEvent("ar_sub_1", "artist", 2, "ArtistMonthlyListenersScraped",
		map[string]any{"monthly_listeners": 200}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	sqliteStore := NewSQLiteStore(sqliteDB)
	if err := sqliteStore.Append(ctx, "ar_sub_1", 0, v1, v2); err != nil {
		t.Fatalf("SQLite Append: %v", err)
	}
	jsStore := NewJetStreamStore(js)
	if err := jsStore.Append(ctx, "ar_sub_1", 0, v1, v2prime); err != nil {
		t.Fatalf("JS Append: %v", err)
	}
	if err := sqliteStore.MarkPublished(ctx, []string{v1.ID, v2.ID}); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := CheckJetStreamParity(ctx, sqliteDB, js); err == nil {
		t.Fatal("parity with substituted tip should fail")
	} else if !strings.Contains(err.Error(), "ar_sub_1") {
		t.Fatalf("parity error should name the stream, got: %v", err)
	}
}

// outageStore fails Append as indeterminate once, then fails Load: the
// retry helper must preserve the unknown-outcome signal through the
// reconciliation failure instead of returning only the load error.
type outageStore struct {
	Store
	loadErr error
}

func (s *outageStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	return fmt.Errorf("%w: simulated timeout", ErrAppendIndeterminate)
}

func (s *outageStore) Load(ctx context.Context, streamID string) ([]Event, error) {
	return nil, s.loadErr
}

func TestAppendWithRetry_PreservesIndeterminateOnLoadFailure(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	flaky := &outageStore{Store: store, loadErr: errors.New("connection refused")}

	v1 := mustTestEvent(t, "ar_outage_1", 1)
	err := AppendWithRetry(ctx, flaky, "ar_outage_1", 0, v1)
	if err == nil {
		t.Fatal("AppendWithRetry should fail")
	}
	if !errors.Is(err, ErrAppendIndeterminate) {
		t.Fatalf("error should preserve ErrAppendIndeterminate, got: %v", err)
	}
}

func TestWrapPublishError(t *testing.T) {
	// Deterministic server rejection (APIError: e.g. DiscardNew stream
	// full): nothing persisted, so no indeterminate marking — callers must
	// not waste reconcile cycles on it.
	rejected := fmt.Errorf("publish: %w", &jetstream.APIError{Code: 500, ErrorCode: 10077})
	err := wrapPublishError("ar_w_1", "evt-1", rejected)
	if err == nil || errors.Is(err, ErrAppendIndeterminate) || errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("rejection should be plain, got: %v", err)
	}

	// Transport failure (no APIError): outcome unknown, must reconcile.
	timeout := fmt.Errorf("publish: %w", errors.New("timeout"))
	err = wrapPublishError("ar_w_1", "evt-1", timeout)
	if !errors.Is(err, ErrAppendIndeterminate) {
		t.Fatalf("timeout should be indeterminate, got: %v", err)
	}
}

func TestAppendWithRetry_DeadContextReturnsEarly(t *testing.T) {
	store, _ := setupTestJSStore(t)
	// Always loses the race, and counts reconciliation Loads: with a dead
	// context the guard must return before any Load runs.
	racer := &countingConflictStore{Store: store}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // dead before the first attempt

	v1 := mustTestEvent(t, "ar_dead_1", 1)
	err := AppendWithRetry(ctx, racer, "ar_dead_1", 0, v1)
	if err == nil {
		t.Fatal("AppendWithRetry with dead ctx should fail")
	}
	if !errors.Is(err, ErrConcurrencyConflict) || !errors.Is(err, context.Canceled) {
		t.Fatalf("error should preserve conflict + ctx signals, got: %v", err)
	}
	if racer.loads != 0 {
		t.Fatalf("dead ctx ran %d reconciliation Loads, want 0", racer.loads)
	}
}

// TestJetStreamStore_WatchStampsGlobalPosition pins the watcher contract:
// JetStream-mode events carry the DOMAIN_EVENTS stream sequence in
// GlobalPosition (from message metadata — the envelope does not hold it), so
// consumers can resume with Watch(ctx, lastPos).
func TestJetStreamStore_WatchStampsGlobalPosition(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_watch_1", 0, mustTestEvent(t, "ar_watch_1", 1)); err != nil {
		t.Fatalf("Append v1: %v", err)
	}
	if err := store.Append(ctx, "ar_watch_1", 1, mustTestEvent(t, "ar_watch_1", 2)); err != nil {
		t.Fatalf("Append v2: %v", err)
	}

	w, err := store.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = w.Stop() }()

	var got []Event
	deadline := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case evt, ok := <-w.Events():
			if !ok {
				t.Fatalf("watch channel closed early; got %d events, want 2", len(got))
			}
			got = append(got, evt)
		case <-deadline:
			t.Fatalf("timed out with %d events, want 2", len(got))
		}
	}
	if got[0].GlobalPosition != 1 || got[1].GlobalPosition != 2 {
		t.Fatalf("GlobalPosition = [%d %d], want [1 2]", got[0].GlobalPosition, got[1].GlobalPosition)
	}
}

// countingConflictStore loses every CAS race and counts Load calls.
type countingConflictStore struct {
	Store
	loads int
}

func (s *countingConflictStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	return fmt.Errorf("%w: simulated race", ErrConcurrencyConflict)
}

func (s *countingConflictStore) Load(ctx context.Context, streamID string) ([]Event, error) {
	s.loads++
	return s.Store.Load(ctx, streamID)
}

// TestJetStreamWatch_ClosesWhenConsumerDeleted pins the terminal contract:
// when the server deletes the watcher's consumer (what InactiveThreshold does
// after a reader blocked on the events channel), the watcher must close its
// channel so callers can resume with Watch(ctx, lastPos) instead of retrying
// fetches on a consumer that no longer exists.
func TestJetStreamWatch_ClosesWhenConsumerDeleted(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	if err := store.Append(ctx, "ar_watch", 0, mustTestEvent(t, "ar_watch", 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	w, err := store.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// The watcher is live once it delivers the appended event, which also
	// guarantees its consumer exists.
	select {
	case _, ok := <-w.Events():
		if !ok {
			t.Fatal("watcher channel closed before delivering the appended event")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watcher did not deliver the appended event")
	}

	stream, err := store.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	lister := stream.ConsumerNames(ctx)
	for name := range lister.Name() {
		if strings.HasPrefix(name, "watch_") {
			if err := stream.DeleteConsumer(ctx, name); err != nil {
				t.Fatalf("DeleteConsumer %s: %v", name, err)
			}
		}
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("ConsumerNames: %v", err)
	}

	// The channel must close (consumer-gone is terminal), not stay open with
	// an infinite fetch-retry loop.
	select {
	case _, ok := <-w.Events():
		if ok {
			t.Fatal("watcher delivered an event after consumer deletion")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("watcher channel never closed after consumer deletion")
	}
}

// TestJetStreamWatch_ConcurrentWatchersGetDistinctConsumers pins the
// consumer-name uniqueness contract: two watchers started in the same clock
// tick must each replay the full history from their own consumer, not share
// one cursor.
func TestJetStreamWatch_ConcurrentWatchersGetDistinctConsumers(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	if err := store.Append(ctx, "ar_watch2", 0, mustTestEvent(t, "ar_watch2", 1), mustTestEvent(t, "ar_watch2", 2)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	a, err := store.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("Watch a: %v", err)
	}
	defer func() { _ = a.Stop() }()
	b, err := store.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("Watch b: %v", err)
	}
	defer func() { _ = b.Stop() }()

	drain := func(w Watcher) int {
		n := 0
		deadline := time.After(10 * time.Second)
		for n < 2 {
			select {
			case _, ok := <-w.Events():
				if !ok {
					return n
				}
				n++
			case <-deadline:
				return n
			}
		}
		return n
	}
	if n := drain(a); n != 2 {
		t.Errorf("watcher a received %d events, want 2", n)
	}
	if n := drain(b); n != 2 {
		t.Errorf("watcher b received %d events, want 2 (a shared consumer would starve one watcher)", n)
	}
}

func TestJetStreamStore_LoadPopulatesGlobalPositionAndHeaders(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	v1 := mustTestEvent(t, "ar_gp_1", 1)
	if err := store.Append(ctx, "ar_gp_1", 0, v1); err != nil {
		t.Fatalf("Append v1: %v", err)
	}
	// Single-event fast path in loadSince must stamp GlobalPosition.
	single, err := store.Load(ctx, "ar_gp_1")
	if err != nil || len(single) != 1 {
		t.Fatalf("Load single = %d, %v; want 1", len(single), err)
	}
	if single[0].GlobalPosition != 1 {
		t.Fatalf("single[0].GlobalPosition = %d, want 1", single[0].GlobalPosition)
	}

	v2 := mustTestEvent(t, "ar_gp_1", 2)
	if err := store.AppendSequenced(ctx, "ar_gp_1", 1, uint64(single[0].GlobalPosition), v2); err != nil {
		t.Fatalf("AppendSequenced v2: %v", err)
	}
	multi, err := store.Load(ctx, "ar_gp_1")
	if err != nil || len(multi) != 2 {
		t.Fatalf("Load multi = %d, %v; want 2", len(multi), err)
	}
	if multi[0].GlobalPosition != 1 || multi[1].GlobalPosition != 2 {
		t.Fatalf("multi GlobalPosition = [%d %d], want [1 2]", multi[0].GlobalPosition, multi[1].GlobalPosition)
	}
}

func TestJetStreamStore_WatchWithFiltersAndCatchUpSignal(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_wf_1", 0, mustTestEvent(t, "ar_wf_1", 1)); err != nil {
		t.Fatalf("Append ar_wf_1: %v", err)
	}
	albumEvt, err := NewEvent("al_wf_1", "album", 1, "AlbumCreated", map[string]any{"title": "Lateralus"}, nil)
	if err != nil {
		t.Fatalf("NewEvent album: %v", err)
	}
	if err := store.Append(ctx, "al_wf_1", 0, albumEvt); err != nil {
		t.Fatalf("Append al_wf_1: %v", err)
	}

	caughtUp := make(chan struct{})
	w, err := store.Watch(ctx, 0, WithWatchFilters("album.al_wf_1"), WithCatchUpSignal(caughtUp))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = w.Stop() }()

	select {
	case evt := <-w.Events():
		if evt.StreamID != "al_wf_1" || evt.StreamType != "album" {
			t.Fatalf("got unexpected event %+v, want al_wf_1/album", evt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for filtered album event")
	}

	select {
	case <-caughtUp:
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up signal was never closed")
	}
}

func TestJetStreamStore_WatchWithMultipleOverlappingFilters(t *testing.T) {
	store, ctx := setupTestJSStore(t)

	if err := store.Append(ctx, "ar_wf_2", 0, mustTestEvent(t, "ar_wf_2", 1)); err != nil {
		t.Fatalf("Append ar_wf_2: %v", err)
	}
	albumEvt, err := NewEvent("al_wf_2", "album", 1, "AlbumCreated", map[string]any{"title": "Fear Inoculum"}, nil)
	if err != nil {
		t.Fatalf("NewEvent album: %v", err)
	}
	if err := store.Append(ctx, "al_wf_2", 0, albumEvt); err != nil {
		t.Fatalf("Append al_wf_2: %v", err)
	}

	// Overlapping filters: "artist" covers all artists, "artist.ar_wf_2" is a subset.
	// nats-server would reject this if placed in FilterSubjects.
	caughtUp := make(chan struct{})
	w, err := store.Watch(ctx, 0, WithWatchFilters("artist", "artist.ar_wf_2"), WithCatchUpSignal(caughtUp))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = w.Stop() }()

	select {
	case evt := <-w.Events():
		if evt.StreamID != "ar_wf_2" || evt.StreamType != "artist" {
			t.Fatalf("got unexpected event %+v, want ar_wf_2/artist", evt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for artist event")
	}

	select {
	case <-caughtUp:
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up signal was never closed")
	}
}
