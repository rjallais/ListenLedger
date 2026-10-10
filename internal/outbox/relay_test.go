package outbox

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"ListenLedger/internal/db"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

func setupTestRelay(t *testing.T) (*eventsourcing.SQLiteStore, *Relay, context.Context) {
	t.Helper()
	ctx := context.Background()
	tmpDir := t.TempDir()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

	ns := startTestOutboxNATS(t, tmpDir)
	nc := connectTestOutboxNATS(t, ns.ClientURL())
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})

	js, err := messaging.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	if err := messaging.EnsureDomainEventsStream(ctx, js); err != nil {
		t.Fatalf("EnsureDomainEventsStream: %v", err)
	}

	store := eventsourcing.NewSQLiteStore(sqliteDB)
	relay := NewRelay(slog.Default(), sqliteDB, js)
	return store, relay, ctx
}

func appendTestEvent(t *testing.T, ctx context.Context, store *eventsourcing.SQLiteStore, streamID string, version int64) eventsourcing.Event {
	t.Helper()
	evt, err := eventsourcing.NewEvent(streamID, "artist", version, "ArtistMonthlyListenersScraped",
		map[string]any{"monthly_listeners": 100}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	var expected int64
	if version > 1 {
		expected = version - 1
	}
	if err := store.Append(ctx, streamID, expected, evt); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return evt
}

func TestRelay_PublishesAndMarks(t *testing.T) {
	store, relay, ctx := setupTestRelay(t)

	appendTestEvent(t, ctx, store, "ar_relay_1", 1)
	appendTestEvent(t, ctx, store, "ar_relay_1", 2)

	if n, err := store.UnpublishedCount(ctx); err != nil || n != 2 {
		t.Fatalf("UnpublishedCount = %d, %v; want 2", n, err)
	}

	published, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}
	if published != 2 {
		t.Fatalf("RelayOnce published = %d, want 2", published)
	}

	if n, _ := store.UnpublishedCount(ctx); n != 0 {
		t.Fatalf("UnpublishedCount after relay = %d, want 0", n)
	}

	stream, err := relay.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Stream.Info: %v", err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("DOMAIN_EVENTS msgs = %d, want 2", info.State.Msgs)
	}

	// Second pass is a no-op.
	if n, err := relay.RelayOnce(ctx); err != nil || n != 0 {
		t.Fatalf("RelayOnce(second) = %d, %v; want 0, nil", n, err)
	}

	// Publish frontier is checkpointed for resume/lag observability.
	pos, err := store.GetCheckpoint(ctx, eventsourcing.CheckpointDomainEventsRelay)
	if err != nil {
		t.Fatalf("GetCheckpoint(relay): %v", err)
	}
	if pos <= 0 {
		t.Fatalf("relay checkpoint = %d, want > 0", pos)
	}
}

func TestRelay_CrashCatchupAfterSyncPublish(t *testing.T) {
	store, relay, ctx := setupTestRelay(t)

	// Commit an event, then simulate the projection's synchronous publish
	// succeeding (the normal path) — the relay must dedup, not duplicate.
	evt := appendTestEvent(t, ctx, store, "ar_relay_2", 1)
	subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
	data, err := evt.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if _, err := messaging.PublishDomainEvent(ctx, relay.js, subject, evt.ID, data); err != nil {
		t.Fatalf("sync PublishDomainEvent: %v", err)
	}

	published, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}
	if published != 1 {
		t.Fatalf("RelayOnce published = %d, want 1 (duplicate-acked)", published)
	}

	stream, err := relay.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Stream.Info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("DOMAIN_EVENTS msgs = %d, want 1 (no duplicate stored)", info.State.Msgs)
	}
	if n, _ := store.UnpublishedCount(ctx); n != 0 {
		t.Fatalf("UnpublishedCount = %d, want 0", n)
	}
}

func startTestOutboxNATS(t *testing.T, storeDir string) *natsserver.Server {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoSigs:    true,
		NoLog:     true,
		JetStream: true,
		StoreDir:  filepath.Join(storeDir, "nats-store"),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server failed to become ready")
	}
	return ns
}

func connectTestOutboxNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	return nc
}
