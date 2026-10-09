package eventsourcing_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ListenLedger/internal/eventsourcing"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
)

func setupTestStore(t *testing.T) (*eventsourcing.SQLiteStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "es_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	migrations := []string{
		`CREATE TABLE IF NOT EXISTS events (
			id TEXT PRIMARY KEY,
			stream_id TEXT NOT NULL,
			stream_type TEXT NOT NULL,
			version INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			metadata TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL,
			CONSTRAINT uq_stream_version UNIQUE (stream_id, version)
		);`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			stream_id TEXT PRIMARY KEY,
			stream_type TEXT NOT NULL,
			version INTEGER NOT NULL,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS outbox (
			event_id TEXT PRIMARY KEY,
			published_at TEXT,
			FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_position INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL DEFAULT (datetime('now'))
		);`,
	}

	dbFilename := filepath.Join(dir, "test.sqlite")
	db, err := toolbeltdb.NewDatabase(
		context.Background(),
		toolbeltdb.DatabaseWithFilename(dbFilename),
		toolbeltdb.DatabaseWithMigrations(migrations),
	)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("failed to create test db: %v", err)
	}

	store := eventsourcing.NewSQLiteStore(db)
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	return store, cleanup
}

func TestSQLiteStore_AppendAndLoad(t *testing.T) {
	ctx := context.Background()
	store, cleanup := setupTestStore(t)
	defer cleanup()

	streamID := "artist_test_1"

	e1, err := eventsourcing.NewEvent(streamID, "artist", 1, "ArtistRegistered", map[string]string{"name": "Alice in Chains"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating event: %v", err)
	}

	// 1. Append version 1 with expectedVersion 0
	if err := store.Append(ctx, streamID, 0, e1); err != nil {
		t.Fatalf("failed to append first event: %v", err)
	}

	// 2. Concurrency Conflict: try to append version 1 again with expectedVersion 0
	e1Duplicate, _ := eventsourcing.NewEvent(streamID, "artist", 1, "ArtistRegistered", map[string]string{"name": "Duplicate"}, nil)
	if err := store.Append(ctx, streamID, 0, e1Duplicate); err == nil {
		t.Fatal("expected concurrency conflict error, got nil")
	}

	// 3. Append version 2 with expectedVersion 1
	e2, err := eventsourcing.NewEvent(streamID, "artist", 2, "ArtistMonthlyListenersScraped", map[string]any{"monthly_listeners": 4500000}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating second event: %v", err)
	}
	if err := store.Append(ctx, streamID, 1, e2); err != nil {
		t.Fatalf("failed to append second event: %v", err)
	}

	// 4. Load all events
	events, err := store.Load(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to load events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Version != 1 || events[1].Version != 2 {
		t.Fatalf("unexpected event versions: %d, %d", events[0].Version, events[1].Version)
	}
}

func TestDecodePayload_RONTimeAndLegacyJSON(t *testing.T) {
	type payload struct {
		ScrapedAt time.Time `json:"scraped_at"`
	}

	want := time.Date(2026, time.September, 24, 12, 30, 45, 123456789, time.UTC)
	event, err := eventsourcing.NewEvent("artist-1", "artist", 1, "ArtistMonthlyListenersScraped", payload{ScrapedAt: want}, nil)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}

	var got payload
	if err := eventsourcing.DecodePayload(event.Payload, &got); err != nil {
		t.Fatalf("DecodePayload() for RON payload error = %v", err)
	}
	if !got.ScrapedAt.Equal(want) {
		t.Fatalf("DecodePayload() scraped_at = %s, want %s", got.ScrapedAt, want)
	}

	legacyJSON := []byte(`{"scraped_at":"2026-09-24T12:30:45.123456789Z"}`)
	if err := eventsourcing.DecodePayload(legacyJSON, &got); err != nil {
		t.Fatalf("DecodePayload() for legacy JSON payload error = %v", err)
	}
	if !got.ScrapedAt.Equal(want) {
		t.Fatalf("DecodePayload() legacy scraped_at = %s, want %s", got.ScrapedAt, want)
	}
}

func TestEventBytes_LegacyJSONPayload(t *testing.T) {
	// Migration-backfilled rows carry plain JSON payloads with PB-style IDs.
	// Bytes() must normalize them to RON so the outbox relay can publish the
	// backfill (previously: "converting embedded RON value to JSON" on every
	// row, 0 published, retry forever).
	evt := eventsourcing.Event{
		ID:         "evt_2MQQARDPUF4AK",
		StreamID:   "mztievqhmfgtox0",
		StreamType: "artist",
		Version:    1,
		EventType:  "ArtistCreated",
		Payload:    []byte(`{"id":"mztievqhmfgtox0","name":"Måneskin","spotify_id":"0lAWpj5szCSwM4rUMHYmrr","genre_group":"rock_metal","list_status":"included"}`),
		Metadata:   []byte(`{}`),
		CreatedAt:  time.Date(2026, time.September, 30, 22, 0, 0, 0, time.UTC),
	}
	data, err := evt.Bytes()
	if err != nil {
		t.Fatalf("Bytes() for legacy JSON payload error = %v", err)
	}
	if len(data) == 0 || !bytes.Contains(data, []byte("neskin")) {
		t.Fatalf("Bytes() output missing payload content: %q", data)
	}

	// RON-native events keep working through the same path.
	native, err := eventsourcing.NewEvent("artist-1", "artist", 1, "ArtistCreated",
		map[string]any{"id": "artist-1", "name": "N"}, nil)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if _, err := native.Bytes(); err != nil {
		t.Fatalf("Bytes() for RON payload error = %v", err)
	}
}

func TestSQLiteStore_SnapshotRoundTripAndLoadAfter(t *testing.T) {
	ctx := context.Background()
	store, cleanup := setupTestStore(t)
	defer cleanup()

	streamID := "artist_snap_1"

	e1, err := eventsourcing.NewEvent(streamID, "artist", 1, "ArtistRegistered", map[string]string{"name": "Nirvana"}, nil)
	if err != nil {
		t.Fatalf("NewEvent e1: %v", err)
	}
	e2, err := eventsourcing.NewEvent(streamID, "artist", 2, "ArtistMonthlyListenersScraped", map[string]any{"monthly_listeners": 100}, nil)
	if err != nil {
		t.Fatalf("NewEvent e2: %v", err)
	}
	e3, err := eventsourcing.NewEvent(streamID, "artist", 3, "ArtistMonthlyListenersScraped", map[string]any{"monthly_listeners": 200}, nil)
	if err != nil {
		t.Fatalf("NewEvent e3: %v", err)
	}
	if err := store.Append(ctx, streamID, 0, e1, e2, e3); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Closing the books: snapshot at v2, then replay only v3.
	snap := eventsourcing.Snapshot{
		StreamID:   streamID,
		StreamType: "artist",
		Version:    2,
		Payload:    []byte(`{"monthly_listeners":100}`),
		CreatedAt:  time.Now().UTC(),
	}
	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := store.LoadSnapshot(ctx, streamID)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.Version != 2 || got.StreamType != "artist" {
		t.Fatalf("snapshot = %+v, want version 2 artist", got)
	}

	after, err := store.LoadAfter(ctx, streamID, got.Version)
	if err != nil {
		t.Fatalf("LoadAfter: %v", err)
	}
	if len(after) != 1 || after[0].Version != 3 {
		t.Fatalf("LoadAfter len=%d, want 1 event at v3", len(after))
	}

	if _, err := store.LoadSnapshot(ctx, "missing"); err == nil {
		t.Fatal("LoadSnapshot(missing) should error")
	}
}

func TestGlobalPositionAndCheckpoints(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Initial checkpoint should be 0
	cp, err := store.GetCheckpoint(ctx, "test_proj")
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}
	if cp != 0 {
		t.Fatalf("initial checkpoint = %d, want 0", cp)
	}

	// Append across two different streams
	e1, err := eventsourcing.NewEvent("stream-1", "artist", 1, "ArtistRegistered", map[string]any{"name": "One"}, nil)
	if err != nil {
		t.Fatalf("NewEvent e1: %v", err)
	}
	e2, err := eventsourcing.NewEvent("stream-2", "song", 1, "SongCreated", map[string]any{"title": "Track"}, nil)
	if err != nil {
		t.Fatalf("NewEvent e2: %v", err)
	}
	if err := store.Append(ctx, "stream-1", 0, e1); err != nil {
		t.Fatalf("Append stream-1: %v", err)
	}
	if err := store.Append(ctx, "stream-2", 0, e2); err != nil {
		t.Fatalf("Append stream-2: %v", err)
	}

	// Verify events loaded by stream carry GlobalPosition > 0
	loaded, err := store.Load(ctx, "stream-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].GlobalPosition <= 0 {
		t.Fatalf("expected GlobalPosition > 0, got %d", loaded[0].GlobalPosition)
	}

	// Read from global position 0
	events, err := store.LoadFromGlobalPosition(ctx, 0, 10)
	if err != nil {
		t.Fatalf("LoadFromGlobalPosition(0): %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].StreamID != "stream-1" || events[1].StreamID != "stream-2" {
		t.Fatalf("unexpected event order: %+v", events)
	}
	if events[1].GlobalPosition <= events[0].GlobalPosition {
		t.Fatalf("expected monotonically increasing position, got %d then %d", events[0].GlobalPosition, events[1].GlobalPosition)
	}

	// Update and read checkpoint
	if err := store.SaveCheckpoint(ctx, "test_proj", events[1].GlobalPosition); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	updatedCp, err := store.GetCheckpoint(ctx, "test_proj")
	if err != nil {
		t.Fatalf("GetCheckpoint updated: %v", err)
	}
	if updatedCp != events[1].GlobalPosition {
		t.Fatalf("got checkpoint %d, want %d", updatedCp, events[1].GlobalPosition)
	}

	// Reading after checkpoint returns no events
	afterEvents, err := store.LoadFromGlobalPosition(ctx, updatedCp, 10)
	if err != nil {
		t.Fatalf("LoadFromGlobalPosition after checkpoint: %v", err)
	}
	if len(afterEvents) != 0 {
		t.Fatalf("expected 0 events after checkpoint, got %d", len(afterEvents))
	}
}

func TestSQLiteStore_ReplaceRefusesWithoutOptIn(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	streamID := "rewrite_guard"
	e1, err := eventsourcing.NewEvent(streamID, "artist", 1, "ArtistRegistered", map[string]string{"name": "Guard"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, streamID, 0, e1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	loaded, err := store.Load(ctx, streamID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	t.Setenv("LISTENLEDGER_ALLOW_EVENT_REWRITE", "")
	if err := store.Replace(ctx, streamID, loaded); err == nil {
		t.Fatal("Replace without opt-in should fail, got nil")
	}

	t.Setenv("LISTENLEDGER_ALLOW_EVENT_REWRITE", "1")
	if err := store.Replace(ctx, streamID, loaded); err != nil {
		t.Fatalf("Replace with opt-in: %v", err)
	}
}
