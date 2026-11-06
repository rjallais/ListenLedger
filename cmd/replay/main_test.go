package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

func TestParseStreamTypes(t *testing.T) {
	got, err := parseStreamTypes("artist,album,song,batch")
	if err != nil {
		t.Fatalf("parseStreamTypes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d streams, want 4", len(got))
	}

	if _, err := parseStreamTypes("artist,unknown"); err == nil {
		t.Fatal("unknown stream type should error")
	}
	if _, err := parseStreamTypes(""); err == nil {
		t.Fatal("empty selection should error")
	}
}

func TestIsLiveDataDir(t *testing.T) {
	t.Setenv("PB_DATA_DIR", "/tmp/replay-test-live")
	if !isLiveDataDir("/tmp/replay-test-live") {
		t.Fatal("matching PB_DATA_DIR should count as live")
	}
	if !isLiveDataDir("/tmp/replay-test-live/") {
		t.Fatal("trailing slash must not bypass the live guard")
	}
	if isLiveDataDir("/tmp/replay-test-staging-copy") {
		t.Fatal("staging copy should not count as live")
	}
	if isLiveDataDir("/tmp/replay-test-staging-copy/") {
		t.Fatal("staging copy with trailing slash should not count as live")
	}
}

func seedReplayArtist(t *testing.T, ctx context.Context, dataDir, artistID string) {
	t.Helper()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	store := eventsourcing.NewSQLiteStore(database)
	evt, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, artist.EventTypeArtistCreated,
		artist.CreatedPayload{ID: artistID, Name: "Replay Test", SpotifyID: "sp_replay", GenreGroup: "rock_metal", ListStatus: "included"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := store.Append(ctx, artistID, 0, evt); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func openReplayStore(t *testing.T, ctx context.Context, dataDir string) (*eventsourcing.SQLiteStore, func()) {
	t.Helper()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	return eventsourcing.NewSQLiteStore(database), func() { _ = database.Close() }
}

func TestReplayAppliesAndCheckpoints(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	selected := map[string]struct{}{artist.StreamTypeArtist: {}}

	seedReplayArtist(t, ctx, dataDir, "ar_replay_cp")

	if err := replay(ctx, logger, dataDir, selected, false); err != nil {
		t.Fatalf("replay: %v", err)
	}

	store, closeStore := openReplayStore(t, ctx, dataDir)
	defer closeStore()
	pos, err := store.GetCheckpoint(ctx, eventsourcing.CheckpointArtistProjection)
	if err != nil {
		t.Fatalf("GetCheckpoint(artist): %v", err)
	}
	if pos <= 0 {
		t.Fatalf("artist checkpoint = %d, want > 0", pos)
	}

	// Resume mode re-folds idempotently without reset and keeps the frontier.
	if err := replay(ctx, logger, dataDir, selected, true); err != nil {
		t.Fatalf("replay --since-checkpoint: %v", err)
	}
	pos2, err := store.GetCheckpoint(ctx, eventsourcing.CheckpointArtistProjection)
	if err != nil {
		t.Fatalf("GetCheckpoint(artist) after resume: %v", err)
	}
	if pos2 < pos {
		t.Fatalf("checkpoint regressed %d -> %d", pos, pos2)
	}

	// Dry run touches nothing and reports the frontier.
	if err := dryRun(ctx, logger, dataDir, selected); err != nil {
		t.Fatalf("dryRun: %v", err)
	}
}

func TestTailCountsRespectsCheckpoints(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	selected := map[string]struct{}{artist.StreamTypeArtist: {}}

	seedReplayArtist(t, ctx, dataDir, "ar_tail_a")
	seedReplayArtist(t, ctx, dataDir, "ar_tail_b")

	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	store := eventsourcing.NewSQLiteStore(database)

	// No checkpoint: everything is pending.
	tails, err := tailCounts(ctx, store, selected)
	if err != nil {
		t.Fatalf("tailCounts: %v", err)
	}
	if tails[artist.StreamTypeArtist] != 2 {
		t.Fatalf("tail without checkpoint = %d, want 2", tails[artist.StreamTypeArtist])
	}

	// Checkpoint past the first event: only the second stream is pending.
	first, err := store.Load(ctx, "ar_tail_a")
	if err != nil || len(first) == 0 {
		t.Fatalf("load ar_tail_a: %v", len(first))
	}
	if err := store.SaveCheckpoint(ctx, eventsourcing.CheckpointArtistProjection, first[0].GlobalPosition); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	tails, err = tailCounts(ctx, store, selected)
	if err != nil {
		t.Fatalf("tailCounts: %v", err)
	}
	if tails[artist.StreamTypeArtist] != 1 {
		t.Fatalf("tail past first event = %d, want 1", tails[artist.StreamTypeArtist])
	}
}

func appendReplayListeners(t *testing.T, ctx context.Context, dataDir, artistID string, listeners int64) {
	t.Helper()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	store := eventsourcing.NewSQLiteStore(database)
	repo := artist.NewRepository(store)
	agg, err := repo.Load(ctx, artistID)
	if err != nil {
		t.Fatalf("Load %s: %v", artistID, err)
	}
	if err := agg.RecordMonthlyListeners(listeners, "test", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if _, err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

// TestReplayIncrementalTouchesOnlyNewFacts proves resume cost scales with new
// facts: after a full apply, appending to one of two streams and resuming
// re-projects only the touched stream while converging its row.
func TestReplayIncrementalTouchesOnlyNewFacts(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	selected := map[string]struct{}{artist.StreamTypeArtist: {}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	seedReplayArtist(t, ctx, dataDir, "ar_incr_a")
	seedReplayArtist(t, ctx, dataDir, "ar_incr_b")
	if err := replay(ctx, quiet, dataDir, selected, false); err != nil {
		t.Fatalf("replay: %v", err)
	}

	appendReplayListeners(t, ctx, dataDir, "ar_incr_b", 777000)

	var logs bytes.Buffer
	resumeLogger := slog.New(slog.NewTextHandler(&logs, nil))
	if err := replay(ctx, resumeLogger, dataDir, selected, true); err != nil {
		t.Fatalf("replay --since-checkpoint: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "ar_incr_b") {
		t.Fatalf("resume should re-project touched stream ar_incr_b, logs:\n%s", out)
	}
	if strings.Contains(out, "ar_incr_a") {
		t.Fatalf("resume must skip untouched stream ar_incr_a, logs:\n%s", out)
	}

	store, closeStore := openReplayStore(t, ctx, dataDir)
	defer closeStore()
	agg, err := artist.NewRepository(store).Load(ctx, "ar_incr_b")
	if err != nil {
		t.Fatalf("Load ar_incr_b: %v", err)
	}
	if agg.MonthlyListeners != 777000 {
		t.Fatalf("resumed listeners = %d, want 777000", agg.MonthlyListeners)
	}
	assertReplayedRowListeners(t, ctx, dataDir, "ar_incr_b", 777000)
}

// TestReplaySinceCheckpointRepairsDriftRowAtTip simulates a row that drifted
// while its read_model_positions row sits at the stream tip: Project would
// skip every event, so --since-checkpoint must clear the position first and
// re-fold to convergence.
func TestReplaySinceCheckpointRepairsDriftRowAtTip(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	selected := map[string]struct{}{artist.StreamTypeArtist: {}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	seedReplayArtist(t, ctx, dataDir, "ar_drift_x")
	if err := replay(ctx, quiet, dataDir, selected, false); err != nil {
		t.Fatalf("replay: %v", err)
	}

	appendReplayListeners(t, ctx, dataDir, "ar_drift_x", 555000)

	// Drift the row and push its stored position well past the stream tip.
	{
		database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
		if err != nil {
			t.Fatalf("SetupDB: %v", err)
		}
		if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
			if _, err := tx.Prep(`UPDATE artists SET monthly_listeners = 1 WHERE id = 'ar_drift_x';`).Step(); err != nil {
				return err
			}
			_, err := tx.Prep(`UPDATE read_model_positions SET version = version + 100 WHERE stream_id = 'ar_drift_x';`).Step()
			return err
		}); err != nil {
			t.Fatalf("drifting row: %v", err)
		}
		if err := database.Close(); err != nil {
			t.Fatalf("close after drift: %v", err)
		}
	}

	if err := replay(ctx, quiet, dataDir, selected, true); err != nil {
		t.Fatalf("replay --since-checkpoint: %v", err)
	}
	assertReplayedRowListeners(t, ctx, dataDir, "ar_drift_x", 555000)
}

// assertReplayedRowListeners verifies the projected SQLite row converged,
// not just the aggregate: resume must fold facts into read models.
func assertReplayedRowListeners(t *testing.T, ctx context.Context, dataDir, artistID string, want int64) {
	t.Helper()
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	var got int64
	var found bool
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT monthly_listeners FROM artists WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		found = true
		got = stmt.ColumnInt64(0)
		return nil
	}); err != nil {
		t.Fatalf("read artists row: %v", err)
	}
	if !found {
		t.Fatalf("projected row %s missing after resume", artistID)
	}
	if got != want {
		t.Fatalf("projected row listeners = %d, want %d", got, want)
	}
}

func TestReplayFromJetStream(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	selected := map[string]struct{}{artist.StreamTypeArtist: {}}

	// Phase 1: write the log straight to JetStream (no SQLite events), then
	// shut the server down — the rebuild must find the facts on restart.
	natsDir := filepath.Join(dataDir, "nats")
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: natsDir,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("Connect: %v", err)
	}
	js, err := messaging.NewJetStream(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("NewJetStream: %v", err)
	}
	if err := messaging.EnsureDomainEventsStream(ctx, js); err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("EnsureDomainEventsStream: %v", err)
	}
	evt, err := eventsourcing.NewEvent("ar_js_replay", artist.StreamTypeArtist, 1, artist.EventTypeArtistCreated,
		artist.CreatedPayload{ID: "ar_js_replay", Name: "JS Replay", SpotifyID: "sp_js", GenreGroup: "rock_metal", ListStatus: "included"}, nil)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("NewEvent: %v", err)
	}
	if err := eventsourcing.NewJetStreamStore(js).Append(ctx, "ar_js_replay", 0, evt); err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("JS Append: %v", err)
	}
	nc.Close()
	ns.Shutdown()

	// Phase 2: rebuild read models from the JetStream log alone.
	if err := replayFromJetStream(ctx, logger, dataDir, selected); err != nil {
		t.Fatalf("replayFromJetStream: %v", err)
	}
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	var name string
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT name FROM artists WHERE id = 'ar_js_replay';")
		defer func() { _ = stmt.Reset() }()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return fmt.Errorf("no artists row for ar_js_replay")
		}
		name = stmt.ColumnText(0)
		return nil
	}); err != nil {
		t.Fatalf("read artists row: %v", err)
	}
	if name != "JS Replay" {
		t.Fatalf("artists name = %q, want %q", name, "JS Replay")
	}

	// Enumeration helper finds the seeded stream; dry run is read-only.
	// Shut the first server down before dryRunJS: two embedded servers on
	// one StoreDir corrupt it, and dryRunJS opens its own.
	js2, cleanup, err := openReplayJetStream(ctx, dataDir)
	if err != nil {
		t.Fatalf("openReplayJetStream: %v", err)
	}
	infos, err := jsStreamInfos(ctx, js2, selected)
	cleanup()
	if err != nil {
		t.Fatalf("jsStreamInfos: %v", err)
	}
	if len(infos) != 1 || infos[0].ID != "ar_js_replay" || infos[0].Type != artist.StreamTypeArtist {
		t.Fatalf("jsStreamInfos = %+v, want [ar_js_replay/artist]", infos)
	}
	if err := dryRunJS(ctx, logger, dataDir, selected); err != nil {
		t.Fatalf("dryRunJS: %v", err)
	}
}

// TestReplaySinceCheckpointRepairsDriftedSongRow verifies that --since-checkpoint
// clears read_model_positions for touched streams and re-folds from version 0,
// converging columns updated by domain events without deleting existing rows
// (preserving catalog rows predating the event log).
func TestReplaySinceCheckpointRepairsDriftedSongRow(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	selected := map[string]struct{}{song.StreamTypeSong: {}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Create song in SQLite store
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	s, err := song.NewSong("sg_drift_1", "Original Title", "Artist", "Album", "2024-01-01", "album", "", 2024, 0, 0, false)
	if err != nil {
		t.Fatalf("NewSong: %v", err)
	}
	if _, err := s.LinkTidalID("tidal-123", "test", 1.0); err != nil {
		t.Fatalf("LinkTidalID: %v", err)
	}
	if err := store.Append(ctx, "sg_drift_1", 0, s.UncommittedEvents()...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = database.Close()

	// Full initial replay creates the songs row with tidal_id
	if err := replay(ctx, quiet, dataDir, selected, false); err != nil {
		t.Fatalf("initial replay: %v", err)
	}

	// Corrupt tidal_id in the database row and advance read_model_positions to tip
	database, err = db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		if _, err := tx.Prep(`UPDATE songs SET tidal_id = 'corrupted' WHERE id = 'sg_drift_1';`).Step(); err != nil {
			return err
		}
		_, err := tx.Prep(`UPDATE read_model_positions SET version = version + 10 WHERE stream_id = 'sg_drift_1';`).Step()
		return err
	}); err != nil {
		t.Fatalf("corrupting song row: %v", err)
	}
	_ = database.Close()

	// Append a new event to touch the stream after checkpoint
	database, err = db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store = eventsourcing.NewSQLiteStore(database)
	repo := song.NewRepository(store)
	sLoaded, err := repo.Load(ctx, "sg_drift_1")
	if err != nil {
		t.Fatalf("Load song: %v", err)
	}
	if err := sLoaded.SetRecent(true, 1, 1); err != nil {
		t.Fatalf("SetRecent: %v", err)
	}
	if _, err := repo.Save(ctx, sLoaded); err != nil {
		t.Fatalf("Save song: %v", err)
	}
	_ = database.Close()

	// Replay with --since-checkpoint clears position and re-folds, restoring tidal_id
	if err := replay(ctx, quiet, dataDir, selected, true); err != nil {
		t.Fatalf("replay --since-checkpoint: %v", err)
	}

	// Verify tidal_id converged back to "tidal-123" and is_recent updated to 1
	database, err = db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()
	var restoredTidal string
	var isRecent int64
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT tidal_id, is_recent FROM songs WHERE id = 'sg_drift_1';")
		defer func() { _ = stmt.Reset() }()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return fmt.Errorf("songs row not found")
		}
		restoredTidal = stmt.ColumnText(0)
		isRecent = stmt.ColumnInt64(1)
		return nil
	}); err != nil {
		t.Fatalf("read songs row: %v", err)
	}
	if restoredTidal != "tidal-123" {
		t.Fatalf("restored tidal_id = %q, want %q", restoredTidal, "tidal-123")
	}
	if isRecent != 1 {
		t.Fatalf("is_recent = %d, want 1", isRecent)
	}
}
