package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
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
