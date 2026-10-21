package handlers

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"
)

// TestBatchProgress_SQLiteProjection verifies the restart-safe path: batches
// live in SQLite, so a fresh Handler on the same data dir resumes them.
func TestBatchProgress_SQLiteProjection(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	if h.batchStore == nil {
		t.Fatal("batchStore should be wired when db is set")
	}

	snapshot, err := h.createBatchProgress(context.Background(), []string{"ar_b1", "ar_b2"}, map[string]int{"P5RockIncluded": 2})
	if err != nil {
		t.Fatalf("createBatchProgress: %v", err)
	}
	if snapshot.Total != 2 || snapshot.Completed != 0 || snapshot.Done {
		t.Fatalf("initial = %+v", snapshot)
	}

	// Members reference real pending artists so the throttled reconcile
	// leaves them alone; completions below come only from mark calls.
	seedQueueTestArtist(t, ctx, h, "ar_b1", "pending")
	seedQueueTestArtist(t, ctx, h, "ar_b2", "pending")

	// Failed is terminal: advances.
	h.markBatchArtistDone("ar_b1", "failed")
	got, ok := h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok || got.Completed != 1 {
		t.Fatalf("after failed = %+v, %t", got, ok)
	}

	// Pending never advances.
	h.markBatchArtistDone("ar_b2", "pending")
	got, ok = h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok || got.Completed != 1 {
		t.Fatalf("after pending = %+v, %t", got, ok)
	}

	// Active snapshot tracks the unfinished batch (queue-stats path).
	active, ok := h.getActiveBatchSnapshot(ctx)
	if !ok || active.ID != snapshot.ID {
		t.Fatalf("active = %+v, %t", active, ok)
	}

	// "Restart": new Handler over the same DB resumes the batch.
	h2 := New(nil, nil, nil, h.cfg, WithDatabase(h.db))
	h2.markBatchArtistDone("ar_b2", "idle")
	resumed, ok := h2.getBatchSnapshot(ctx, snapshot.ID)
	if !ok {
		t.Fatal("batch missing after handler restart")
	}
	if resumed.Completed != 2 || !resumed.Done {
		t.Fatalf("resumed = %+v, want Completed=2 Done=true", resumed)
	}

	// Finished batch is no longer active.
	if _, ok := h2.getActiveBatchSnapshot(ctx); ok {
		t.Fatal("finished batch should not be active")
	}
	latest, ok := h2.getLatestBatchSnapshot(ctx)
	if !ok || !latest.Done {
		t.Fatalf("latest = %+v, %t", latest, ok)
	}
}

func TestBatchReconcile_SQLiteProjection(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	// Seed artists table: ar_r1 already finished (missed event), ar_r2 pending.
	seedQueueTestArtist(t, ctx, h, "ar_r1", "idle")
	seedQueueTestArtist(t, ctx, h, "ar_r2", "pending")

	snapshot, err := h.createBatchProgress(context.Background(), []string{"ar_r1", "ar_r2"}, nil)
	if err != nil {
		t.Fatalf("createBatchProgress: %v", err)
	}
	// Snapshot read triggers throttled reconcile; force it by resetting the timer.
	h.batchMu.Lock()
	h.lastBatchReconcile = h.lastBatchReconcile.Add(-time.Hour)
	h.batchMu.Unlock()

	got, ok := h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok {
		t.Fatal("batch missing")
	}
	if got.Completed != 1 {
		t.Fatalf("reconciled = %+v, want Completed=1 (ar_r1 via artists table)", got)
	}
}

// TestBatchReconciler_BackgroundConverges proves crash recovery without NATS:
// a member whose artist already left pending is completed by the background
// loop alone, with no markBatchArtistDone event and no snapshot read.
func TestBatchReconciler_BackgroundConverges(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	seedQueueTestArtist(t, ctx, h, "ar_bg1", "idle")

	snapshot, err := h.createBatchProgress(ctx, []string{"ar_bg1"}, nil)
	if err != nil {
		t.Fatalf("createBatchProgress: %v", err)
	}
	if snapshot.Completed != 0 {
		t.Fatalf("initial = %+v, want Completed=0", snapshot)
	}

	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.RunBatchReconciler(rctx, 10*time.Millisecond)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	got, ok, err := h.batchStore.Snapshot(ctx, snapshot.ID)
	if err != nil || !ok {
		t.Fatalf("batch missing after background reconcile: %v", err)
	}
	if got.Completed != 1 || !got.Done {
		t.Fatalf("background reconciled = %+v, want Completed=1 Done=true", got)
	}
}

// TestBatchProgress_LargeBatchAndExpiredCallerContext ensures that createBatchProgress
// succeeds even if the caller's request context was canceled or depleted by prior steps,
// and correctly provisions large batches (e.g. 100 artists).
func TestBatchProgress_LargeBatchAndExpiredCallerContext(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel() // deliberately pre-expired caller context

	artistIDs := make([]string, 100)
	for i := range artistIDs {
		artistIDs[i] = "ar_bulk_" + string(rune('a'+(i%26))) + "_" + string(rune('0'+(i%10))) + "_" + time.Now().Format("150405.000000000")
	}

	snap, err := h.createBatchProgress(expiredCtx, artistIDs, map[string]int{"P3RockNotAdded": 100})
	if err != nil {
		t.Fatalf("createBatchProgress failed with pre-expired caller context: %v", err)
	}
	if snap.Total != 100 || snap.Completed != 0 || snap.Done {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	ctx := context.Background()
	got, ok := h.getBatchSnapshot(ctx, snap.ID)
	if !ok {
		t.Fatalf("batch %s not found in store", snap.ID)
	}
	if got.Total != 100 {
		t.Fatalf("got total = %d, want 100", got.Total)
	}
}

// TestBatchRefreshArtistSelectSQLOrderMatchesPriority locks the SQLite batch
// selection order to priority.Determine via prioritizeArtistJobs. Drift here
// would change the LIMIT subset and mislabel the batch priority stats.
func TestBatchRefreshArtistSelectSQLOrderMatchesPriority(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	seedArtist := func(id, listStatus, genre, fetchStatus string, listeners int64) *core.Record {
		t.Helper()
		err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, '');")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, id)
			stmt.BindText(2, "Artist "+id)
			stmt.BindText(3, "sp_"+id)
			stmt.BindInt64(4, listeners)
			stmt.BindText(5, genre)
			stmt.BindText(6, listStatus)
			stmt.BindText(7, fetchStatus)
			_, err := stmt.Step()
			return err
		})
		if err != nil {
			t.Fatalf("seed artist %s: %v", id, err)
		}

		record := core.NewRecord(core.NewBaseCollection("artists"))
		record.Id = id
		record.Set("list_status", listStatus)
		record.Set("genre_group", genre)
		record.Set("fetch_status", fetchStatus)
		record.Set("monthly_listeners", listeners)
		return record
	}

	records := []*core.Record{
		seedArtist("ar_wait_rock", "waiting", "rock_metal", "idle", 20),
		seedArtist("ar_wait_other", "waiting", "everything_else", "idle", 15),
		seedArtist("ar_recent_rock_high", "recently_added", "rock_metal", "idle", 90),
		seedArtist("ar_recent_rock_low", "recently_added", "rock_metal", "idle", 10),
		seedArtist("ar_recent_other", "recently_added", "everything_else", "idle", 80),
		seedArtist("ar_not_rock", "not_added", "rock_metal", "idle", 70),
		seedArtist("ar_not_other", "not_added", "everything_else", "idle", 60),
		seedArtist("ar_incl_rock", "included", "rock_metal", "idle", 50),
		seedArtist("ar_incl_other", "included", "everything_else", "idle", 40),
	}
	// Pending artists are excluded by the WHERE clause; with the highest
	// listener count this row would lead the order if the filter regressed.
	seedArtist("ar_pending", "included", "rock_metal", "pending", 100)

	var got []string
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(batchRefreshArtistSelectSQL)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano))
		stmt.BindInt64(2, 100)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			got = append(got, stmt.ColumnText(0))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("select batch artists: %v", err)
	}

	want := make([]string, 0, len(records))
	for _, job := range prioritizeArtistJobs(records) {
		want = append(want, job.Record.Id)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("SQLite order = %v, want %v", got, want)
	}
}
