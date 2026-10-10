package handlers

import (
	"context"
	"os"
	"testing"
	"time"
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
