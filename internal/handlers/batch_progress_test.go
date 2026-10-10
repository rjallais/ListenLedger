package handlers

import (
	"os"
	"testing"

	"ListenLedger/internal/priority"
)

func TestMarkBatchArtistDoneOnlyOnSuccess(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	ctx := t.Context()
	snapshot, err := h.createBatchProgress(t.Context(), []string{"artist-1", "artist-2"}, map[string]int{"P0Queued": 2})
	if err != nil {
		t.Fatalf("createBatchProgress: %v", err)
	}
	if snapshot.Completed != 0 {
		t.Fatalf("initial Completed = %d, want 0", snapshot.Completed)
	}
	// Pending rows keep the background reconcile from completing members
	// behind the test's back; completions below come only from mark calls.
	seedQueueTestArtist(t, ctx, h, "artist-1", "pending")
	seedQueueTestArtist(t, ctx, h, "artist-2", "pending")

	// Failed attempts should advance progress (terminal state).
	h.markBatchArtistDone("artist-1", "failed")
	failedSnapshot, ok := h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after failed update")
	}
	if failedSnapshot.Completed != 1 {
		t.Fatalf("Completed after failed status = %d, want 1", failedSnapshot.Completed)
	}

	// Pending updates should not advance progress.
	h.markBatchArtistDone("artist-2", "pending")
	pendingSnapshot, ok := h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after pending update")
	}
	if pendingSnapshot.Completed != 1 {
		t.Fatalf("Completed after pending status = %d, want 1", pendingSnapshot.Completed)
	}

	// Successful update (idle) should advance progress and complete the batch.
	h.markBatchArtistDone("artist-2", "idle")
	successSnapshot, ok := h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after success update")
	}
	if successSnapshot.Completed != 2 {
		t.Fatalf("Completed after success status = %d, want 2", successSnapshot.Completed)
	}
	if !successSnapshot.Done {
		t.Fatalf("Done after all artists processed = false, want true")
	}

	// Duplicate event for already-done artist should not increment again.
	h.markBatchArtistDone("artist-1", "idle")
	duplicateSnapshot, ok := h.getBatchSnapshot(ctx, snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after duplicate update")
	}
	if duplicateSnapshot.Completed != 2 {
		t.Fatalf("Completed after duplicate = %d, want 2", duplicateSnapshot.Completed)
	}
}

func TestEnqueueBatchRefreshJobs_Empty(t *testing.T) {
	h := &Handler{}
	ids, stats := h.enqueueBatchRefreshJobs(t.Context(), nil, 10)
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %d", len(ids))
	}
	if len(stats) != 0 {
		t.Fatalf("expected empty stats, got %v", stats)
	}

	ids, stats = h.enqueueBatchRefreshJobs(t.Context(), []priority.Job{}, 0)
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %d", len(ids))
	}
}
