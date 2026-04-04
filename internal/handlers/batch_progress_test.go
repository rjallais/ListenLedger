//go:build goexperiment.jsonv2

package handlers

import "testing"

func TestMarkBatchArtistDoneOnlyOnSuccess(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}

	snapshot := h.createBatchProgress([]string{"artist-1", "artist-2"}, map[string]int{"P0_Queued": 2})
	if snapshot.Completed != 0 {
		t.Fatalf("initial Completed = %d, want 0", snapshot.Completed)
	}

	// Failed attempts should advance progress (terminal state).
	h.markBatchArtistDone("artist-1", "failed")
	failedSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after failed update")
	}
	if failedSnapshot.Completed != 1 {
		t.Fatalf("Completed after failed status = %d, want 1", failedSnapshot.Completed)
	}

	// Pending updates should not advance progress.
	h.markBatchArtistDone("artist-2", "pending")
	pendingSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after pending update")
	}
	if pendingSnapshot.Completed != 1 {
		t.Fatalf("Completed after pending status = %d, want 1", pendingSnapshot.Completed)
	}

	// Successful update (idle) should advance progress and complete the batch.
	h.markBatchArtistDone("artist-2", "idle")
	successSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
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
	duplicateSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after duplicate update")
	}
	if duplicateSnapshot.Completed != 2 {
		t.Fatalf("Completed after duplicate = %d, want 2", duplicateSnapshot.Completed)
	}
}
