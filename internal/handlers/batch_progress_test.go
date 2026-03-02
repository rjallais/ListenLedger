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

	// Failed attempts should not advance progress.
	h.markBatchArtistDone("artist-1", "failed")
	failedSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after failed update")
	}
	if failedSnapshot.Completed != 0 {
		t.Fatalf("Completed after failed status = %d, want 0", failedSnapshot.Completed)
	}

	// Pending updates should not advance progress.
	h.markBatchArtistDone("artist-1", "pending")
	pendingSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after pending update")
	}
	if pendingSnapshot.Completed != 0 {
		t.Fatalf("Completed after pending status = %d, want 0", pendingSnapshot.Completed)
	}

	// Successful update (idle) should advance progress exactly once.
	h.markBatchArtistDone("artist-1", "idle")
	successSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after success update")
	}
	if successSnapshot.Completed != 1 {
		t.Fatalf("Completed after success status = %d, want 1", successSnapshot.Completed)
	}
	if successSnapshot.Done {
		t.Fatalf("Done after one success = true, want false")
	}

	// Duplicate successful event for the same artist should not increment again.
	h.markBatchArtistDone("artist-1", "idle")
	duplicateSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after duplicate success update")
	}
	if duplicateSnapshot.Completed != 1 {
		t.Fatalf("Completed after duplicate success = %d, want 1", duplicateSnapshot.Completed)
	}

	// Final successful artist completes the batch.
	h.markBatchArtistDone("artist-2", "idle")
	finalSnapshot, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("batch snapshot not found after final success update")
	}
	if finalSnapshot.Completed != 2 {
		t.Fatalf("Completed after final success = %d, want 2", finalSnapshot.Completed)
	}
	if !finalSnapshot.Done {
		t.Fatalf("Done after all successes = false, want true")
	}
}
