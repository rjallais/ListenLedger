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

// TestBatchCompletesWhenSomeArtistsNeverSettle covers the 99/100 stuck
// regression: a batch pre-registered candidates that were never actually
// queued (duplicates/errors). Without resizing Total and using the pending
// set for completion, the batch never reaches Done even after every queued
// artist settles.
func TestBatchCompletesAfterDroppedCandidates(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}

	// Pre-register 3 candidate IDs but only 2 will actually be queued.
	snapshot := h.createBatchProgress([]string{"artist-1", "artist-2", "artist-3"}, map[string]int{"P0_Queued": 3})
	if snapshot.Total != 3 {
		t.Fatalf("initial Total = %d, want 3", snapshot.Total)
	}

	// Only artist-1 and artist-2 were successfully queued.
	h.removeArtistsFromBatchSilent(snapshot.ID, []string{"artist-1", "artist-2"})

	resized, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("snapshot missing after resize")
	}
	assertBatchResizedAfterDrop(t, resized)
	assertBatchAfterFirstSettle(t, h, snapshot.ID)
	assertBatchFinalAfterSecondSettle(t, h, snapshot.ID)
}

// assertBatchResizedAfterDrop checks the batch state immediately after dropping
// a pre-registered candidate: Total down to 2, not Done, Completed 0.
func assertBatchResizedAfterDrop(t *testing.T, resized batchProgressSnapshot) {
	t.Helper()
	if resized.Total != 2 {
		t.Fatalf("Total after dropping candidate = %d, want 2", resized.Total)
	}
	if resized.Done {
		t.Fatalf("Done after dropping candidate = true, want false (2 still pending)")
	}
	// Nothing has settled yet, so Completed must still be 0 — Total dropped
	// from 3 to 2, not a fake bump to Completed.
	if resized.Completed != 0 {
		t.Fatalf("Completed after resize = %d, want 0", resized.Completed)
	}
}

// assertBatchAfterFirstSettle marks artist-1 done and verifies the batch is
// still open (1 artist still pending).
func assertBatchAfterFirstSettle(t *testing.T, h *Handler, batchID string) {
	t.Helper()
	h.markBatchArtistDone("artist-1", "idle")
	if doneSnap, ok := h.getBatchSnapshot(batchID); ok && doneSnap.Done {
		t.Fatalf("Done after first settle = true, want false (1 still pending)")
	}
}

// assertBatchFinalAfterSecondSettle marks artist-2 done and verifies the batch
// reaches Done with Completed == 2.
func assertBatchFinalAfterSecondSettle(t *testing.T, h *Handler, batchID string) {
	t.Helper()
	h.markBatchArtistDone("artist-2", "failed")
	finalSnap, ok := h.getBatchSnapshot(batchID)
	if !ok {
		t.Fatalf("snapshot missing after all settled")
	}
	if !finalSnap.Done {
		t.Fatalf("Done after all settled = false, want true (pending empty)")
	}
	if finalSnap.Completed != 2 {
		t.Fatalf("Completed after all settled = %d, want 2", finalSnap.Completed)
	}
}

// TestBatchCompletesWhenEventNeverArrives covers the dropped-event regression:
// the final artist.updated never reaches the subscriber, but a subsequent
// status transition for another artist on the same batch still completes only
// its own slot. Verifies that a single missed event for artist-2 leaves the
// batch open (correct), and that removeArtistsFromBatchSilent can recover it
// when the caller knows artist-2 will never settle.
func TestBatchRecoverableViaSilentRemove(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}

	snapshot := h.createBatchProgress([]string{"artist-1", "artist-2"}, map[string]int{"P0_Queued": 2})

	h.markBatchArtistDone("artist-1", "idle")
	if snap, _ := h.getBatchSnapshot(snapshot.ID); snap.Done {
		t.Fatalf("Done after one of two settled = true, want false")
	}

	// Simulate the worker knowing artist-2 will never receive an event
	// (e.g. it was re-classified / message Terminated). Silent removal of the
	// only remaining pending artist must finish the batch.
	h.removeArtistsFromBatchSilent(snapshot.ID, []string{"artist-1"})
	final, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("snapshot missing after silent remove")
	}
	if !final.Done {
		t.Fatalf("Done after removing last pending = false, want true")
	}
	if final.Total != 1 {
		t.Fatalf("Total after removing last pending = %d, want 1", final.Total)
	}
	if final.Completed != 1 {
		t.Fatalf("Completed after removing last pending = %d, want 1", final.Completed)
	}
}

// TestBatchEmptyFromStartIsDone ensures the trivial empty batch is done, which
// the empty-pending predicate must preserve.
func TestBatchEmptyFromStartIsDone(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
	snapshot := h.createBatchProgress(nil, map[string]int{})
	if !snapshot.Done {
		t.Fatalf("empty batch Done = false, want true")
	}
}

// TestBatchCompletesAfterAllArtistsRemoved ensures reconciliation that drains
// the pending set back to zero — shrinking Total to zero — still marks the
// batch Done. Without this the completion predicate would reject a zero-Total
// batch even though nothing remains to process.
func TestBatchCompletesAfterAllArtistsRemoved(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
	snapshot := h.createBatchProgress([]string{"artist-1", "artist-2"}, map[string]int{"P0_Queued": 2})

	h.removeArtistsFromBatchSilent(snapshot.ID, nil)
	got, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("snapshot missing after removing all pending")
	}
	if !got.Done {
		t.Fatalf("Done after removing all pending = false, want true (Total=%d, Completed=%d)", got.Total, got.Completed)
	}
	if got.Total != 0 {
		t.Fatalf("Total after removing all pending = %d, want 0", got.Total)
	}
	if got.Completed != 0 {
		t.Fatalf("Completed after removing all pending = %d, want 0", got.Completed)
	}
}

// TestBatchCompletesAfterPartialArtistRemoval ensures reconciliation that
// removes only the unsettled artists still yields Done when the remaining
// (already settled) artists leave Pending empty.
func TestBatchCompletesAfterPartialArtistRemoval(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
	snapshot := h.createBatchProgress([]string{"artist-1", "artist-2", "artist-3"}, map[string]int{"P0_Queued": 3})

	h.markBatchArtistDone("artist-1", "idle")
	h.removeArtistsFromBatchSilent(snapshot.ID, []string{"artist-1"})
	got, ok := h.getBatchSnapshot(snapshot.ID)
	if !ok {
		t.Fatalf("snapshot missing after partial remove")
	}
	if !got.Done {
		t.Fatalf("Done after partial remove = false, want true")
	}
	if got.Total != 1 {
		t.Fatalf("Total after partial remove = %d, want 1", got.Total)
	}
	if got.Completed != 1 {
		t.Fatalf("Completed after partial remove = %d, want 1", got.Completed)
	}
}

// TestSSESeesCompletionForLastArtist reproduces the 99/100 stuck race: two
// independent NATS subscribers (batch-tracker + SSE) process the same
// artist.updated message concurrently. The fix lets the SSE callback drive
// markBatchArtistDone itself before reading the snapshot, so even if the
// batch-tracker goroutine has not yet applied the update, the SSE read sees
// the completed state. This test asserts the read-after-update invariant the
// SSE callback relies on for the final artist in a batch.
func TestSSESeesCompletionForLastArtist(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
	snapshot := h.createBatchProgress([]string{"a1", "a2", "a3"}, map[string]int{"P0_Queued": 3})
	_ = snapshot

	// First two artists settle via the "batch-tracker" path.
	h.markBatchArtistDone("a1", "idle")
	h.markBatchArtistDone("a2", "failed")

	// Simulate the SSE path on the final artist: it must update then read.
	h.markBatchArtistDone("a3", "idle")
	got, ok := h.getLatestBatchSnapshot()
	if !ok {
		t.Fatalf("getLatestBatchSnapshot: no batch")
	}
	if got.Completed != 3 || got.Total != 3 {
		t.Fatalf("final snapshot = %+v, want Completed=3 Total=3", got)
	}
	if !got.Done {
		t.Fatalf("Done after last settle via SSE path = false, want true")
	}
}

// TestSSEPendingEventDoesNotRegressCompletion ensures that when the
// pending-status event for a settled artist arrives at the SSE callback after
// the idle event (goroutine reordering), markBatchArtistDone is a no-op and
// the snapshot read still reports Done — the late pending push must not clobber
// the UI's done state.
func TestSSEPendingEventDoesNotRegressCompletion(t *testing.T) {
	h := &Handler{
		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
	h.createBatchProgress([]string{"a1"}, map[string]int{"P0_Queued": 1})

	h.markBatchArtistDone("a1", "idle") // completes the batch
	if snap, _ := h.getLatestBatchSnapshot(); !snap.Done {
		t.Fatalf("Done after idle = false, want true")
	}

	// Late pending event (e.g. in-flight status from processRequest) arrives.
	h.markBatchArtistDone("a1", "pending")
	snap, ok := h.getLatestBatchSnapshot()
	if !ok {
		t.Fatalf("snapshot missing after late pending")
	}
	if !snap.Done {
		t.Fatalf("Done after late pending = false, want true (must not regress)")
	}
	if snap.Completed != 1 {
		t.Fatalf("Completed after late pending = %d, want 1", snap.Completed)
	}
}

