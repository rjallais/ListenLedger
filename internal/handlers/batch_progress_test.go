package handlers

import (
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"ListenLedger/internal/messaging"
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

func TestBatchRefreshCandidates_Empty(t *testing.T) {
	if got := batchRefreshCandidates(nil, 10, time.Now()); len(got) != 0 {
		t.Fatalf("expected 0 candidates, got %d", len(got))
	}
	if got := batchRefreshCandidates([]priority.Job{}, 0, time.Now()); len(got) != 0 {
		t.Fatalf("expected 0 candidates for zero count, got %d", len(got))
	}
	if ids := batchCandidateArtistIDs(nil); len(ids) != 0 {
		t.Fatalf("expected no member ids, got %v", ids)
	}
	if stats := batchCandidateStats(nil); len(stats) != 0 {
		t.Fatalf("expected empty stats, got %v", stats)
	}
}

func TestMarkBatchArtistDonePublishesProgress(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port: -1,
	})
	if err != nil {
		t.Fatalf("natsserver.NewServer: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Close()
	h.nc = nc

	ctx := t.Context()
	progressCh := make(chan messaging.BatchProgressUpdated, 10)
	_, err = nc.Subscribe(messaging.SubjectBatchProgress, func(msg *nats.Msg) {
		p, err := messaging.UnmarshalBatchProgressUpdated(msg.Data)
		if err == nil {
			progressCh <- p
		}
	})
	if err != nil {
		t.Fatalf("nc.Subscribe: %v", err)
	}

	snapshot, err := h.createBatchProgress(ctx, []string{"ar-1", "ar-2"}, map[string]int{"P0Queued": 2})
	if err != nil {
		t.Fatalf("createBatchProgress: %v", err)
	}

	// First event on start
	select {
	case p := <-progressCh:
		if p.BatchID != snapshot.ID || p.Total != 2 || p.Completed != 0 || p.Done != false {
			t.Fatalf("unexpected start progress: %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for start progress")
	}

	seedQueueTestArtist(t, ctx, h, "ar-1", "pending")
	seedQueueTestArtist(t, ctx, h, "ar-2", "pending")

	// Complete first artist
	h.markBatchArtistDone("ar-1", "idle")
	select {
	case p := <-progressCh:
		if p.BatchID != snapshot.ID || p.Total != 2 || p.Completed != 1 || p.Done != false {
			t.Fatalf("unexpected progress after ar-1: %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for progress after ar-1")
	}

	// Complete second artist
	h.markBatchArtistDone("ar-2", "idle")
	select {
	case p := <-progressCh:
		if p.BatchID != snapshot.ID || p.Total != 2 || p.Completed != 2 || p.Done != true {
			t.Fatalf("unexpected progress after ar-2: %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for progress after ar-2")
	}
}
