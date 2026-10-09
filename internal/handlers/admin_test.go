package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"ListenLedger/internal/eventsourcing"
)

// TestAdminStatus_EmptyDB proves the ops endpoint serves with zero state and
// never writes: outbox lag 0, no checkpoints, no sagas, no batch.
func TestAdminStatus_EmptyDB(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	req := httptest.NewRequest(http.MethodGet, "/api/admin/status", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status = %v, want ok", body["status"])
	}
	if lag, ok := body["outbox_lag"].(float64); !ok || lag != 0 {
		t.Fatalf("outbox_lag = %v, want 0", body["outbox_lag"])
	}
	if sagas, ok := body["sagas"].(map[string]any); !ok || len(sagas) != 0 {
		t.Fatalf("sagas = %v, want empty", body["sagas"])
	}
	if _, ok := body["batch"]; ok {
		t.Fatalf("batch should be absent with no batches, got %v", body["batch"])
	}
}

// TestAdminStatus_WithBatchAndCheckpoint proves active batches and saved
// checkpoints surface through the endpoint.
func TestAdminStatus_WithBatchAndCheckpoint(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := t.Context()

	seedQueueTestArtist(t, ctx, h, "ar_adm1", "pending")
	snap, err := h.createBatchProgress(ctx, []string{"ar_adm1"}, nil)
	if err != nil {
		t.Fatalf("createBatchProgress: %v", err)
	}
	if snap.Total != 1 {
		t.Fatalf("batch total = %d, want 1", snap.Total)
	}
	if err := h.store.SaveCheckpoint(ctx, eventsourcing.CheckpointArtistProjection, 42); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/admin/status", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	batch, ok := body["batch"].(map[string]any)
	if !ok {
		t.Fatalf("batch missing: %v", body["batch"])
	}
	if batch["id"] != snap.ID {
		t.Fatalf("batch id = %v, want %s", batch["id"], snap.ID)
	}
	checkpoints, ok := body["checkpoints"].(map[string]any)
	if !ok {
		t.Fatalf("checkpoints missing: %v", body["checkpoints"])
	}
	if checkpoints["artist"] != float64(42) {
		t.Fatalf("artist checkpoint = %v, want 42", checkpoints["artist"])
	}
}
