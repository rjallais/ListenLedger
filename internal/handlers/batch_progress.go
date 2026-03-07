//go:build goexperiment.jsonv2

package handlers

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

type batchProgress struct {
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time

	Total     int
	Completed int
	Done      bool

	Stats   map[string]int
	Pending map[string]struct{}
}

type batchProgressSnapshot struct {
	ID        string
	Total     int
	Completed int
	Done      bool
	Stats     map[string]int
}

func (h *Handler) ensureBatchProgressSubscriber() {
	h.batchMu.Lock()
	if h.batchUpdates != nil {
		h.batchMu.Unlock()
		return
	}
	h.batchMu.Unlock()

	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			return
		}
		h.markBatchArtistDone(update.ArtistID, update.FetchStatus)
	})
	if err != nil {
		log.Printf("[batch] Failed to subscribe to %s: %v", messaging.SubjectArtistUpdated, err)
		return
	}

	h.batchMu.Lock()
	defer h.batchMu.Unlock()
	if h.batchUpdates != nil {
		_ = sub.Unsubscribe()
		return
	}
	h.batchUpdates = sub
	log.Printf("[batch] Tracking progress from %s", messaging.SubjectArtistUpdated)
}

func (h *Handler) markBatchArtistDone(artistID, fetchStatus string) {
	if artistID == "" || fetchStatus != "idle" {
		return
	}

	h.batchMu.Lock()
	defer h.batchMu.Unlock()

	batchID, ok := h.artistBatch[artistID]
	if !ok {
		return
	}
	batch, ok := h.batches[batchID]
	if !ok {
		delete(h.artistBatch, artistID)
		return
	}
	if _, pending := batch.Pending[artistID]; !pending {
		return
	}

	delete(batch.Pending, artistID)
	delete(h.artistBatch, artistID)
	batch.Completed++
	if batch.Completed >= batch.Total {
		batch.Completed = batch.Total
		batch.Done = true
	}
	batch.UpdatedAt = time.Now()
}

func (h *Handler) createBatchProgress(artistIDs []string, stats map[string]int) batchProgressSnapshot {
	now := time.Now()
	pending := make(map[string]struct{}, len(artistIDs))
	for _, artistID := range artistIDs {
		pending[artistID] = struct{}{}
	}

	snapshotStats := make(map[string]int, len(stats))
	for key, value := range stats {
		snapshotStats[key] = value
	}

	batchID := strconv.FormatInt(now.UnixNano(), 36)
	progress := &batchProgress{
		ID:        batchID,
		CreatedAt: now,
		UpdatedAt: now,
		Total:     len(artistIDs),
		Completed: 0,
		Done:      len(artistIDs) == 0,
		Stats:     snapshotStats,
		Pending:   pending,
	}

	h.batchMu.Lock()
	defer h.batchMu.Unlock()

	h.pruneBatchStateLocked(now.Add(-2 * time.Hour))

	h.batches[batchID] = progress
	h.latestBatch = batchID
	for artistID := range pending {
		h.artistBatch[artistID] = batchID
	}

	return h.batchSnapshotLocked(progress)
}

func (h *Handler) pruneBatchStateLocked(cutoff time.Time) {
	for batchID, batch := range h.batches {
		if !batch.Done || batch.UpdatedAt.After(cutoff) {
			continue
		}
		delete(h.batches, batchID)
		if h.latestBatch == batchID {
			h.latestBatch = ""
		}
	}

	for artistID, batchID := range h.artistBatch {
		if _, ok := h.batches[batchID]; ok {
			continue
		}
		delete(h.artistBatch, artistID)
	}

	if h.latestBatch != "" {
		return
	}

	var latestID string
	var latestTime time.Time
	for batchID, batch := range h.batches {
		if batch.UpdatedAt.After(latestTime) {
			latestID = batchID
			latestTime = batch.UpdatedAt
		}
	}
	h.latestBatch = latestID
}

func (h *Handler) getBatchSnapshot(batchID string) (batchProgressSnapshot, bool) {
	if batchID == "" {
		return batchProgressSnapshot{}, false
	}

	h.batchMu.RLock()
	defer h.batchMu.RUnlock()

	batch, ok := h.batches[batchID]
	if !ok {
		return batchProgressSnapshot{}, false
	}
	return h.batchSnapshotLocked(batch), true
}

func (h *Handler) getActiveBatchSnapshot() (batchProgressSnapshot, bool) {
	h.batchMu.RLock()
	defer h.batchMu.RUnlock()

	batchID := h.latestBatch
	if batchID == "" {
		return batchProgressSnapshot{}, false
	}
	batch, ok := h.batches[batchID]
	if !ok {
		return batchProgressSnapshot{}, false
	}
	snapshot := h.batchSnapshotLocked(batch)
	if snapshot.Done {
		return batchProgressSnapshot{}, false
	}
	return snapshot, true
}

func (h *Handler) getLatestBatchSnapshot() (batchProgressSnapshot, bool) {
	h.batchMu.RLock()
	defer h.batchMu.RUnlock()

	batchID := h.latestBatch
	if batchID == "" {
		return batchProgressSnapshot{}, false
	}
	batch, ok := h.batches[batchID]
	if !ok {
		return batchProgressSnapshot{}, false
	}
	return h.batchSnapshotLocked(batch), true
}

func (h *Handler) batchSnapshotLocked(batch *batchProgress) batchProgressSnapshot {
	if batch == nil {
		return batchProgressSnapshot{}
	}

	stats := make(map[string]int, len(batch.Stats))
	for key, value := range batch.Stats {
		stats[key] = value
	}

	done := batch.Done || (batch.Total > 0 && batch.Completed >= batch.Total)

	return batchProgressSnapshot{
		ID:        batch.ID,
		Total:     batch.Total,
		Completed: batch.Completed,
		Done:      done,
		Stats:     stats,
	}
}

func (h *Handler) patchBatchRefreshState(e *core.RequestEvent, snapshot batchProgressSnapshot) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	payload := fmt.Appendf(
		nil,
		`{"batchID":%q,"batchTotal":%d,"batchCompleted":%d,"batchDone":%t}`,
		snapshot.ID,
		snapshot.Total,
		snapshot.Completed,
		snapshot.Done,
	)
	if err := sse.PatchSignals(payload); err != nil {
		return err
	}
	return sse.PatchElementTempl(
		templates.BatchRefreshResult(snapshot.ID, snapshot.Total, snapshot.Completed, snapshot.Stats, snapshot.Done),
	)
}
