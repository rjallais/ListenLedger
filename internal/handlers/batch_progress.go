//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"log"
	"maps"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

type batchProgress struct {
	CreatedAt time.Time
	UpdatedAt time.Time

	ID string

	Stats   map[string]int
	Pending map[string]struct{}

	Total     int
	Completed int
	Done      bool
}

func (b *batchProgress) isDone() bool {
	return b.Done || len(b.Pending) == 0
}

type batchProgressSnapshot struct {
	ID        string
	Stats     map[string]int
	Total     int
	Completed int
	Done      bool
}

func (h *Handler) ensureBatchProgressSubscriber() {
	h.batchMu.RLock()
	if h.batchUpdates != nil {
		h.batchMu.RUnlock()
		return
	}
	h.batchMu.RUnlock()

	h.batchSubMu.Lock()
	defer h.batchSubMu.Unlock()

	h.batchMu.RLock()
	if h.batchUpdates != nil {
		h.batchMu.RUnlock()
		return
	}
	h.batchMu.RUnlock()

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
	h.batchUpdates = sub
	h.batchMu.Unlock()
	log.Printf("[batch] Tracking progress from %s", messaging.SubjectArtistUpdated)
}

func (h *Handler) markBatchArtistDone(artistID, fetchStatus string) {
	if !isCompletedFetchStatus(artistID, fetchStatus) {
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
	// Completion is driven by the pending set, not by Completed >= Total.
	// Total is set once at creation from candidate IDs and is never resized
	// upward, so comparing against it fails when some candidates were dropped
	// before queueing (duplicates/errors). The empty-pending set is the
	// authoritative signal that every tracked artist has settled, even when
	// reconciliation has drained Total back to zero.
	batch.Done = batch.isDone()
	batch.UpdatedAt = time.Now()
}

func isCompletedFetchStatus(artistID, fetchStatus string) bool {
	return artistID != "" && (fetchStatus == "idle" || fetchStatus == "failed")
}

func (h *Handler) createBatchProgress(artistIDs []string, stats map[string]int) batchProgressSnapshot {
	now := time.Now()
	pending := make(map[string]struct{}, len(artistIDs))
	for _, artistID := range artistIDs {
		pending[artistID] = struct{}{}
	}

	snapshotStats := make(map[string]int, len(stats))
	maps.Copy(snapshotStats, stats)

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
	h.removeCompletedBatchesLocked(cutoff)
	h.removeOrphanedArtistMappingsLocked()
	if h.latestBatch == "" {
		h.latestBatch = h.findLatestBatchIDLocked()
	}
}

func (h *Handler) removeCompletedBatchesLocked(cutoff time.Time) {
	for batchID, batch := range h.batches {
		if !batch.Done || batch.UpdatedAt.After(cutoff) {
			continue
		}
		delete(h.batches, batchID)
		if h.latestBatch == batchID {
			h.latestBatch = ""
		}
	}
}

func (h *Handler) removeOrphanedArtistMappingsLocked() {
	for artistID, batchID := range h.artistBatch {
		if _, ok := h.batches[batchID]; ok {
			continue
		}
		delete(h.artistBatch, artistID)
	}
}

func (h *Handler) findLatestBatchIDLocked() string {
	var latestID string
	var latestTime time.Time
	for batchID, batch := range h.batches {
		if batch.UpdatedAt.After(latestTime) {
			latestID = batchID
			latestTime = batch.UpdatedAt
		}
	}
	return latestID
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

	var activeBatch *batchProgress
	var latestTime time.Time

	for _, batch := range h.batches {
	done := batch.Done || len(batch.Pending) == 0
		if !done {
			if batch.UpdatedAt.After(latestTime) {
				activeBatch = batch
				latestTime = batch.UpdatedAt
			}
		}
	}

	if activeBatch == nil {
		return batchProgressSnapshot{}, false
	}

	return h.batchSnapshotLocked(activeBatch), true
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
	maps.Copy(stats, batch.Stats)

	done := batch.isDone()

	return batchProgressSnapshot{
		ID:        batch.ID,
		Total:     batch.Total,
		Completed: batch.Completed,
		Done:      done,
		Stats:     stats,
	}
}

func (h *Handler) patchBatchRefreshState(ctx context.Context, e *core.RequestEvent, snapshot batchProgressSnapshot) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	payload := formatBatchSignal(snapshot.ID, snapshot.Total, snapshot.Completed, snapshot.Done)
	if err := sse.PatchSignals(payload); err != nil {
		return err
	}
	return sse.PatchElementTempl(
		templates.BatchRefreshResult(snapshot.ID, snapshot.Total, snapshot.Completed, snapshot.Stats, snapshot.Done),
	)
}
