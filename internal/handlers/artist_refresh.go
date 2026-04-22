//go:build goexperiment.jsonv2

package handlers

import (
	"log"
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"
)

// handleRefresh triggers a refresh request for an artist.
func (h *Handler) handleRefresh(e *core.RequestEvent) error {
	ctx := e.Request.Context()
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	if !h.hasAvailableQuota(ctx) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available. Please check /api/quota for details.",
		})
	}

	record, err := h.findArtistRecord(ctx, artistID)
	if err != nil {
		log.Printf("[artist_refresh] findArtistRecord error: %v", err)
		return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
	}

	requestID, duplicate, err := h.queueArtistRefresh(ctx, record)
	if err != nil {
		log.Printf("[artist_refresh] queueArtistRefresh error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to queue refresh"})
	}
	if duplicate {
		log.Printf("[handlers] Duplicate refresh request ignored for artist %s", record.Id)
		return respondArtistRefreshQueued(e, record.Id, "already_queued")
	}

	h.markArtistRefreshQueued(ctx, record, requestID)
	return respondArtistRefreshQueued(e, record.Id, "queued")
}

func (h *Handler) handleBatchRefresh(e *core.RequestEvent) error {
	h.ensureBatchProgressSubscriber()

	if err := e.Request.ParseForm(); err != nil {
		log.Printf("[batch] ParseForm failed: %v", err)
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid form data"})
	}

	if snapshot, ok := h.resumableBatchRefreshSnapshot(e.Request.FormValue("batch_id")); ok {
		return h.patchBatchRefreshState(e, snapshot)
	}

	count, err := parseBatchRefreshCount(e.Request)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	// hasAvailableQuota is a best-effort gate: it checks whether any quota
	// remains across all configured providers. Individual providers enforce
	// quota authoritatively during message processing via ErrQuotaExhausted.
	if !h.hasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available.",
		})
	}

	// Match the PocketBase DateTime format (space-separated, not RFC3339 T-separated)
	// so SQLite string comparison works correctly.
	jobs, err := h.batchRefreshJobs(e.Request.Context(), batchRefreshCutoff(time.Now()), count)
	if err != nil {
		log.Printf("[batch] batchRefreshJobs failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to fetch artists"})
	}

	jobs = limitPriorityJobs(jobs, count)
	queuedArtistIDs, stats := h.enqueueBatchRefreshJobs(e.Request.Context(), jobs)

	snapshot := h.createBatchProgress(queuedArtistIDs, stats)
	log.Printf("[batch] Created batch %s with %d queued artist(s)", snapshot.ID, snapshot.Total)
	return h.patchBatchRefreshState(e, snapshot)
}
