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

	_, duplicate, err := h.queueArtistRefresh(ctx, record)
	if err != nil {
		log.Printf("[artist_refresh] queueArtistRefresh error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to queue refresh"})
	}
	if duplicate {
		log.Printf("[handlers] Duplicate refresh request ignored for artist %s", record.Id)
		return respondArtistRefreshQueued(e, record.Id, "already_queued")
	}

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
	// remains across all configured providers. It does NOT reserve capacity
	// for the requested count because per-job reservation is infeasible —
	// quota is consumed asynchronously by provider goroutine pools in the
	// worker, and providers like local-headless and Browserless have no
	// numeric credit API to reserve against.
	//
	// Authoritative quota enforcement happens downstream:
	//   - Worker provider pools detect quota exhaustion via
	//     spotify.ErrQuotaExhausted (triggered by HTTP 401/402/403/429 from
	//     external providers).
	//   - On exhaustion, the provider's pool shuts down and NAKs in-flight
	//     messages back to JetStream for redelivery by surviving providers.
	//   - When all pools are exhausted, the NATS consumer is drained and
	//     remaining messages stay queued for redelivery after restart.
	//
	// See also: enqueueBatchRefreshJobs, batchRefreshJobs.
	if !h.hasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available.",
		})
	}

	// Match the PocketBase DateTime format (space-separated, not RFC3339 T-separated)
	// so SQLite string comparison works correctly.
	jobs, err := h.batchRefreshJobs(e.Request.Context(), batchRefreshCutoff(time.Now()))
	if err != nil {
		log.Printf("[batch] batchRefreshJobs failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to fetch artists"})
	}

	queuedArtistIDs, stats := h.enqueueBatchRefreshJobs(e.Request.Context(), jobs, count)
	if len(queuedArtistIDs) == 0 {
		return e.JSON(http.StatusUnprocessableEntity, map[string]string{"error": "no artists queued"})
	}

	snapshot := h.createBatchProgress(queuedArtistIDs, stats)
	log.Printf("[batch] Created batch %s with %d queued artist(s)", snapshot.ID, snapshot.Total)
	return h.patchBatchRefreshState(e, snapshot)
}
