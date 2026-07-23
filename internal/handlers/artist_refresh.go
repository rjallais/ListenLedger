//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/priority"
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
		if errors.Is(err, sql.ErrNoRows) {
			return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
		}
		log.Printf("[artist_refresh] findArtistRecord error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to lookup artist"})
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
		return h.patchBatchRefreshState(e.Request.Context(), e, snapshot)
	}

	count, err := parseBatchRefreshCount(e.Request)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	// Run the queueing workflow on a bounded server-side context so a client-side
	// XHR abort (for example, browser navigation/retry) doesn't cancel the batch
	// creation midway.
	opCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
	if !h.hasAvailableQuota(opCtx) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available.",
		})
	}

	// Match the PocketBase DateTime format (space-separated, not RFC3339 T-separated)
	// so SQLite string comparison works correctly.
	jobs, err := h.batchRefreshJobs(opCtx, batchRefreshCutoff(time.Now()))
	if err != nil {
		log.Printf("[batch] batchRefreshJobs failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to fetch artists"})
	}

	// Register the batch progress FIRST, before queueing any NATS messages.
	// Otherwise workers can pick up messages and fire artist.updated events
	// (which markBatchArtistDone uses) before artist IDs are registered in
	// h.artistBatch, causing those updates to be silently dropped.
	artistIDs, stats := jobIDsAndStats(jobs, count)
	if len(artistIDs) == 0 {
		return h.respondEmptyBatch(opCtx, e)
	}

	snapshot := h.createBatchProgress(artistIDs, stats)
	log.Printf("[batch] Created batch %s with %d queued artist(s)", snapshot.ID, snapshot.Total)
	return h.finalizeBatchRefresh(opCtx, e, batchFinalizeParams{
		snapshot:   snapshot,
		jobs:       jobs,
		count:      count,
		artistIDs:  artistIDs,
	})
}

// respondEmptyBatch handles the no-artists case for a batch refresh request,
// returning either a JSON error or an empty done-snapshot depending on the
// request format.
func (h *Handler) respondEmptyBatch(ctx context.Context, e *core.RequestEvent) error {
	if wantsJSONResponse(e.Request) {
		return e.JSON(http.StatusUnprocessableEntity, map[string]string{"error": "no artists to refresh"})
	}
	return h.patchBatchRefreshState(ctx, e, batchProgressSnapshot{
		ID:        "",
		Total:     0,
		Completed: 0,
		Done:      true,
	})
}

// batchFinalizeParams bundles the inputs needed to queue and reconcile a batch
// refresh after its progress has been registered.
type batchFinalizeParams struct {
	snapshot  batchProgressSnapshot
	jobs      []priority.Job
	count     int
	artistIDs []string
}

// finalizeBatchRefresh queues the batch jobs, reconciles any artists that were
// pre-registered but not actually queued (e.g. duplicate detection or enqueue
// error), and returns the corrected batch snapshot.
func (h *Handler) finalizeBatchRefresh(ctx context.Context, e *core.RequestEvent, p batchFinalizeParams) error {
	// Now queue the jobs. artist.updated events from this batch will find
	// their artist IDs in h.artistBatch because we registered them above.
	queuedIDs, _, err := h.enqueueBatchRefreshJobs(ctx, p.jobs, p.count)
	if err != nil {
		return err
	}

	// Remove any artist IDs that were pre-registered but not actually queued
	// (e.g., duplicate detection or enqueue error). This keeps the batch
	// accurate — skipped artists won't count as "pending forever".
	if len(queuedIDs) < len(p.artistIDs) {
		h.removeArtistsFromBatchSilent(p.snapshot.ID, queuedIDs)
	}

	// Re-read the batch after reconciliation so the returned snapshot
	// reflects the corrected Total/Done/Completed rather than the stale
	// pre-enqueue state the front-end received at creation.
	snapshot := p.snapshot
	if reconciled, ok := h.getBatchSnapshot(p.snapshot.ID); ok {
		snapshot = reconciled
	}

	return h.patchBatchRefreshState(ctx, e, snapshot)
}
