package handlers

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/templates"
)

// HandleRefresh triggers a refresh request for an artist.
func (h *Handler) HandleRefresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	artistID := getRouteParam(r, "artistId")
	if artistID == "" {
		writeError(w, http.StatusBadRequest, "artist ID required")
		return
	}

	if !h.hasAvailableQuota() {
		writeError(w, http.StatusTooManyRequests, "No scraping quota available. Please check /api/quota for details.")
		return
	}

	record, err := h.findArtistRecord(ctx, artistID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "artist not found")
			return
		}
		log.Printf("[artist_refresh] findArtistRecord error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to lookup artist")
		return
	}

	_, duplicate, err := h.queueArtistRefresh(ctx, record)
	if err != nil {
		log.Printf("[artist_refresh] queueArtistRefresh error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to queue refresh")
		return
	}
	if duplicate {
		log.Printf("[handlers] Duplicate refresh request ignored for artist %s", record.Id)
		_ = respondArtistRefreshQueuedHTTP(w, r, record.Id, "already_queued")
		return
	}

	_ = respondArtistRefreshQueuedHTTP(w, r, record.Id, "queued")
}

func (h *Handler) handleRefresh(e *core.RequestEvent) error {
	h.HandleRefresh(e.Response, e.Request)
	return nil
}

// HandleBatchRefresh triggers batch refresh workflow.
func (h *Handler) HandleBatchRefresh(w http.ResponseWriter, r *http.Request) {
	h.ensureBatchProgressSubscriber()

	if err := r.ParseForm(); err != nil {
		log.Printf("[batch] ParseForm failed: %v", err)
		writeError(w, http.StatusBadRequest, "invalid form data")
		return
	}

	if snapshot, ok := h.resumableBatchRefreshSnapshot(r.Context(), r.FormValue("batch_id")); ok {
		_ = h.patchBatchRefreshStateHTTP(w, r, snapshot)
		return
	}

	count, err := parseBatchRefreshCount(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	opCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !h.hasAvailableQuota() {
		if wantsJSONResponse(r) {
			writeError(w, http.StatusTooManyRequests, "No scraping quota available.")
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.BatchRefreshErrorAlert("No scraping quota available."))
		return
	}

	if h.batchStore == nil {
		log.Printf("[batch] batch progress store is not configured")
		if wantsJSONResponse(r) {
			writeError(w, http.StatusServiceUnavailable, "batch progress tracking is unavailable")
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.BatchRefreshErrorAlert("Batch progress tracking is unavailable."))
		return
	}

	jobs, err := h.batchRefreshJobs(opCtx, batchRefreshCutoff(time.Now()))
	if err != nil {
		log.Printf("[batch] batchRefreshJobs failed: %v", err)
		if wantsJSONResponse(r) {
			writeError(w, http.StatusInternalServerError, "failed to fetch artists")
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.BatchRefreshErrorAlert("Failed to fetch artists for batch refresh."))
		return
	}

	queuedArtistIDs, stats := h.enqueueBatchRefreshJobs(opCtx, jobs, count)
	if len(queuedArtistIDs) == 0 {
		if wantsJSONResponse(r) {
			writeError(w, http.StatusUnprocessableEntity, "no artists queued")
			return
		}
		_ = h.patchBatchRefreshStateHTTP(w, r, batchProgressSnapshot{
			ID:        "",
			Stats:     stats,
			Total:     0,
			Completed: 0,
			Done:      true,
		})
		return
	}

	snapshot, err := h.createBatchProgress(opCtx, queuedArtistIDs, stats)
	if err != nil {
		log.Printf("[batch] createBatchProgress failed: %v", err)
		if wantsJSONResponse(r) {
			writeError(w, http.StatusInternalServerError, "failed to track batch progress")
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.BatchRefreshErrorAlert("Failed to track batch progress."))
		return
	}
	log.Printf("[batch] Created batch %s with %d queued artist(s)", snapshot.ID, snapshot.Total)
	if wantsJSONResponse(r) {
		_ = writeJSON(w, http.StatusOK, snapshot)
		return
	}
	_ = h.patchBatchRefreshStateHTTP(w, r, snapshot)
}

func (h *Handler) handleBatchRefresh(e *core.RequestEvent) error {
	h.HandleBatchRefresh(e.Response, e.Request)
	return nil
}
