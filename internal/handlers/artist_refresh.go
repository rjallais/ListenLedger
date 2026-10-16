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

	opTimeout := 30 * time.Second
	if needed := time.Duration(count) * time.Second; needed > opTimeout {
		opTimeout = needed
	}
	opCtx, cancel := context.WithTimeout(context.Background(), opTimeout)
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

	jobs, err := h.batchRefreshJobs(opCtx, batchRefreshCutoff(time.Now()), count)
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

	if len(jobs) == 0 {
		h.respondBatchRefreshEmpty(w, r, nil)
		return
	}

	var (
		snapshot        batchProgressSnapshot
		queuedArtistIDs []string
	)
	if h.app == nil {
		// No PocketBase app (tests / legacy wiring): queue sequentially, then
		// start the batch from the accepted subset — every member is already
		// pending when the batch record is written.
		var stats map[string]int
		queuedArtistIDs, stats = h.enqueueBatchRefreshJobsSequential(opCtx, jobs, count)
		if len(queuedArtistIDs) == 0 {
			h.respondBatchRefreshEmpty(w, r, stats)
			return
		}
		snapshot, err = h.createBatchProgress(opCtx, queuedArtistIDs, stats)
	} else {
		// Durable intent before dispatch: persist rows plus pending status in
		// one PocketBase transaction, record the batch (BatchStarted with the
		// full member list) once every member is pending, then publish. The
		// fact must follow the persist commit — before it, members are not yet
		// pending and the batch reconciler would complete them at birth — and
		// must precede the publish, so a killed enqueue leaves a logged,
		// reconcilable batch: never-dispatched rows are swept to failed and
		// members complete through the reconciler instead of wedging as ghosts.
		candidates := batchRefreshCandidates(jobs, count, time.Now())
		if len(candidates) == 0 {
			h.respondBatchRefreshEmpty(w, r, nil)
			return
		}
		if persistErr := h.persistBatchRefreshCandidates(opCtx, candidates); persistErr != nil {
			log.Printf("[batch] Transaction failed to queue batch artists: %v", persistErr)
			h.respondBatchRefreshEmpty(w, r, nil)
			return
		}
		snapshot, err = h.createBatchProgress(opCtx, batchCandidateArtistIDs(candidates), batchCandidateStats(candidates))
		if err != nil {
			// Fail closed: compensate the persisted rows and dispatch nothing —
			// no request may run behind a missing batch record.
			h.abortBatchRefreshCandidates(candidates)
		} else {
			queuedArtistIDs = h.processBatchRefreshResults(opCtx, h.publishBatchRefreshCandidates(opCtx, candidates))
			if len(queuedArtistIDs) == 0 {
				log.Printf("[batch] No candidate dispatched for batch %s; reconciler will close it", snapshot.ID)
			}
			// Fold instant completions (failed dispatches rolled back above)
			// into the response snapshot.
			if current, ok := h.getBatchSnapshot(opCtx, snapshot.ID); ok {
				snapshot = current
			}
		}
	}
	if err != nil {
		log.Printf("[batch] createBatchProgress failed: %v", err)
		if wantsJSONResponse(r) {
			writeError(w, http.StatusInternalServerError, "failed to track batch progress")
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchSignals([]byte(`{"batchID":"","batchDone":true,"_batch_refresh_loading":false}`))
		_ = sse.PatchElementTempl(templates.BatchRefreshErrorAlert("Failed to track batch progress."))
		return
	}
	log.Printf("[batch] Created batch %s with %d member(s), %d dispatched", snapshot.ID, snapshot.Total, len(queuedArtistIDs))
	if wantsJSONResponse(r) {
		_ = writeJSON(w, http.StatusOK, snapshot)
		return
	}
	_ = h.patchBatchRefreshStateHTTP(w, r, snapshot)
}

// respondBatchRefreshEmpty answers a batch request that queued nothing with an
// empty done snapshot; JSON callers get a 422 so the frontend shows the error.
func (h *Handler) respondBatchRefreshEmpty(w http.ResponseWriter, r *http.Request, stats map[string]int) {
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
}

func (h *Handler) handleBatchRefresh(e *core.RequestEvent) error {
	h.HandleBatchRefresh(e.Response, e.Request)
	return nil
}
