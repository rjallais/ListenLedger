package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/commands"
	"ListenLedger/internal/messaging"
)

// parseCommandsFilter reads list/redrive filters from query params.
// ?artist_id= &type=refresh|batch|backfilled &unresolved=true &limit= &since=&until= (RFC3339).
func parseCommandsFilter(r *http.Request) (commands.Filter, error) {
	f := commands.Filter{
		ArtistID: strings.TrimSpace(r.URL.Query().Get("artist_id")),
		Type:     strings.TrimSpace(r.URL.Query().Get("type")),
		Limit:    getQueryParamInt(r, "limit", 100, 1, 1000),
	}
	switch f.Type {
	case "", commands.TypeRefresh, commands.TypeBatch, commands.TypeBackfilled:
	default:
		return commands.Filter{}, fmt.Errorf("type must be refresh, batch, or backfilled")
	}
	if v := strings.TrimSpace(r.URL.Query().Get("unresolved")); v != "" {
		f.UnresolvedOnly = v == "true" || v == "1"
	}
	if v := strings.TrimSpace(r.URL.Query().Get("since")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return commands.Filter{}, fmt.Errorf("invalid since timestamp (want RFC3339)")
		}
		f.Since = t
	}
	if v := strings.TrimSpace(r.URL.Query().Get("until")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return commands.Filter{}, fmt.Errorf("invalid until timestamp (want RFC3339)")
		}
		f.Until = t
	}
	return f, nil
}

// HandleCommandsList returns the durable command history as JSON.
func (h *Handler) HandleCommandsList(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "command log not configured")
		return
	}
	f, err := parseCommandsFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cmds, err := commands.List(r.Context(), h.db, f)
	if err != nil {
		log.Printf("[commands] list error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to list commands")
		return
	}
	_ = writeJSON(w, http.StatusOK, cmds)
}

func (h *Handler) handleCommandsList(e *core.RequestEvent) error {
	h.HandleCommandsList(e.Response, e.Request)
	return nil
}

// HandleCommandsRedrive republishes logged commands (pure republish, no state
// mutation). Already-succeeded dispatches ack-skip in the worker by
// request_id design; use ?unresolved=true to target commands that never
// completed. ?dry_run=true reports what would publish without publishing.
func (h *Handler) HandleCommandsRedrive(w http.ResponseWriter, r *http.Request) {
	if h.db == nil || h.js == nil {
		writeError(w, http.StatusServiceUnavailable, "command redrive not configured")
		return
	}
	f, err := parseCommandsFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	dryRun := strings.TrimSpace(r.URL.Query().Get("dry_run")) == "true" ||
		strings.TrimSpace(r.URL.Query().Get("dry_run")) == "1"

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if !dryRun && !h.hasAvailableQuota() {
		writeError(w, http.StatusTooManyRequests, "No scraping quota available.")
		return
	}

	publish := func(ctx context.Context, req messaging.ScrapeRequested) error {
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := h.publishScrapeRequest(pubCtx, req)
		return err
	}

	stats, err := commands.Redrive(ctx, h.db, publish, f, dryRun)
	if err != nil {
		log.Printf("[commands] redrive error: %v", err)
		writeError(w, http.StatusInternalServerError, "redrive failed")
		return
	}

	log.Printf("[commands] redrive total=%d published=%d failed=%d dry_run=%t",
		stats.Total, stats.Published, stats.Failed, stats.DryRun)
	h.notifyQueueUpdated()

	if wantsJSONResponse(r) {
		_ = writeJSON(w, http.StatusOK, stats)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleCommandsRedrive(e *core.RequestEvent) error {
	h.HandleCommandsRedrive(e.Response, e.Request)
	return nil
}
