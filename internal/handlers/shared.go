package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/config"
)

// sseOpts is used for short-lived SSE responses (batch POST, refresh POST, etc.)
// where compression is safe because the response completes quickly.
var sseOpts = []datastar.SSEOption{
	datastar.WithCompression(
		datastar.WithClientPriority(),
		datastar.WithBrotli(
			datastar.WithBrotliLevel(5),
		),
		datastar.WithGzip(),
	),
}

// sseStreamOpts is used for the long-lived /api/events SSE connection.
// No compression: compressors buffer data before flushing, which prevents
// SSE events from being delivered immediately and causes
// ERR_INCOMPLETE_CHUNKED_ENCODING on the client.
var sseStreamOpts []datastar.SSEOption

// patchOpts builds the datastar PatchElement options for a fragment update.
// It always applies the merge selector/mode, and prepends view-transition
// options when enabled in config (VIEW_TRANSITIONS defaults to true).
//
// viewTransitionSelector is a CSS selector identifying the element being
// replaced (e.g. "#artist-row-123" or a templ-generated target id). It is only
// meaningful with element patches, not signal patches.
func patchOpts(cfg *config.Config, viewTransitionSelector string, merge ...datastar.PatchElementOption) []datastar.PatchElementOption {
	if !useViewTransitions(cfg) {
		return merge
	}
	out := []datastar.PatchElementOption{
		datastar.WithUseViewTransitions(true),
	}
	if viewTransitionSelector != "" {
		out = append(out, datastar.WithViewTransitionSelector(viewTransitionSelector))
	}
	out = append(out, merge...)
	return out
}

// useViewTransitions reports whether view transitions are enabled in config,
// with a safe default (on) when config is nil.
func useViewTransitions(cfg *config.Config) bool {
	if cfg == nil {
		return true
	}
	return cfg.UseViewTransitions
}

var allowedGenreGroups = map[string]bool{
	"rock_metal":      true,
	"everything_else": true,
}

var allowedListStatuses = map[string]bool{
	"included":       true,
	"recently_added": true,
	"not_added":      true,
	"waiting":        true,
}

const (
	songsCurrentPlaylistSize = 500
	songsRecentBatchSize     = 13
	songsRecentBatchWindow   = 13 * 24 * time.Hour
	songsDefaultPageSize     = 50
	songsMaxPageSize         = 100

	playlistSortAddedDesc  = "added_desc"
	playlistSortReleaseAsc = "release_asc"
)

// getRouteParam extracts a named URL parameter from the request, checking Chi's route context
// first and falling back to standard library r.PathValue. Whitespace is trimmed.
func getRouteParam(r *http.Request, key string) string {
	if val := chi.URLParam(r, key); val != "" {
		return strings.TrimSpace(val)
	}
	if key == "path" {
		if val := chi.URLParam(r, "*"); val != "" {
			return val
		}
	}
	return strings.TrimSpace(r.PathValue(key))
}

// getRouteParamInt extracts a named URL parameter and parses it as an integer.
func getRouteParamInt(r *http.Request, key string) (int, error) {
	val := getRouteParam(r, key)
	if val == "" {
		return 0, fmt.Errorf("missing route parameter %q", key)
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid integer for parameter %q: %w", key, err)
	}
	return n, nil
}

// getQueryParam extracts a query parameter from the URL, trimming whitespace and returning
// fallback if empty.
func getQueryParam(r *http.Request, key, fallback string) string {
	val := strings.TrimSpace(r.URL.Query().Get(key))
	if val == "" {
		return fallback
	}
	return val
}

// parseBoundedInt parses an integer string, clamping it between minVal and maxVal.
// If empty or invalid, fallback is returned. When maxVal <= 0, no upper bound is enforced.
func parseBoundedInt(raw string, fallback, minVal, maxVal int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if parsed < minVal {
		return minVal
	}
	if maxVal > 0 && parsed > maxVal {
		return maxVal
	}
	return parsed
}

// getQueryParamInt parses an integer query parameter clamped within [minVal, maxVal].
// If maxVal <= 0, no upper bound is enforced.
func getQueryParamInt(r *http.Request, key string, fallback, minVal, maxVal int) int {
	return parseBoundedInt(r.URL.Query().Get(key), fallback, minVal, maxVal)
}

// RenderTempl renders a templ component to the standard HTTP response writer.
func RenderTempl(w http.ResponseWriter, r *http.Request, component templ.Component) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return component.Render(r.Context(), w)
}

// RenderDatastar renders a templ component patch via Datastar SSE using Handler configuration.
func (h *Handler) RenderDatastar(w http.ResponseWriter, r *http.Request, c templ.Component, opts ...datastar.PatchElementOption) error {
	var cfg *config.Config
	if h != nil {
		cfg = h.cfg
	}
	return RenderDatastarWithConfig(w, r, cfg, c, opts...)
}

// RenderDatastarWithConfig renders a templ component patch via Datastar SSE with explicit config options.
func RenderDatastarWithConfig(w http.ResponseWriter, r *http.Request, cfg *config.Config, c templ.Component, opts ...datastar.PatchElementOption) error {
	sse := datastar.NewSSE(w, r, sseOpts...)
	allOpts := patchOpts(cfg, "", opts...)
	if err := sse.PatchElementTempl(c, allOpts...); err != nil {
		return fmt.Errorf("patch element via datastar: %w", err)
	}
	return nil
}

// patchFeedbackNotice resets the form-feedback visibility signal before
// patching a modal notice: a previous Dismiss/Add-Another sets
// _feedbackVisible false, and the fresh notice must show even then.
func patchFeedbackNotice(sse *datastar.ServerSentEventGenerator, c templ.Component) error {
	_ = sse.PatchSignals([]byte(`{"_feedbackVisible": true}`))
	return sse.PatchElementTempl(c)
}

// writeJSON writes a JSON response with status code and sets Content-Type.
func writeJSON(w http.ResponseWriter, status int, data any) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(data)
}

// writeError writes a JSON error response with the provided status and error message.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func wantsJSONResponse(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/json") &&
		!strings.Contains(r.Header.Get("Accept"), "text/event-stream") &&
		r.Header.Get("Datastar-Request") == "" &&
		r.Header.Get("X-Datastar-Request") == ""
}

// currentGenreFromRequest infers the genre from the request Referer URL's "genre" query parameter.
// It returns the matched genre when it is in the allowed set; otherwise it falls back to "rock_metal".
// If the Referer header is missing or cannot be parsed, "rock_metal" is returned.
func currentGenreFromRequest(r *http.Request) string {
	const defaultGenre = "rock_metal"

	ref := r.Referer()
	if ref == "" {
		return defaultGenre
	}

	parsed, err := url.Parse(ref)
	if err != nil {
		return defaultGenre
	}

	genre := parsed.Query().Get("genre")
	if genre != "" && allowedGenreGroups[genre] {
		return genre
	}

	return defaultGenre
}

func formatUpdatedAt(raw string) string {
	if raw == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t, err = time.Parse("2006-01-02 15:04:05.000Z", raw)
	}
	if err != nil {
		t, err = time.Parse("2006-01-02 15:04:05", raw)
	}
	if err != nil {
		return raw
	}
	return t.UTC().Format("02 Jan 2006 15:04:05 UTC")
}
