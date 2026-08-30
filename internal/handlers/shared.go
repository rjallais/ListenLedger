package handlers

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/pocketbase/pocketbase/core"
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

// renderTempl renders a templ component to the HTTP response.
func renderTempl(e *core.RequestEvent, component templ.Component) error {
	e.Response.Header().Set("Content-Type", "text/html; charset=utf-8")
	return component.Render(e.Request.Context(), e.Response)
}

func renderDatastar(e *core.RequestEvent, c templ.Component, opts ...datastar.PatchElementOption) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	if err := sse.PatchElementTempl(c, opts...); err != nil {
		return fmt.Errorf("patch element via datastar: %w", err)
	}
	return nil
}

func formatBatchSignal(id string, total, completed int, done bool) []byte {
	return fmt.Appendf(
		nil,
		`{"batchID":%q,"batchTotal":%d,"batchCompleted":%d,"batchDone":%t}`,
		id,
		total,
		completed,
		done,
	)
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

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.000Z",
		"2006-01-02 15:04:05Z",
	}

	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.Local().Format("Jan 2, 2006 3:04 PM")
		}
	}

	return raw
}
