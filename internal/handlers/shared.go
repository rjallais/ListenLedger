//go:build goexperiment.jsonv2

package handlers

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"
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

func renderDatastar(e *core.RequestEvent, c templ.Component) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	return sse.PatchElementTempl(c)
}

func wantsJSONResponse(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/json") &&
		!strings.Contains(r.Header.Get("Accept"), "text/event-stream") &&
		r.Header.Get("Datastar-Request") == "" &&
		r.Header.Get("X-Datastar-Request") == ""
}

// currentGenreFromRequest infers the genre from the request Referer URL's "genre" query parameter.
// It returns "everything_else" when that parameter equals "everything_else"; otherwise it returns "rock_metal".
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
	if genre == "everything_else" {
		return genre
	}

	return defaultGenre
}

func formatUpdatedAt(raw string) string {
	if raw == "" {
		return ""
	}

	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.Format("Jan 2, 2006")
	}

	return raw
}
