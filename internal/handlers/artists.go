//go:build goexperiment.jsonv2

package handlers

import (
	"fmt"
	"log"
	"net/http"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/templates"
)

// handleCreateArtist creates a new artist from form data.
func (h *Handler) handleCreateArtist(e *core.RequestEvent) error {
	ctx := e.Request.Context()
	input, err := parseArtistCreateInput(e.Request)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	// Check if spotify_id already exists
	existingRecords := make([]*core.Record, 0)
	err = h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp("spotify_id = {:spotify_id}", dbx.Params{"spotify_id": input.spotifyID})).
		Limit(1).
		All(&existingRecords)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to check for existing artist"})
	}
	if len(existingRecords) > 0 {
		existingName := existingRecords[0].GetString("name")
		return e.JSON(http.StatusConflict, map[string]string{
			"error": fmt.Sprintf("Artist ID already exists: %s", existingName),
		})
	}

	// Create new artist record
	collection, err := h.app.FindCollectionByNameOrId("artists")
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "artists collection not found"})
	}

	record := core.NewRecord(collection)
	record.Set("name", input.name)
	record.Set("spotify_id", input.spotifyID)
	record.Set("genre_group", input.genreGroup)
	record.Set("list_status", input.listStatus)
	record.Set("fetch_status", "idle")
	record.Set("monthly_listeners", input.monthlyListeners)
	record.Set("collection_songs", input.collectionSongs)
	record.Set("total_songs", 0)

	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create artist"})
	}

	// Get total count for this genre to calculate dynamic total_songs.
	totalCount := 0
	totalCount, err = h.countArtistsByGenreExcludingWaiting(ctx, input.genreGroup)
	if err != nil {
		totalCount = 0
	}

	// Return the new artist row as SSE fragment
	return renderDatastar(e, templates.NewArtistCreateResponse(artistFromRecord(record, totalCount)))
}

func (h *Handler) handleArtists(e *core.RequestEvent) error {
	ctx := e.Request.Context()
	params := parseArtistListParams(e.Request)
	filterParams := nonWaitingArtistParams(params.genre)

	// Get total count for pagination (excluding waiting).
	totalCount, err := h.countArtistsByGenreExcludingWaiting(ctx, params.genre)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	totalPages := (totalCount + params.limit - 1) / params.limit

	// Fetch paginated artists
	offset := (params.page - 1) * params.limit
	records := make([]*core.Record, 0)
	err = h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(nonWaitingArtistFilter, filterParams)).
		OrderBy("monthly_listeners DESC").
		Limit(int64(params.limit)).
		Offset(int64(offset)).
		All(&records)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Get counts for each genre (excluding waiting).
	rockMetalCount, err := h.countArtistsByGenreExcludingWaiting(ctx, "rock_metal")
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	everythingElseCount, err := h.countArtistsByGenreExcludingWaiting(ctx, "everything_else")
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Get waiting artists count (for queue section).
	waitingCount, err := h.countWaitingArtists(ctx)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Build rank cache for O(1) lookup (avoids O(N) per artist)
	rankCache, err := h.buildArtistRankMap(ctx, params.genre)
	if err != nil {
		log.Printf("[handlers] warning: failed to build rank cache: %v", err)
	}

	// Convert to type-safe structs using rank cache
	artists := artistsFromRecords(records, rankCache)

	pagination := templates.Pagination{
		CurrentPage: params.page,
		TotalPages:  totalPages,
		Limit:       params.limit,
		TotalCount:  totalCount,
		Genre:       params.genre,
	}

	return renderTempl(e, templates.ArtistsPage(artists, rockMetalCount, everythingElseCount, params.genre, waitingCount, pagination))
}

// handleWaitingArtistsAPI returns waiting artist cards for lazy loading.
func (h *Handler) handleWaitingArtistsAPI(e *core.RequestEvent) error {
	ctx := e.Request.Context()
	params := parseWaitingArtistListParams(e.Request)

	// Fetch waiting artists
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp("list_status = {:waiting}", dbx.Params{"waiting": waitingArtistStatus})).
		OrderBy("monthly_listeners DESC", "name").
		Limit(int64(params.limit)).
		Offset(int64(params.offset)).
		All(&records)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load waiting artists")
	}

	// Get total count
	totalCount, err := h.countWaitingArtists(ctx)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load waiting artists")
	}
	hasMore := params.offset+len(records) < totalCount

	// Convert to type-safe structs (waiting artists don't need ranking)
	artists := artistsFromRecords(records, nil)

	// NOTE: WaitingArtistRows uses data-merge-mode="append" so previously shown artists remain visible.
	return renderDatastar(e, templates.WaitingArtistRows(artists, params.offset+len(records), hasMore))
}
