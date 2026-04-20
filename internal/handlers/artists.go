//go:build goexperiment.jsonv2

package handlers

import (
	"fmt"
	"net/http"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/templates"
)

// handleCreateArtist creates a new artist from form data.
func (h *Handler) handleCreateArtist(e *core.RequestEvent) error {
	input, err := parseArtistCreateInput(e.Request)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	// Check if spotify_id already exists
	existingRecords, err := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id = {:spotify_id}",
		"",
		1,
		0,
		dbx.Params{"spotify_id": input.spotifyID},
	)
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
	record.Set("total_songs", 0) // Not stored, calculated dynamically

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create artist"})
	}

	// Get total count for this genre to calculate dynamic total_songs.
	totalCount := 0
	totalCount, err = h.countArtistsByGenreExcludingWaiting(input.genreGroup)
	if err != nil {
		totalCount = 0
	}

	// Return the new artist row as SSE fragment
	return renderDatastar(e, templates.NewArtistCreateResponse(artistFromRecord(record, totalCount)))
}

func (h *Handler) handleArtists(e *core.RequestEvent) error {
	params := parseArtistListParams(e.Request)
	filter := nonWaitingArtistListFilter(params.genre)
	filterParams := nonWaitingArtistParams(params.genre)

	// Get total count for pagination (excluding waiting).
	totalCount, err := h.countArtistsByGenreExcludingWaiting(params.genre)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	totalPages := (totalCount + params.limit - 1) / params.limit

	// Fetch paginated artists
	offset := (params.page - 1) * params.limit
	records, err := h.app.FindRecordsByFilter(
		"artists",
		filter,
		"-monthly_listeners",
		params.limit,
		offset,
		filterParams,
	)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Get counts for each genre (excluding waiting).
	rockMetalCount, err := h.countArtistsByGenreExcludingWaiting("rock_metal")
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	everythingElseCount, err := h.countArtistsByGenreExcludingWaiting("everything_else")
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Get waiting artists count (for queue section).
	waitingCount, err := h.countWaitingArtists()
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Convert to type-safe structs
	artists := artistsFromRecords(records, func(index int, _ *core.Record) int {
		return rankedArtistTotalSongs(totalCount, offset, index)
	})

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
	params := parseWaitingArtistListParams(e.Request)

	// Fetch waiting artists
	records, err := h.app.FindRecordsByFilter(
		"artists",
		"list_status = {:waiting}",
		"-monthly_listeners,name",
		params.limit,
		params.offset,
		dbx.Params{"waiting": waitingArtistStatus},
	)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load waiting artists")
	}

	// Get total count
	totalCount, err := h.countWaitingArtists()
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load waiting artists")
	}
	hasMore := params.offset+len(records) < totalCount

	// Convert to type-safe structs
	artists := artistsFromRecords(records, func(_ int, record *core.Record) int {
		return record.GetInt("collection_songs")
	})

	// NOTE: WaitingArtistRows uses data-merge-mode="append" so previously shown artists remain visible.
	return renderDatastar(e, templates.WaitingArtistRows(artists, params.offset+len(records), hasMore))
}
