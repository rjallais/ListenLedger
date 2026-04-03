//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/priority"
	"ListenLedger/internal/quota"
	"ListenLedger/templates"
)

// handleCreateArtist creates a new artist from form data.
func (h *Handler) handleCreateArtist(e *core.RequestEvent) error {
	// Parse form data
	if err := e.Request.ParseForm(); err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid form data"})
	}

	name := e.Request.FormValue("name")
	if name == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist name is required"})
	}

	spotifyID := e.Request.FormValue("spotify_id")
	if spotifyID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID is required"})
	}

	// Check if spotify_id already exists
	existingRecords, err := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id = {:spotify_id}",
		"",
		1,
		0,
		dbx.Params{"spotify_id": spotifyID},
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

	genreGroup := e.Request.FormValue("genre_group")
	if genreGroup == "" {
		genreGroup = "rock_metal" // Default
	} else if !allowedGenreGroups[genreGroup] {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"error": "genre_group must be rock_metal or everything_else",
		})
	}

	listStatus := e.Request.FormValue("list_status")
	if listStatus == "" {
		listStatus = "recently_added" // Default for newly added artists
	} else if !allowedListStatuses[listStatus] {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"error": "list_status must be included, recently_added, not_added, or waiting",
		})
	}

	// Parse optional numeric fields
	monthlyListeners := 0
	if ml := e.Request.FormValue("monthly_listeners"); ml != "" {
		if parsed, err := strconv.Atoi(ml); err == nil && parsed >= 0 {
			monthlyListeners = parsed
		}
	}

	collectionSongs := 0
	if cs := e.Request.FormValue("collection_songs"); cs != "" {
		if parsed, err := strconv.Atoi(cs); err == nil && parsed >= 0 {
			collectionSongs = parsed
		}
	}

	// Create new artist record
	collection, err := h.app.FindCollectionByNameOrId("artists")
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "artists collection not found"})
	}

	record := core.NewRecord(collection)
	record.Set("name", name)
	record.Set("spotify_id", spotifyID)
	record.Set("genre_group", genreGroup)
	record.Set("list_status", listStatus)
	record.Set("fetch_status", "idle")
	record.Set("monthly_listeners", monthlyListeners)
	record.Set("collection_songs", collectionSongs)
	record.Set("total_songs", 0) // Not stored, calculated dynamically

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create artist"})
	}

	// Get total count for this genre to calculate dynamic total_songs.
	totalCount := 0
	totalCount64, err := h.app.CountRecords("artists", dbx.NewExp(
		"genre_group = {:genre} AND list_status != {:waiting}",
		dbx.Params{"genre": genreGroup, "waiting": "waiting"},
	))
	if err == nil {
		totalCount = int(totalCount64)
	}

	// Return the new artist row as SSE fragment
	artist := templates.Artist{
		ID:               record.Id,
		Name:             record.GetString("name"),
		SpotifyID:        record.GetString("spotify_id"),
		MonthlyListeners: record.GetInt("monthly_listeners"),
		GenreGroup:       record.GetString("genre_group"),
		ListStatus:       record.GetString("list_status"),
		FetchStatus:      record.GetString("fetch_status"),
		CollectionSongs:  record.GetInt("collection_songs"),
		TotalSongs:       totalCount, // New artist goes to top, gets highest count
		LastUpdated:      formatUpdatedAt(record.GetString("last_updated")),
	}

	return renderDatastar(e, templates.NewArtistCreateResponse(artist))
}

func (h *Handler) handleArtists(e *core.RequestEvent) error {
	// Pagination parameters
	page := 1
	limit := 50
	if p := e.Request.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if l := e.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	// Get genre filter from query
	genreFilter := e.Request.URL.Query().Get("genre")
	if genreFilter != "rock_metal" && genreFilter != "everything_else" {
		genreFilter = "rock_metal" // Default tab
	}

	// Build filter - exclude 'waiting' artists from main table.
	filter := "genre_group = {:genre} && list_status != {:waiting}"
	filterParams := dbx.Params{"genre": genreFilter, "waiting": "waiting"}

	// Get total count for pagination (excluding waiting).
	totalCount64, err := h.app.CountRecords("artists", dbx.NewExp(
		"genre_group = {:genre} AND list_status != {:waiting}",
		filterParams,
	))
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	totalCount := int(totalCount64)
	totalPages := (totalCount + limit - 1) / limit

	// Fetch paginated artists
	offset := (page - 1) * limit
	records, err := h.app.FindRecordsByFilter(
		"artists",
		filter,
		"-monthly_listeners",
		limit,
		offset,
		filterParams,
	)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}

	// Get counts for each genre (excluding waiting).
	rockMetalCount64, err := h.app.CountRecords("artists", dbx.NewExp(
		"genre_group = {:genre} AND list_status != {:waiting}",
		dbx.Params{"genre": "rock_metal", "waiting": "waiting"},
	))
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	everythingElseCount64, err := h.app.CountRecords("artists", dbx.NewExp(
		"genre_group = {:genre} AND list_status != {:waiting}",
		dbx.Params{"genre": "everything_else", "waiting": "waiting"},
	))
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	rockMetalCount := int(rockMetalCount64)
	everythingElseCount := int(everythingElseCount64)

	// Get waiting artists count (for queue section).
	waitingCount64, err := h.app.CountRecords("artists", dbx.HashExp{"list_status": "waiting"})
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load artists")
	}
	waitingCount := int(waitingCount64)

	// Convert to type-safe structs
	// TotalSongs is calculated dynamically as reverse position in the list
	// Position 1 (most listeners) gets totalCount, position 2 gets totalCount-1, etc.
	artists := make([]templates.Artist, 0, len(records))
	for i, r := range records {
		// Calculate position (1-indexed) in the full list
		position := offset + i + 1
		// Reverse position: artist at top gets totalCount, last artist gets 1
		dynamicTotalSongs := totalCount - position + 1

		artists = append(artists, templates.Artist{
			ID:               r.Id,
			Name:             r.GetString("name"),
			SpotifyID:        r.GetString("spotify_id"),
			MonthlyListeners: r.GetInt("monthly_listeners"),
			GenreGroup:       r.GetString("genre_group"),
			ListStatus:       r.GetString("list_status"),
			FetchStatus:      r.GetString("fetch_status"),
			CollectionSongs:  r.GetInt("collection_songs"),
			TotalSongs:       dynamicTotalSongs,
			LastUpdated:      formatUpdatedAt(r.GetString("last_updated")),
		})
	}

	pagination := templates.Pagination{
		CurrentPage: page,
		TotalPages:  totalPages,
		Limit:       limit,
		TotalCount:  totalCount,
		Genre:       genreFilter,
	}

	return renderTempl(e, templates.ArtistsPage(artists, rockMetalCount, everythingElseCount, genreFilter, waitingCount, pagination))
}

// handleWaitingArtistsAPI returns waiting artist cards for lazy loading.
func (h *Handler) handleWaitingArtistsAPI(e *core.RequestEvent) error {
	// Parse pagination params
	offset := 0
	limit := 1 // Default to 1 at a time
	if o := e.Request.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	if l := e.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 10 {
			limit = parsed
		}
	}

	// Fetch waiting artists
	records, err := h.app.FindRecordsByFilter(
		"artists",
		"list_status = {:waiting}",
		"-monthly_listeners,name",
		limit,
		offset,
		dbx.Params{"waiting": "waiting"},
	)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load waiting artists")
	}

	// Get total count
	totalCount64, err := h.app.CountRecords("artists", dbx.HashExp{"list_status": "waiting"})
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load waiting artists")
	}
	totalCount := int(totalCount64)
	hasMore := offset+len(records) < totalCount

	// Convert to type-safe structs
	artists := make([]templates.Artist, 0, len(records))
	for _, r := range records {
		collectionSongs := r.GetInt("collection_songs")
		artists = append(artists, templates.Artist{
			ID:               r.Id,
			Name:             r.GetString("name"),
			SpotifyID:        r.GetString("spotify_id"),
			MonthlyListeners: r.GetInt("monthly_listeners"),
			GenreGroup:       r.GetString("genre_group"),
			ListStatus:       r.GetString("list_status"),
			FetchStatus:      r.GetString("fetch_status"),
			CollectionSongs:  collectionSongs,
			TotalSongs:       collectionSongs,
			LastUpdated:      formatUpdatedAt(r.GetString("last_updated")),
		})
	}

	// NOTE: WaitingArtistRows uses data-merge-mode="append" so previously shown artists remain visible.
	return renderDatastar(e, templates.WaitingArtistRows(artists, offset+len(records), hasMore))
}

func (h *Handler) dynamicArtistTotalSongs(record *core.Record) int {
	collectionSongs := record.GetInt("collection_songs")
	if record.GetString("list_status") == "waiting" {
		return collectionSongs
	}

	filter := "genre_group = {:genre} && list_status != {:waiting}"
	filterParams := dbx.Params{
		"genre":   record.GetString("genre_group"),
		"waiting": "waiting",
	}

	totalCount64, err := h.app.CountRecords("artists", dbx.NewExp(
		"genre_group = {:genre} AND list_status != {:waiting}",
		filterParams,
	))
	if err != nil {
		return collectionSongs
	}
	totalCount := int(totalCount64)
	if totalCount == 0 {
		return collectionSongs
	}

	records, err := h.app.FindRecordsByFilter(
		"artists",
		filter,
		"-monthly_listeners",
		totalCount,
		0,
		filterParams,
	)
	if err != nil {
		return collectionSongs
	}

	for i, candidate := range records {
		if candidate.Id == record.Id {
			position := i + 1
			return totalCount - position + 1
		}
	}

	return collectionSongs
}

// handleRefresh triggers a refresh request for an artist.
func (h *Handler) handleRefresh(e *core.RequestEvent) error {
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	// Check if we have quota available before accepting the request
	checker := quota.NewChecker(h.cfg)
	if !checker.HasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available. Please check /api/quota for details.",
		})
	}

	// Find the artist record
	record, err := h.app.FindRecordById("artists", artistID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
	}

	// Create and publish scrape request via durable JetStream stream.
	requestID := strconv.FormatInt(time.Now().UnixNano(), 10)

	req := messaging.NewScrapeRequested(
		record.Id,
		record.GetString("spotify_id"),
		record.GetString("name"),
		requestID,
	)

	ctx, cancel := context.WithTimeout(e.Request.Context(), 5*time.Second)
	defer cancel()

	ack, err := h.publishScrapeRequest(ctx, req)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to queue refresh"})
	}
	if ack != nil && ack.Duplicate {
		log.Printf("[handlers] Duplicate refresh request ignored for artist %s", record.Id)
		if wantsJSONResponse(e.Request) {
			return e.JSON(http.StatusOK, map[string]string{"status": "already_queued"})
		}
		sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
		payload := fmt.Sprintf(`{"artistFetchStatus":{%q:"pending"}}`, record.Id)
		return sse.PatchSignals([]byte(payload))
	}

	correlation.Associate(record.Id, requestID)
	h.createScrapeJobRecord(requestID, record.Id)

	record.Set("fetch_status", "pending")
	if err := h.app.Save(record); err != nil {
		log.Printf("[handlers] Warning: failed to update fetch_status: %v", err)
	}

	if wantsJSONResponse(e.Request) {
		return e.JSON(http.StatusOK, map[string]string{"status": "queued"})
	}

	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	payload := fmt.Sprintf(`{"artistFetchStatus":{%q:"pending"}}`, record.Id)
	return sse.PatchSignals([]byte(payload))
}

func (h *Handler) handleBatchRefresh(e *core.RequestEvent) error {
	h.ensureBatchProgressSubscriber()

	if err := e.Request.ParseForm(); err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid form data"})
	}

	if batchID := strings.TrimSpace(e.Request.FormValue("batch_id")); batchID != "" {
		if snapshot, ok := h.getBatchSnapshot(batchID); ok {
			log.Printf(
				"[batch] Resuming batch %s (%d/%d complete)",
				snapshot.ID,
				snapshot.Completed,
				snapshot.Total,
			)
			return h.patchBatchRefreshState(e, snapshot)
		}
	}

	if snapshot, ok := h.getActiveBatchSnapshot(); ok {
		log.Printf(
			"[batch] Active batch %s already running (%d/%d complete); returning current state",
			snapshot.ID,
			snapshot.Completed,
			snapshot.Total,
		)
		return h.patchBatchRefreshState(e, snapshot)
	}

	countStr := e.Request.FormValue("count")
	if countStr == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "count required"})
	}
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "count must be a positive integer"})
	}

	checker := quota.NewChecker(h.cfg)
	if !checker.HasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available.",
		})
	}

	fourHoursAgo := time.Now().Add(-4 * time.Hour)
	// Match the PocketBase DateTime format (space-separated, not RFC3339 T-separated)
	// so SQLite string comparison works correctly.
	pbCutoff := fourHoursAgo.UTC().Format("2006-01-02 15:04:05.000Z")
	records, err := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id != '' && spotify_id != null && (last_updated = '' || last_updated < {:cutoff})",
		"-monthly_listeners",
		0, 0,
		dbx.Params{"cutoff": pbCutoff},
	)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to fetch artists"})
	}

	var jobs []priority.Job
	for _, r := range records {
		p := priority.Determine(r)
		jobs = append(jobs, priority.Job{Record: r, Priority: p})
	}

	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority < jobs[j].Priority
		}
		return jobs[i].Record.GetInt("monthly_listeners") > jobs[j].Record.GetInt("monthly_listeners")
	})

	if count > len(jobs) {
		count = len(jobs)
	}
	jobs = jobs[:count]

	queuedArtistIDs := make([]string, 0, len(jobs))
	stats := make(map[string]int)
	for _, job := range jobs {
		r := job.Record
		requestID := strconv.FormatInt(time.Now().UnixNano(), 10)

		req := messaging.NewScrapeRequested(
			r.Id,
			r.GetString("spotify_id"),
			r.GetString("name"),
			requestID,
		)

		ctx, cancel := context.WithTimeout(e.Request.Context(), 5*time.Second)
		ack, err := h.publishScrapeRequest(ctx, req)
		cancel()

		if err != nil {
			log.Printf("[batch] Failed to queue %s: %v", r.GetString("name"), err)
			continue
		}
		if ack != nil && ack.Duplicate {
			log.Printf("[batch] Duplicate request for %s skipped", r.GetString("name"))
			continue
		}

		correlation.Associate(r.Id, requestID)
		h.createScrapeJobRecord(requestID, r.Id)
		queuedArtistIDs = append(queuedArtistIDs, r.Id)
		stats[job.Priority.String()]++
		r.Set("fetch_status", "pending")
		if err := h.app.Save(r); err != nil {
			log.Printf("[batch] Warning: failed to mark artist %s pending: %v", r.Id, err)
		}
	}

	snapshot := h.createBatchProgress(queuedArtistIDs, stats)
	log.Printf("[batch] Created batch %s with %d queued artist(s)", snapshot.ID, snapshot.Total)
	return h.patchBatchRefreshState(e, snapshot)
}

// handleUpdateListStatus updates the list_status of an artist.
func (h *Handler) handleUpdateListStatus(e *core.RequestEvent) error {
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	// Get status from path parameter
	newStatus := e.Request.PathValue("status")

	// Validate status value
	validStatuses := map[string]bool{
		"included":       true,
		"recently_added": true,
		"not_added":      true,
		"waiting":        true,
	}
	if !validStatuses[newStatus] {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status value"})
	}

	// Find and update the artist record
	record, err := h.app.FindRecordById("artists", artistID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
	}

	// Track old status to determine if we need to remove from queue
	oldStatus := record.GetString("list_status")

	record.Set("list_status", newStatus)
	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist"})
	}

	currentGenre := currentGenreFromRequest(e.Request)
	collectionSongs := record.GetInt("collection_songs")
	totalSongs := h.dynamicArtistTotalSongs(record)

	// Build artist struct for response
	artist := templates.Artist{
		ID:               record.Id,
		Name:             record.GetString("name"),
		SpotifyID:        record.GetString("spotify_id"),
		MonthlyListeners: record.GetInt("monthly_listeners"),
		GenreGroup:       record.GetString("genre_group"),
		ListStatus:       record.GetString("list_status"),
		FetchStatus:      record.GetString("fetch_status"),
		CollectionSongs:  collectionSongs,
		TotalSongs:       totalSongs,
		LastUpdated:      formatUpdatedAt(record.GetString("last_updated")),
	}

	if oldStatus != newStatus && (oldStatus == "waiting" || newStatus == "waiting") {
		return renderDatastar(e, templates.ArtistStatusTransition(oldStatus, artist, currentGenre))
	}

	if newStatus == "waiting" {
		return renderDatastar(e, templates.WaitingArtistCard(artist))
	}

	// Otherwise return the updated row for the main table
	return renderDatastar(e, templates.ArtistRow(artist))
}

// handleUpdateCollectionSongs increments or decrements the collection_songs count.
func (h *Handler) handleUpdateCollectionSongs(e *core.RequestEvent) error {
	artistID := e.Request.PathValue("artistId")
	if artistID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist ID required"})
	}

	action := e.Request.PathValue("action")
	if action != "inc" && action != "dec" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "action must be 'inc' or 'dec'"})
	}

	// Find the artist record
	record, err := h.app.FindRecordById("artists", artistID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "artist not found"})
	}

	// Update collection_songs
	currentCount := record.GetInt("collection_songs")
	if action == "inc" {
		record.Set("collection_songs", currentCount+1)
	} else if currentCount > 0 {
		record.Set("collection_songs", currentCount-1)
	}

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist"})
	}

	collectionSongs := record.GetInt("collection_songs")

	// Return the updated artist row as SSE fragment for Datastar
	artist := templates.Artist{
		ID:               record.Id,
		Name:             record.GetString("name"),
		SpotifyID:        record.GetString("spotify_id"),
		MonthlyListeners: record.GetInt("monthly_listeners"),
		GenreGroup:       record.GetString("genre_group"),
		ListStatus:       record.GetString("list_status"),
		FetchStatus:      record.GetString("fetch_status"),
		CollectionSongs:  collectionSongs,
		TotalSongs:       collectionSongs,
		LastUpdated:      formatUpdatedAt(record.GetString("last_updated")),
	}

	return renderDatastar(e, templates.ArtistRow(artist))
}
