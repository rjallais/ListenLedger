//go:build goexperiment.jsonv2

// Package handlers provides HTTP route handlers for the web application.
package handlers

import (
	"MonthlyListeners/config"
	"MonthlyListeners/internal/correlation"
	"MonthlyListeners/internal/messaging"
	"MonthlyListeners/internal/priority"
	"MonthlyListeners/internal/quota"
	"MonthlyListeners/templates"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a-h/templ"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/router"
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

var (
	batchTracker   sync.Map
	batchCounter   int64
	batchCounterMu sync.Mutex
)

type Handler struct {
	app *pocketbase.PocketBase
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg *config.Config
}

// New creates a new Handler instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Handler {
	return &Handler{
		app: app,
		nc:  nc,
		js:  js,
		cfg: cfg,
	}
}

// RegisterRoutes registers all HTTP routes with the router.
func (h *Handler) RegisterRoutes(r *router.Router[*core.RequestEvent]) {
	// Static files (CSS, JS, images)
	r.GET("/static/{path...}", h.handleStatic)

	// Main views
	r.GET("/", h.handleIndex)
	r.GET("/albums", h.handleAlbums)
	r.GET("/artists", h.handleArtists)
	r.GET("/songs", h.handleSongs)

	// Album lazy loading endpoints
	r.GET("/api/albums/{status}", h.handleAlbumsAPI)
	r.POST("/api/albums", h.handleCreateAlbum)
	r.POST("/api/albums/{albumId}/status/{status}", h.handleUpdateAlbumStatus)
	r.POST("/api/albums/{albumId}/collection/{action}", h.handleUpdateAlbumCollectionSongs)
	r.POST("/api/albums/{albumId}/total/{action}", h.handleUpdateAlbumTotalSongs)

	// Artist lazy loading endpoints
	r.GET("/api/artists/waiting", h.handleWaitingArtistsAPI)

	r.POST("/api/refresh/batch", h.handleBatchRefresh)

	// API endpoints
	r.POST("/api/refresh/{artistId}", h.handleRefresh)
	r.POST("/api/artists", h.handleCreateArtist)
	r.POST("/api/songs", h.handleCreateSong)
	r.POST("/api/artists/{artistId}/status/{status}", h.handleUpdateListStatus)
	r.POST("/api/artists/{artistId}/collection/{action}", h.handleUpdateCollectionSongs)
	r.GET("/api/events", h.handleSSE)
	r.GET("/api/quota", h.handleQuota)
	r.GET("/api/queue", h.handleQueue)

	log.Println("[handlers] Routes registered")
}

// handleStatic serves static files from the static directory.
func (h *Handler) handleStatic(e *core.RequestEvent) error {
	path := e.Request.PathValue("path")
	return e.FileFS(os.DirFS("static"), path)
}

// handleIndex redirects to albums view.
func (h *Handler) handleIndex(e *core.RequestEvent) error {
	return e.Redirect(http.StatusFound, "/albums")
}

// renderTempl renders a templ component to the HTTP response.
func renderTempl(e *core.RequestEvent, component templ.Component) error {
	e.Response.Header().Set("Content-Type", "text/html; charset=utf-8")
	return component.Render(e.Request.Context(), e.Response)
}

// handleAlbums renders the albums view.
func (h *Handler) handleAlbums(e *core.RequestEvent) error {
	fullCount, err := h.app.CountRecords("albums", dbx.HashExp{"status": "full"})
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load albums")
	}

	processedCount, err := h.app.CountRecords("albums", dbx.HashExp{"status": "processed_once"})
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load albums")
	}

	waitingCount, err := h.app.CountRecords("albums", dbx.NewExp(
		"status != {:full} AND status != {:processed}",
		dbx.Params{"full": "full", "processed": "processed_once"},
	))
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load albums")
	}

	return renderTempl(e, templates.AlbumsPage(int(fullCount), int(processedCount), int(waitingCount)))
}

func albumStatusForUI(status string) string {
	switch status {
	case "full":
		return "full"
	case "processed_once":
		return "processed"
	case "waiting":
		return "waiting"
	default:
		return "waiting"
	}
}

func albumStatusForDB(status string) (string, bool) {
	switch status {
	case "full":
		return "full", true
	case "processed":
		return "processed_once", true
	case "waiting":
		return "waiting", true
	case "processed_once":
		return "processed_once", true
	default:
		return "", false
	}
}

func albumFromRecord(r *core.Record) templates.Album {
	return templates.Album{
		ID:              r.Id,
		Title:           r.GetString("title"),
		ArtistName:      r.GetString("artist_name"),
		CollectionSongs: r.GetInt("collection_songs"),
		TotalSongs:      r.GetInt("total_songs"),
		Status:          albumStatusForUI(r.GetString("status")),
	}
}

// handleAlbumsAPI returns album rows for lazy loading.
func (h *Handler) handleAlbumsAPI(e *core.RequestEvent) error {
	status := e.Request.PathValue("status")

	// Parse pagination params
	offset := 0
	limit := 50
	if o := e.Request.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	if l := e.Request.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	// Map status param to database expression and query ordering.
	var filterExpr dbx.Expression
	var orderExpr string
	switch status {
	case "full":
		filterExpr = dbx.HashExp{"status": "full"}
		orderExpr = "`total_songs` DESC, LOWER(`title`) ASC"
	case "processed":
		filterExpr = dbx.HashExp{"status": "processed_once"}
		orderExpr = "(`total_songs` - `collection_songs`) DESC, " +
			"CASE WHEN `total_songs` > 0 THEN CAST(`collection_songs` AS REAL) / `total_songs` ELSE 0 END DESC, " +
			"LOWER(`title`) ASC"
	case "waiting":
		filterExpr = dbx.NewExp(
			"status != {:full} AND status != {:processed}",
			dbx.Params{"full": "full", "processed": "processed_once"},
		)
		orderExpr = "CASE WHEN `total_songs` > 0 THEN CAST(`collection_songs` AS REAL) / `total_songs` ELSE 0 END DESC, " +
			"`collection_songs` DESC, LOWER(`title`) ASC"
		// For waiting albums, default to 1 at a time
		if e.Request.URL.Query().Get("limit") == "" {
			limit = 1
		}
	default:
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status"})
	}

	totalCount64, err := h.app.CountRecords("albums", filterExpr)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load albums")
	}
	totalCount := int(totalCount64)

	records := make([]*core.Record, 0, limit)
	if totalCount > 0 {
		if err := h.app.RecordQuery("albums").
			AndWhere(filterExpr).
			OrderBy(orderExpr).
			Offset(int64(offset)).
			Limit(int64(limit)).
			All(&records); err != nil {
			return e.String(http.StatusInternalServerError, "Failed to load albums")
		}
	}
	hasMore := offset+len(records) < totalCount

	// Convert to type-safe structs
	albums := make([]templates.Album, 0, len(records))
	for _, r := range records {
		albums = append(albums, albumFromRecord(r))
	}

	return renderDatastar(e, templates.AlbumRows(albums, status, offset+len(records), hasMore))
}

// handleCreateAlbum creates a new album from form data.
func (h *Handler) handleCreateAlbum(e *core.RequestEvent) error {
	if err := e.Request.ParseForm(); err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid form data"})
	}

	title := e.Request.FormValue("title")
	if title == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "album title is required"})
	}

	artistName := e.Request.FormValue("artist_name")
	if artistName == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "artist name is required"})
	}

	statusValue := e.Request.FormValue("status")
	if statusValue == "" {
		statusValue = "waiting"
	}
	statusDB, ok := albumStatusForDB(statusValue)
	if !ok {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status value"})
	}

	collectionSongs := 0
	if cs := e.Request.FormValue("collection_songs"); cs != "" {
		if parsed, err := strconv.Atoi(cs); err == nil && parsed >= 0 {
			collectionSongs = parsed
		}
	}

	totalSongs := 0
	if ts := e.Request.FormValue("total_songs"); ts != "" {
		if parsed, err := strconv.Atoi(ts); err == nil && parsed >= 0 {
			totalSongs = parsed
		}
	}
	if totalSongs > 0 && collectionSongs > totalSongs {
		totalSongs = collectionSongs
	}

	collection, err := h.app.FindCollectionByNameOrId("albums")
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "albums collection not found"})
	}

	record := core.NewRecord(collection)
	record.Set("title", title)
	record.Set("artist_name", artistName)
	record.Set("status", statusDB)
	record.Set("collection_songs", collectionSongs)
	record.Set("total_songs", totalSongs)

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create album"})
	}

	return renderDatastar(e, templates.NewAlbumEntry(albumFromRecord(record)))
}

// handleUpdateAlbumStatus updates the status of an album.
func (h *Handler) handleUpdateAlbumStatus(e *core.RequestEvent) error {
	albumID := e.Request.PathValue("albumId")
	if albumID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "album ID required"})
	}

	newStatus := e.Request.PathValue("status")
	statusDB, ok := albumStatusForDB(newStatus)
	if !ok {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status value"})
	}

	record, err := h.app.FindRecordById("albums", albumID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
	}

	oldStatus := albumStatusForUI(record.GetString("status"))
	record.Set("status", statusDB)
	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
	}

	album := albumFromRecord(record)
	if oldStatus == album.Status {
		if album.Status == "waiting" {
			return renderDatastar(e, templates.AlbumCard(album))
		}
		return renderDatastar(e, templates.AlbumRow(album))
	}

	return renderDatastar(e, templates.AlbumStatusTransition(oldStatus, album))
}

// handleUpdateAlbumCollectionSongs increments or decrements collection songs.
func (h *Handler) handleUpdateAlbumCollectionSongs(e *core.RequestEvent) error {
	albumID := e.Request.PathValue("albumId")
	if albumID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "album ID required"})
	}

	action := e.Request.PathValue("action")
	if action != "inc" && action != "dec" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "action must be 'inc' or 'dec'"})
	}

	record, err := h.app.FindRecordById("albums", albumID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
	}

	current := record.GetInt("collection_songs")
	total := record.GetInt("total_songs")
	if action == "inc" {
		current++
	} else if action == "dec" && current > 0 {
		current--
	}
	record.Set("collection_songs", current)
	if total > 0 && current > total {
		record.Set("total_songs", current)
	}

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
	}

	album := albumFromRecord(record)
	if album.Status == "waiting" {
		return renderDatastar(e, templates.AlbumCard(album))
	}
	return renderDatastar(e, templates.AlbumRow(album))
}

// handleUpdateAlbumTotalSongs increments or decrements total songs.
func (h *Handler) handleUpdateAlbumTotalSongs(e *core.RequestEvent) error {
	albumID := e.Request.PathValue("albumId")
	if albumID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "album ID required"})
	}

	action := e.Request.PathValue("action")
	if action != "inc" && action != "dec" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "action must be 'inc' or 'dec'"})
	}

	record, err := h.app.FindRecordById("albums", albumID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
	}

	total := record.GetInt("total_songs")
	if action == "inc" {
		total++
	} else if action == "dec" && total > 0 {
		total--
	}
	if total < 0 {
		total = 0
	}

	collection := record.GetInt("collection_songs")
	if total < collection {
		collection = total
		record.Set("collection_songs", collection)
	}
	record.Set("total_songs", total)

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
	}

	album := albumFromRecord(record)
	if album.Status == "waiting" {
		return renderDatastar(e, templates.AlbumCard(album))
	}
	return renderDatastar(e, templates.AlbumRow(album))
}

func renderDatastar(e *core.RequestEvent, c templ.Component) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	return sse.PatchElementTempl(c)
}

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
	existingRecords, _ := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id = {:spotify_id}",
		"",
		1,
		0,
		dbx.Params{"spotify_id": spotifyID},
	)
	if len(existingRecords) > 0 {
		existingName := existingRecords[0].GetString("name")
		return e.JSON(http.StatusConflict, map[string]string{
			"error": fmt.Sprintf("Artist ID already exists: %s", existingName),
		})
	}

	genreGroup := e.Request.FormValue("genre_group")
	if genreGroup == "" {
		genreGroup = "rock_metal" // Default
	}

	listStatus := e.Request.FormValue("list_status")
	if listStatus == "" {
		listStatus = "recently_added" // Default for newly added artists
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
	record.Set("last_updated", time.Now())
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
		LastUpdated:      record.GetDateTime("last_updated").Time().Format("Jan 2, 2006"),
	}

	return renderDatastar(e, templates.NewArtistRow(artist))
}

// handleCreateSong creates a new song and updates related album/artist counters.
func (h *Handler) handleCreateSong(e *core.RequestEvent) error {
	if err := e.Request.ParseForm(); err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid form data"})
	}

	songName := strings.TrimSpace(e.Request.FormValue("name"))
	if songName == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "song name is required"})
	}

	albumName := strings.TrimSpace(e.Request.FormValue("album"))
	if albumName == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "album is required"})
	}

	releaseYearRaw := strings.TrimSpace(e.Request.FormValue("release_year"))
	if releaseYearRaw == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "release year is required"})
	}
	releaseYear, err := strconv.Atoi(releaseYearRaw)
	if err != nil || releaseYear < 1000 || releaseYear > 9999 {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "release_year must be a valid year"})
	}

	artistSpotifyIDsRaw := e.Request.FormValue("artist_spotify_ids")
	if strings.TrimSpace(artistSpotifyIDsRaw) == "" {
		artistSpotifyIDsRaw = e.Request.FormValue("artists")
	}

	artistSpotifyIDs, err := parseSpotifyIDs(artistSpotifyIDsRaw)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	artists := make([]string, 0, len(artistSpotifyIDs))
	for _, artistSpotifyID := range artistSpotifyIDs {
		ctx, cancel := context.WithTimeout(e.Request.Context(), 8*time.Second)
		artistName, statusCode, inferErr := inferArtistNameFromSpotifyID(ctx, artistSpotifyID)
		cancel()
		if inferErr != nil {
			return e.JSON(statusCode, map[string]string{"error": inferErr.Error()})
		}
		artists = append(artists, artistName)
	}

	if err := h.upsertAlbumForSong(albumName, artists[0]); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album metadata"})
	}
	if err := h.upsertArtistsForSong(artists, artistSpotifyIDs); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist metadata"})
	}

	collection, err := h.app.FindCollectionByNameOrId("songs")
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "songs collection not found"})
	}

	record := core.NewRecord(collection)
	record.Set("title", songName)
	record.Set("artist_name", strings.Join(artists, ", "))
	record.Set("album", albumName)
	record.Set("release_year", releaseYear)
	record.Set("release_date", strconv.Itoa(releaseYear))
	record.Set("spotify_id", "")
	record.Set("is_recent", true)

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create song"})
	}

	return renderDatastar(e, templates.NewSongRow(templates.Song{
		ID:          record.Id,
		Title:       record.GetString("title"),
		ArtistName:  record.GetString("artist_name"),
		ReleaseDate: record.GetString("release_date"),
	}))
}

func parseSpotifyIDs(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		spotifyID := strings.TrimSpace(part)
		if spotifyID == "" {
			continue
		}
		if !isValidSpotifyID(spotifyID) {
			return nil, fmt.Errorf("artist_spotify_ids must contain 22-character alphanumeric values")
		}
		key := strings.ToLower(spotifyID)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, spotifyID)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("artist_spotify_ids is required")
	}
	return out, nil
}

func isValidSpotifyID(spotifyID string) bool {
	if len(spotifyID) != 22 {
		return false
	}
	for _, c := range spotifyID {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		default:
			return false
		}
	}
	return true
}

func inferArtistNameFromSpotifyID(ctx context.Context, spotifyID string) (string, int, error) {
	endpoint := "https://open.spotify.com/oembed?url=" + url.QueryEscape("spotify:artist:"+spotifyID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to infer artist name from spotify_id")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to reach spotify to infer artist name")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusNotFound {
		return "", http.StatusBadRequest, fmt.Errorf("could not infer artist name: spotify artist not found")
	}
	if resp.StatusCode != http.StatusOK {
		return "", http.StatusBadGateway, fmt.Errorf("could not infer artist name from spotify")
	}

	var payload struct {
		Title string `json:"title"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("could not infer artist name from spotify response")
	}

	artistName := strings.TrimSpace(payload.Title)
	if artistName == "" {
		return "", http.StatusBadGateway, fmt.Errorf("could not infer artist name from spotify response")
	}

	return artistName, 0, nil
}

func (h *Handler) upsertAlbumForSong(albumName, primaryArtist string) error {
	records, err := h.app.FindRecordsByFilter(
		"albums",
		"LOWER(title) = {:title} AND LOWER(artist_name) = {:artist_name}",
		"",
		1,
		0,
		dbx.Params{
			"title":       strings.ToLower(albumName),
			"artist_name": strings.ToLower(primaryArtist),
		},
	)
	if err != nil {
		return err
	}

	if len(records) > 0 {
		record := records[0]
		collectionSongs := record.GetInt("collection_songs") + 1
		totalSongs := max(record.GetInt("total_songs"), collectionSongs)
		record.Set("collection_songs", collectionSongs)
		record.Set("total_songs", totalSongs)
		return h.app.Save(record)
	}

	collection, err := h.app.FindCollectionByNameOrId("albums")
	if err != nil {
		return err
	}

	record := core.NewRecord(collection)
	record.Set("title", albumName)
	record.Set("artist_name", primaryArtist)
	record.Set("collection_songs", 1)
	record.Set("total_songs", 1)
	record.Set("status", "waiting")
	return h.app.Save(record)
}

func (h *Handler) upsertArtistsForSong(artists []string, artistSpotifyIDs []string) error {
	collection, err := h.app.FindCollectionByNameOrId("artists")
	if err != nil {
		return err
	}

	for i, artistName := range artists {
		artistSpotifyID := artistSpotifyIDs[i]

		records, findErr := h.app.FindRecordsByFilter(
			"artists",
			"spotify_id = {:spotify_id}",
			"",
			1,
			0,
			dbx.Params{"spotify_id": artistSpotifyID},
		)
		if findErr != nil {
			return findErr
		}

		if len(records) == 0 {
			records, findErr = h.app.FindRecordsByFilter(
				"artists",
				"LOWER(name) = {:name}",
				"",
				1,
				0,
				dbx.Params{"name": strings.ToLower(artistName)},
			)
			if findErr != nil {
				return findErr
			}
		}

		if len(records) > 0 {
			record := records[0]
			record.Set("collection_songs", record.GetInt("collection_songs")+1)
			if record.GetString("spotify_id") == "" {
				record.Set("spotify_id", artistSpotifyID)
			}
			if err := h.app.Save(record); err != nil {
				return err
			}
			continue
		}

		record := core.NewRecord(collection)
		record.Set("name", artistName)
		record.Set("spotify_id", artistSpotifyID)
		record.Set("monthly_listeners", 0)
		record.Set("genre_group", "rock_metal")
		record.Set("list_status", "recently_added")
		record.Set("fetch_status", "idle")
		record.Set("collection_songs", 1)
		record.Set("total_songs", 0)
		record.Set("last_updated", time.Now())

		if err := h.app.Save(record); err != nil {
			return err
		}
	}

	return nil
}

// handleArtists renders the artists view.
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
			LastUpdated:      r.GetDateTime("last_updated").Time().Format("Jan 2, 2006"),
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
		artists = append(artists, templates.Artist{
			ID:               r.Id,
			Name:             r.GetString("name"),
			SpotifyID:        r.GetString("spotify_id"),
			MonthlyListeners: r.GetInt("monthly_listeners"),
			GenreGroup:       r.GetString("genre_group"),
			ListStatus:       r.GetString("list_status"),
			FetchStatus:      r.GetString("fetch_status"),
			CollectionSongs:  r.GetInt("collection_songs"),
			TotalSongs:       r.GetInt("total_songs"),
			LastUpdated:      r.GetDateTime("last_updated").Time().Format("Jan 2, 2006"),
		})
	}

	// NOTE: WaitingArtistRows uses data-merge-mode="append" so previously shown artists remain visible.
	return renderDatastar(e, templates.WaitingArtistRows(artists, offset+len(records), hasMore))
}

// handleSongs renders the songs view.
func (h *Handler) handleSongs(e *core.RequestEvent) error {
	// Fetch recent songs
	records, err := h.app.FindRecordsByFilter(
		"songs",
		"is_recent = true",
		"-release_date",
		500,
		0,
		nil,
	)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	// Convert to type-safe structs
	songs := make([]templates.Song, 0, len(records))
	for _, r := range records {
		songs = append(songs, templates.Song{
			ID:          r.Id,
			Title:       r.GetString("title"),
			ArtistName:  r.GetString("artist_name"),
			ReleaseDate: r.GetString("release_date"),
		})
	}

	return renderTempl(e, templates.SongsPage(songs))
}

// handleQuota returns the quota status for all configured providers.
func (h *Handler) handleQuota(e *core.RequestEvent) error {
	checker := quota.NewChecker(h.cfg)
	quotas := checker.CheckAll(e.Request.Context())

	return e.JSON(http.StatusOK, map[string]any{
		"providers":     quotas,
		"has_available": checker.HasAvailableQuota(e.Request.Context()),
		"best_provider": checker.GetBestProvider(e.Request.Context()),
	})
}

func (h *Handler) handleQueue(e *core.RequestEvent) error {
	ctx, cancel := context.WithTimeout(e.Request.Context(), 3*time.Second)
	defer cancel()

	stream, err := h.js.Stream(ctx, messaging.ScrapeRequestsStreamName)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to load JetStream stream info"})
	}
	streamInfo, err := stream.Info(ctx)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to load JetStream stream info"})
	}

	consumer, err := stream.Consumer(ctx, messaging.ScrapeWorkerConsumerName)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to load JetStream consumer info"})
	}
	consumerInfo, err := consumer.Info(ctx)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to load JetStream consumer info"})
	}

	jobsQueued, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "queued"})
	jobsProcessing, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "processing"})
	jobsFailed, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "failed"})

	if !strings.Contains(e.Request.Header.Get("Accept"), "application/json") {
		signals := map[string]any{
			"queuePending":     streamInfo.State.Msgs,
			"queueAckPending":  consumerInfo.NumAckPending,
			"queueRedelivered": consumerInfo.NumRedelivered,
			"jobsQueued":       jobsQueued,
			"jobsProcessing":   jobsProcessing,
			"jobsFailed":       jobsFailed,
		}
		payload, err := json.Marshal(signals)
		if err != nil {
			return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to serialize queue signals"})
		}
		sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
		return sse.PatchSignals(payload)
	}

	return e.JSON(http.StatusOK, map[string]any{
		"stream": map[string]any{
			"name":           streamInfo.Config.Name,
			"subjects":       streamInfo.Config.Subjects,
			"retention":      streamInfo.Config.Retention,
			"storage":        streamInfo.Config.Storage,
			"duplicates":     streamInfo.Config.Duplicates.String(),
			"messages":       streamInfo.State.Msgs,
			"bytes":          streamInfo.State.Bytes,
			"first_seq":      streamInfo.State.FirstSeq,
			"last_seq":       streamInfo.State.LastSeq,
			"consumer_count": streamInfo.State.Consumers,
		},
		"consumer": map[string]any{
			"name":            consumerInfo.Name,
			"durable":         consumerInfo.Config.Durable,
			"ack_wait":        consumerInfo.Config.AckWait.String(),
			"max_deliver":     consumerInfo.Config.MaxDeliver,
			"num_pending":     consumerInfo.NumPending,
			"num_ack_pending": consumerInfo.NumAckPending,
			"num_redelivered": consumerInfo.NumRedelivered,
			"delivered":       consumerInfo.Delivered.Stream,
			"ack_floor":       consumerInfo.AckFloor.Stream,
		},
		"jobs": map[string]any{
			"queued":     jobsQueued,
			"processing": jobsProcessing,
			"failed":     jobsFailed,
		},
	})
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

	// Persist a job record for auditing and retries (best-effort).
	if jobs, err := h.app.FindCollectionByNameOrId("scrape_jobs"); err == nil {
		job := core.NewRecord(jobs)
		job.Set("request_id", requestID)
		job.Set("artist", record.Id)
		job.Set("status", "queued")
		job.Set("attempts", 0)
		job.Set("queued_at", time.Now())
		if err := h.app.Save(job); err != nil {
			log.Printf("[handlers] Warning: failed to create scrape job record: %v", err)
		}
	} else {
		log.Printf("[handlers] Warning: scrape_jobs collection not found: %v", err)
	}

	req := messaging.NewScrapeRequested(
		record.Id,
		record.GetString("spotify_id"),
		record.GetString("name"),
		requestID,
	)

	ctx, cancel := context.WithTimeout(e.Request.Context(), 5*time.Second)
	defer cancel()

	msgID := messaging.ScrapeRequestMsgID(record.Id)
	ack, err := messaging.PublishScrapeRequested(ctx, h.js, req, msgID)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to queue refresh"})
	}
	if ack != nil && ack.Duplicate {
		log.Printf("[handlers] Duplicate refresh request ignored for artist %s", record.Id)
	}

	correlation.Associate(record.Id, requestID)

	record.Set("fetch_status", "pending")
	if err := h.app.Save(record); err != nil {
		log.Printf("[handlers] Warning: failed to update fetch_status: %v", err)
	}

	if strings.Contains(e.Request.Header.Get("Accept"), "application/json") {
		return e.JSON(http.StatusOK, map[string]string{"status": "queued"})
	}

	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	payload := fmt.Sprintf(`{"artistFetchStatus":{%q:"pending"}}`, record.Id)
	return sse.PatchSignals([]byte(payload))
}

func (h *Handler) handleBatchRefresh(e *core.RequestEvent) error {
	if err := e.Request.ParseForm(); err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid form data"})
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

	oneHourAgo := time.Now().Add(-1 * time.Hour)
	records, err := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id != '' && spotify_id != null && (last_updated = '' || last_updated < {:cutoff})",
		"-monthly_listeners",
		0, 0,
		dbx.Params{"cutoff": oneHourAgo.Format("2006-01-02 15:04:05.000Z")},
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

	queued := 0
	for _, job := range jobs {
		r := job.Record
		requestID := strconv.FormatInt(time.Now().UnixNano(), 10)

		if jobsColl, err := h.app.FindCollectionByNameOrId("scrape_jobs"); err == nil {
			jobRec := core.NewRecord(jobsColl)
			jobRec.Set("request_id", requestID)
			jobRec.Set("artist", r.Id)
			jobRec.Set("status", "queued")
			jobRec.Set("attempts", 0)
			jobRec.Set("queued_at", time.Now())
			h.app.Save(jobRec)
		}

		req := messaging.NewScrapeRequested(
			r.Id,
			r.GetString("spotify_id"),
			r.GetString("name"),
			requestID,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msgID := messaging.ScrapeRequestMsgID(r.Id)
		ack, err := messaging.PublishScrapeRequested(ctx, h.js, req, msgID)
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
		batchTracker.Store(r.Id, struct{}{})
		r.Set("fetch_status", "pending")
		h.app.Save(r)
		queued++
	}

	stats := make(map[string]int)
	for _, job := range jobs[:queued] {
		stats[job.Priority.String()]++
	}

	batchCounterMu.Lock()
	batchCounter = 0
	batchCounterMu.Unlock()

	// Use no-compression SSE since this connection stays open for the entire
	// batch processing duration (potentially minutes).
	sse := datastar.NewSSE(e.Response, e.Request, sseStreamOpts...)

	// Send initial progress state (0 / total).
	if err := sse.PatchSignals(fmt.Appendf(nil, `{"batchTotal":%d,"batchCompleted":0}`, queued)); err != nil {
		return err
	}
	if err := sse.PatchElementTempl(templates.BatchRefreshResult(queued, stats)); err != nil {
		return err
	}

	if queued == 0 {
		return nil
	}

	// ── Follow the official Datastar progress-bar pattern ──
	// Keep this SSE connection open and stream progress updates as artists
	// complete, by subscribing to NATS directly from this handler.
	ctx := e.Request.Context()
	completed := int64(0)
	done := make(chan struct{})

	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			return
		}

		// Only count artists that belong to this batch.
		if _, tracked := batchTracker.Load(update.ArtistID); !tracked {
			return
		}
		if update.FetchStatus == "pending" {
			return
		}

		batchTracker.Delete(update.ArtistID)
		batchCounterMu.Lock()
		batchCounter++
		completed = batchCounter
		batchCounterMu.Unlock()

		// Push updated progress via signals.
		payload := fmt.Sprintf(`{"batchCompleted":%d}`, completed)
		if err := sse.PatchSignals([]byte(payload)); err != nil {
			log.Printf("[batch-sse] Failed to send progress: %v", err)
		}

		if completed >= int64(queued) {
			close(done)
		}
	})
	if err != nil {
		log.Printf("[batch-sse] Failed to subscribe: %v", err)
		return nil
	}
	defer sub.Unsubscribe()

	// Block until all artists are done, the client disconnects, or a
	// generous timeout expires.
	timeout := time.After(time.Duration(queued) * 2 * time.Minute)
	select {
	case <-done:
		// All done – send a final 100% patch.
		payload := fmt.Sprintf(`{"batchCompleted":%d}`, queued)
		sse.PatchSignals([]byte(payload))
	case <-ctx.Done():
		log.Printf("[batch-sse] Client disconnected")
	case <-timeout:
		log.Printf("[batch-sse] Timed out waiting for batch to complete")
	}

	return nil
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

	// Build artist struct for response
	artist := templates.Artist{
		ID:               record.Id,
		Name:             record.GetString("name"),
		SpotifyID:        record.GetString("spotify_id"),
		MonthlyListeners: record.GetInt("monthly_listeners"),
		GenreGroup:       record.GetString("genre_group"),
		ListStatus:       record.GetString("list_status"),
		FetchStatus:      record.GetString("fetch_status"),
		CollectionSongs:  record.GetInt("collection_songs"),
		TotalSongs:       record.GetInt("total_songs"),
		LastUpdated:      record.GetDateTime("last_updated").Time().Format("Jan 2, 2006"),
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

	// Return the updated artist row as SSE fragment for Datastar
	artist := templates.Artist{
		ID:               record.Id,
		Name:             record.GetString("name"),
		SpotifyID:        record.GetString("spotify_id"),
		MonthlyListeners: record.GetInt("monthly_listeners"),
		GenreGroup:       record.GetString("genre_group"),
		ListStatus:       record.GetString("list_status"),
		FetchStatus:      record.GetString("fetch_status"),
		CollectionSongs:  record.GetInt("collection_songs"),
		TotalSongs:       record.GetInt("total_songs"),
		LastUpdated:      record.GetDateTime("last_updated").Time().Format("Jan 2, 2006"),
	}

	return renderDatastar(e, templates.ArtistRow(artist))
}

// handleSSE provides Server-Sent Events for real-time updates.
func (h *Handler) handleSSE(e *core.RequestEvent) error {
	w := e.Response
	r := e.Request

	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Use sseStreamOpts (no compression) for the persistent SSE connection.
	sse := datastar.NewSSE(w, r, sseStreamOpts...)

	ctx := r.Context()

	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		// Don't attempt to write to a closed connection.
		select {
		case <-ctx.Done():
			return
		default:
		}

		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			log.Printf("[sse] Failed to unmarshal update: %v", err)
			return
		}

		// Send artist table updates (listeners, timestamps, status).
		payload := fmt.Sprintf(
			`{"artistListeners":{%q:%d},"artistUpdated":{%q:%q},"artistFetchStatus":{%q:%q}}`,
			update.ArtistID,
			update.MonthlyListeners,
			update.ArtistID,
			formatUpdatedAt(update.UpdatedAt),
			update.ArtistID,
			update.FetchStatus,
		)

		if err := sse.PatchSignals([]byte(payload)); err != nil {
			log.Printf("[sse] Failed to send signals: %v", err)
		}
	})

	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to subscribe")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("[sse] Warning: failed to unsubscribe: %v", err)
		}
	}()

	<-ctx.Done()
	return nil
}

// formatNumber formats an integer with comma separators.
func formatNumber(n int) string {
	s := strconv.Itoa(n)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// renderArtistRowFragment renders a single artist row for SSE updates.
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
