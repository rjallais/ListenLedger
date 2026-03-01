//go:build goexperiment.jsonv2

// Package handlers provides HTTP route handlers for the web application.
package handlers

import (
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

	"ListenLedger/config"
	"ListenLedger/internal/buildinfo"
	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/priority"
	"ListenLedger/internal/quota"
	"ListenLedger/templates"
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

type batchProgress struct {
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time

	Total     int
	Completed int
	Done      bool

	Stats   map[string]int
	Pending map[string]struct{}
}

type batchProgressSnapshot struct {
	ID        string
	Total     int
	Completed int
	Done      bool
	Stats     map[string]int
}

type Handler struct {
	app       *pocketbase.PocketBase
	nc        *nats.Conn
	js        jetstream.JetStream
	cfg       *config.Config
	startedAt time.Time

	batchMu      sync.RWMutex
	batches      map[string]*batchProgress
	artistBatch  map[string]string
	latestBatch  string
	batchUpdates *nats.Subscription
}

// New creates a new Handler instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Handler {
	return &Handler{
		app:       app,
		nc:        nc,
		js:        js,
		cfg:       cfg,
		startedAt: time.Now(),

		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
}

// RegisterRoutes registers all HTTP routes with the router.
func (h *Handler) RegisterRoutes(r *router.Router[*core.RequestEvent]) {
	h.ensureBatchProgressSubscriber()

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
	r.POST("/api/songs/{songId}/recent/{value}", h.handleUpdateSongRecent)
	r.GET("/api/songs/sections", h.handleSongsSectionsAPI)
	r.GET("/api/songs/current-playlist", h.handleSongsCurrentPlaylistAPI)
	r.GET("/api/songs/not-recent", h.handleSongsNotRecentAPI)
	r.POST("/api/artists/{artistId}/status/{status}", h.handleUpdateListStatus)
	r.POST("/api/artists/{artistId}/collection/{action}", h.handleUpdateCollectionSongs)
	r.GET("/api/events", h.handleSSE)
	r.GET("/api/quota", h.handleQuota)
	r.GET("/api/queue", h.handleQueue)
	r.POST("/api/queue/retry", h.handleQueueRetry)
	// PocketBase already provides GET /api/health. Keep app-specific health data on a dedicated path.
	r.GET("/api/listenledger/health", h.handleAppHealth)

	log.Println("[handlers] Routes registered")
}

func (h *Handler) ensureBatchProgressSubscriber() {
	h.batchMu.Lock()
	if h.batchUpdates != nil {
		h.batchMu.Unlock()
		return
	}
	h.batchMu.Unlock()

	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			return
		}
		h.markBatchArtistDone(update.ArtistID, update.FetchStatus)
	})
	if err != nil {
		log.Printf("[batch] Failed to subscribe to %s: %v", messaging.SubjectArtistUpdated, err)
		return
	}

	h.batchMu.Lock()
	defer h.batchMu.Unlock()
	if h.batchUpdates != nil {
		_ = sub.Unsubscribe()
		return
	}
	h.batchUpdates = sub
	log.Printf("[batch] Tracking progress from %s", messaging.SubjectArtistUpdated)
}

func (h *Handler) markBatchArtistDone(artistID, fetchStatus string) {
	if artistID == "" || fetchStatus == "pending" {
		return
	}

	h.batchMu.Lock()
	defer h.batchMu.Unlock()

	batchID, ok := h.artistBatch[artistID]
	if !ok {
		return
	}
	batch, ok := h.batches[batchID]
	if !ok {
		delete(h.artistBatch, artistID)
		return
	}
	if _, pending := batch.Pending[artistID]; !pending {
		return
	}

	delete(batch.Pending, artistID)
	delete(h.artistBatch, artistID)
	batch.Completed++
	if batch.Completed >= batch.Total {
		batch.Completed = batch.Total
		batch.Done = true
	}
	batch.UpdatedAt = time.Now()
}

func (h *Handler) createBatchProgress(artistIDs []string, stats map[string]int) batchProgressSnapshot {
	now := time.Now()
	pending := make(map[string]struct{}, len(artistIDs))
	for _, artistID := range artistIDs {
		pending[artistID] = struct{}{}
	}

	snapshotStats := make(map[string]int, len(stats))
	for key, value := range stats {
		snapshotStats[key] = value
	}

	batchID := strconv.FormatInt(now.UnixNano(), 36)
	progress := &batchProgress{
		ID:        batchID,
		CreatedAt: now,
		UpdatedAt: now,
		Total:     len(artistIDs),
		Completed: 0,
		Done:      len(artistIDs) == 0,
		Stats:     snapshotStats,
		Pending:   pending,
	}

	h.batchMu.Lock()
	defer h.batchMu.Unlock()

	h.pruneBatchStateLocked(now.Add(-2 * time.Hour))

	h.batches[batchID] = progress
	h.latestBatch = batchID
	for artistID := range pending {
		h.artistBatch[artistID] = batchID
	}

	return h.batchSnapshotLocked(progress)
}

func (h *Handler) pruneBatchStateLocked(cutoff time.Time) {
	for batchID, batch := range h.batches {
		if !batch.Done || batch.UpdatedAt.After(cutoff) {
			continue
		}
		delete(h.batches, batchID)
		if h.latestBatch == batchID {
			h.latestBatch = ""
		}
	}

	for artistID, batchID := range h.artistBatch {
		if _, ok := h.batches[batchID]; ok {
			continue
		}
		delete(h.artistBatch, artistID)
	}

	if h.latestBatch != "" {
		return
	}

	var latestID string
	var latestTime time.Time
	for batchID, batch := range h.batches {
		if batch.UpdatedAt.After(latestTime) {
			latestID = batchID
			latestTime = batch.UpdatedAt
		}
	}
	h.latestBatch = latestID
}

func (h *Handler) getBatchSnapshot(batchID string) (batchProgressSnapshot, bool) {
	if batchID == "" {
		return batchProgressSnapshot{}, false
	}

	h.batchMu.RLock()
	defer h.batchMu.RUnlock()

	batch, ok := h.batches[batchID]
	if !ok {
		return batchProgressSnapshot{}, false
	}
	return h.batchSnapshotLocked(batch), true
}

func (h *Handler) getActiveBatchSnapshot() (batchProgressSnapshot, bool) {
	h.batchMu.RLock()
	defer h.batchMu.RUnlock()

	batchID := h.latestBatch
	if batchID == "" {
		return batchProgressSnapshot{}, false
	}
	batch, ok := h.batches[batchID]
	if !ok {
		return batchProgressSnapshot{}, false
	}
	snapshot := h.batchSnapshotLocked(batch)
	if snapshot.Done {
		return batchProgressSnapshot{}, false
	}
	return snapshot, true
}

func (h *Handler) getLatestBatchSnapshot() (batchProgressSnapshot, bool) {
	h.batchMu.RLock()
	defer h.batchMu.RUnlock()

	batchID := h.latestBatch
	if batchID == "" {
		return batchProgressSnapshot{}, false
	}
	batch, ok := h.batches[batchID]
	if !ok {
		return batchProgressSnapshot{}, false
	}
	return h.batchSnapshotLocked(batch), true
}

func (h *Handler) batchSnapshotLocked(batch *batchProgress) batchProgressSnapshot {
	if batch == nil {
		return batchProgressSnapshot{}
	}

	stats := make(map[string]int, len(batch.Stats))
	for key, value := range batch.Stats {
		stats[key] = value
	}

	done := batch.Done || (batch.Total > 0 && batch.Completed >= batch.Total)

	return batchProgressSnapshot{
		ID:        batch.ID,
		Total:     batch.Total,
		Completed: batch.Completed,
		Done:      done,
		Stats:     stats,
	}
}

func (h *Handler) patchBatchRefreshState(e *core.RequestEvent, snapshot batchProgressSnapshot) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	payload := fmt.Appendf(
		nil,
		`{"batchID":%q,"batchTotal":%d,"batchCompleted":%d,"batchDone":%t}`,
		snapshot.ID,
		snapshot.Total,
		snapshot.Completed,
		snapshot.Done,
	)
	if err := sse.PatchSignals(payload); err != nil {
		return err
	}
	return sse.PatchElementTempl(
		templates.BatchRefreshResult(snapshot.ID, snapshot.Total, snapshot.Completed, snapshot.Stats, snapshot.Done),
	)
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

	return renderDatastar(e, templates.NewAlbumCreateResponse(albumFromRecord(record)))
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

	return renderDatastar(e, templates.NewArtistCreateResponse(artist))
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

	// Release type: album, ep, or single.
	releaseType := strings.TrimSpace(e.Request.FormValue("release_type"))
	if releaseType == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "release type is required"})
	}
	switch releaseType {
	case "album", "ep", "single":
		// valid
	default:
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "release_type must be album, ep, or single"})
	}

	// Full release date (YYYY-MM-DD). Extract year automatically.
	releaseDateRaw := strings.TrimSpace(e.Request.FormValue("release_date"))
	if releaseDateRaw == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "release date is required"})
	}
	parsedDate, dateErr := time.Parse("2006-01-02", releaseDateRaw)
	if dateErr != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "release_date must be in YYYY-MM-DD format"})
	}
	releaseYear := parsedDate.Year()

	// Total songs on the album/EP/single (user-provided).
	totalSongsOnAlbum := 0
	if ts := strings.TrimSpace(e.Request.FormValue("total_songs")); ts != "" {
		if parsed, parseErr := strconv.Atoi(ts); parseErr == nil && parsed > 0 {
			totalSongsOnAlbum = parsed
		}
	}

	newArtistGenre := strings.TrimSpace(e.Request.FormValue("new_artist_genre"))
	if newArtistGenre == "" {
		newArtistGenre = "rock_metal"
	}
	if !isValidGenreGroup(newArtistGenre) {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "new_artist_genre must be rock_metal or everything_else"})
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
		artistName, statusCode, inferErr := h.inferArtistNameFromSpotifyID(ctx, artistSpotifyID)
		cancel()
		if inferErr != nil {
			return e.JSON(statusCode, map[string]string{"error": inferErr.Error()})
		}
		artists = append(artists, artistName)
	}

	if err := h.upsertAlbumForSong(albumName, artists[0], releaseType, totalSongsOnAlbum); err != nil {
		log.Printf("[handleCreateSong] upsertAlbumForSong failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album metadata"})
	}
	newArtists, err := h.upsertArtistsForSong(artists, artistSpotifyIDs, newArtistGenre)
	if err != nil {
		log.Printf("[handleCreateSong] upsertArtistsForSong failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist metadata"})
	}
	for _, target := range newArtists {
		if queueErr := h.queueArtistRefreshFromSong(e.Request.Context(), target); queueErr != nil {
			log.Printf("[handleCreateSong] Warning: failed to queue refresh for new artist %s (%s): %v", target.Name, target.ID, queueErr)
		}
	}

	collection, err := h.app.FindCollectionByNameOrId("songs")
	if err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "songs collection not found"})
	}

	record := core.NewRecord(collection)
	record.Set("title", songName)
	record.Set("artist_name", strings.Join(artists, ", "))
	record.Set("album", albumName)
	record.Set("release_type", releaseType)
	record.Set("release_year", releaseYear)
	record.Set("release_date", releaseDateRaw)
	record.Set("artist_spotify_ids", strings.Join(artistSpotifyIDs, ","))
	record.Set("spotify_id", "")
	record.Set("is_recent", true)
	batchSeq, batchPos, err := h.nextRecentBatchAssignment(time.Now())
	if err != nil {
		log.Printf("[handleCreateSong] nextRecentBatchAssignment failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to assign recent batch"})
	}
	record.Set("recent_batch_seq", batchSeq)
	record.Set("recent_batch_pos", batchPos)

	if err := h.app.Save(record); err != nil {
		log.Printf("[handleCreateSong] song save failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create song"})
	}

	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, err := h.buildSongPageData(playlistSort)
	if err != nil {
		log.Printf("[handleCreateSong] buildSongPageData failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to load songs"})
	}

	return renderDatastar(e, templates.NewSongCreateResponse(
		record.GetString("title"),
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
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

func isValidGenreGroup(value string) bool {
	switch value {
	case "rock_metal", "everything_else":
		return true
	default:
		return false
	}
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

// formatReleaseDateForUI converts a stored YYYY-MM-DD date to a human-friendly
// format like "2 January 2006". If the value can't be parsed as YYYY-MM-DD,
// it's returned as-is (handles legacy formats gracefully).
func formatReleaseDateForUI(stored string) string {
	t, err := time.Parse("2006-01-02", stored)
	if err != nil {
		return stored // legacy format or plain year — pass through
	}
	return t.Format("2 January 2006")
}

func songReleaseNameFromRecord(record *core.Record) string {
	releaseName := strings.TrimSpace(record.GetString("album"))
	if releaseName != "" {
		return releaseName
	}

	// Legacy seed/import flows may omit album; fall back to title so the
	// release column remains meaningful instead of empty.
	releaseName = strings.TrimSpace(record.GetString("title"))
	if releaseName != "" {
		return releaseName
	}

	return "—"
}

func songFromRecord(record *core.Record) templates.Song {
	recentBatchSeq := record.GetInt("recent_batch_seq")
	if recentBatchSeq < 0 {
		recentBatchSeq = 0
	}
	recentBatchPos := record.GetInt("recent_batch_pos")
	if recentBatchPos < 0 {
		recentBatchPos = 0
	}

	return templates.Song{
		ID:          record.Id,
		Title:       record.GetString("title"),
		ArtistName:  record.GetString("artist_name"),
		ReleaseDate: formatReleaseDateForUI(record.GetString("release_date")),
		ReleaseType: record.GetString("release_type"),
		Album:       songReleaseNameFromRecord(record),
		IsRecent:    record.GetBool("is_recent"),
		BatchSeq:    recentBatchSeq,
		BatchPos:    recentBatchPos,
	}
}

type songListEntry struct {
	song        templates.Song
	createdAt   time.Time
	releaseDate time.Time
}

type songPageData struct {
	CurrentPlaylist []templates.Song
	WaitingRemoval  []templates.Song
	NotRecentCount  int
	PlaylistSort    string
}

func parseSongReleaseDate(stored string) time.Time {
	t, err := time.Parse("2006-01-02", stored)
	if err != nil {
		return time.Time{}
	}
	return t
}

func normalizePlaylistSort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case playlistSortReleaseAsc:
		return playlistSortReleaseAsc
	default:
		return playlistSortAddedDesc
	}
}

func (h *Handler) listSongEntries() ([]songListEntry, error) {
	records, err := h.app.FindRecordsByFilter(
		"songs",
		"",
		"",
		0,
		0,
		nil,
	)
	if err != nil {
		return nil, err
	}

	entries := make([]songListEntry, 0, len(records))
	for _, record := range records {
		entries = append(entries, songListEntry{
			song:        songFromRecord(record),
			createdAt:   record.GetDateTime("created").Time(),
			releaseDate: parseSongReleaseDate(record.GetString("release_date")),
		})
	}

	return entries, nil
}

func (h *Handler) sortRecentSongEntries(entries []songListEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		left := entries[i]
		right := entries[j]

		if left.song.BatchSeq != right.song.BatchSeq {
			return left.song.BatchSeq > right.song.BatchSeq
		}
		// Within each batch, older insertions stay longer in the playlist.
		if left.song.BatchPos != right.song.BatchPos {
			return left.song.BatchPos < right.song.BatchPos
		}
		if !left.createdAt.Equal(right.createdAt) {
			return left.createdAt.Before(right.createdAt)
		}
		if !left.releaseDate.Equal(right.releaseDate) {
			return left.releaseDate.After(right.releaseDate)
		}
		leftTitle := strings.ToLower(left.song.Title)
		rightTitle := strings.ToLower(right.song.Title)
		if leftTitle != rightTitle {
			return leftTitle < rightTitle
		}

		return left.song.ID < right.song.ID
	})
}

func (h *Handler) sortNotRecentSongEntries(entries []songListEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		left := entries[i]
		right := entries[j]

		if !left.releaseDate.Equal(right.releaseDate) {
			return left.releaseDate.After(right.releaseDate)
		}
		if !left.createdAt.Equal(right.createdAt) {
			return left.createdAt.After(right.createdAt)
		}
		leftTitle := strings.ToLower(left.song.Title)
		rightTitle := strings.ToLower(right.song.Title)
		if leftTitle != rightTitle {
			return leftTitle < rightTitle
		}
		return left.song.ID < right.song.ID
	})
}

func (h *Handler) sortCurrentPlaylistEntries(entries []songListEntry, playlistSort string) {
	switch normalizePlaylistSort(playlistSort) {
	case playlistSortReleaseAsc:
		sort.SliceStable(entries, func(i, j int) bool {
			left := entries[i]
			right := entries[j]
			if !left.releaseDate.Equal(right.releaseDate) {
				return left.releaseDate.Before(right.releaseDate)
			}
			if !left.createdAt.Equal(right.createdAt) {
				return left.createdAt.Before(right.createdAt)
			}
			leftTitle := strings.ToLower(left.song.Title)
			rightTitle := strings.ToLower(right.song.Title)
			if leftTitle != rightTitle {
				return leftTitle < rightTitle
			}
			return left.song.ID < right.song.ID
		})
	default:
		sort.SliceStable(entries, func(i, j int) bool {
			left := entries[i]
			right := entries[j]
			if left.song.BatchSeq != right.song.BatchSeq {
				return left.song.BatchSeq > right.song.BatchSeq
			}
			if left.song.BatchPos != right.song.BatchPos {
				return left.song.BatchPos > right.song.BatchPos
			}
			if !left.createdAt.Equal(right.createdAt) {
				return left.createdAt.After(right.createdAt)
			}
			return left.song.ID < right.song.ID
		})
	}
}

func (h *Handler) buildSongPageData(playlistSort string) (songPageData, error) {
	playlistSort = normalizePlaylistSort(playlistSort)

	entries, err := h.listSongEntries()
	if err != nil {
		return songPageData{}, err
	}

	recent := make([]songListEntry, 0, len(entries))
	notRecent := make([]songListEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.song.IsRecent {
			recent = append(recent, entry)
			continue
		}
		notRecent = append(notRecent, entry)
	}

	h.sortRecentSongEntries(recent)
	h.sortNotRecentSongEntries(notRecent)

	currentCount := len(recent)
	if currentCount > songsCurrentPlaylistSize {
		currentCount = songsCurrentPlaylistSize
	}

	currentPlaylistEntries := make([]songListEntry, 0, currentCount)
	waitingRemoval := make([]templates.Song, 0)

	for i, entry := range recent {
		if i < songsCurrentPlaylistSize {
			currentPlaylistEntries = append(currentPlaylistEntries, entry)
			continue
		}
		waitingRemoval = append(waitingRemoval, entry.song)
	}

	h.sortCurrentPlaylistEntries(currentPlaylistEntries, playlistSort)

	currentPlaylist := make([]templates.Song, 0, len(currentPlaylistEntries))
	for _, entry := range currentPlaylistEntries {
		currentPlaylist = append(currentPlaylist, entry.song)
	}

	return songPageData{
		CurrentPlaylist: currentPlaylist,
		WaitingRemoval:  waitingRemoval,
		NotRecentCount:  len(notRecent),
		PlaylistSort:    playlistSort,
	}, nil
}

func (h *Handler) listNotRecentSongs(offset, limit int) ([]templates.Song, int, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = songsDefaultPageSize
	}
	if limit > songsMaxPageSize {
		limit = songsMaxPageSize
	}

	entries, err := h.listSongEntries()
	if err != nil {
		return nil, 0, err
	}

	notRecent := make([]songListEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.song.IsRecent {
			continue
		}
		notRecent = append(notRecent, entry)
	}
	h.sortNotRecentSongEntries(notRecent)

	total := len(notRecent)
	if offset >= total {
		return []templates.Song{}, total, nil
	}

	end := offset + limit
	if end > total {
		end = total
	}

	page := make([]templates.Song, 0, end-offset)
	for _, entry := range notRecent[offset:end] {
		page = append(page, entry.song)
	}

	return page, total, nil
}

func (h *Handler) nextRecentBatchAssignment(now time.Time) (int, int, error) {
	entries, err := h.listSongEntries()
	if err != nil {
		return 0, 0, err
	}

	maxSeq := 0
	maxSeqCount := 0
	maxSeqMaxPos := 0
	latestInMaxSeq := time.Time{}

	for _, entry := range entries {
		if !entry.song.IsRecent {
			continue
		}

		seq := entry.song.BatchSeq
		if seq <= 0 {
			seq = 1
		}

		if seq > maxSeq {
			maxSeq = seq
			maxSeqCount = 1
			maxSeqMaxPos = max(1, entry.song.BatchPos)
			latestInMaxSeq = entry.createdAt
			continue
		}

		if seq == maxSeq {
			maxSeqCount++
			if entry.song.BatchPos > maxSeqMaxPos {
				maxSeqMaxPos = entry.song.BatchPos
			}
			if entry.createdAt.After(latestInMaxSeq) {
				latestInMaxSeq = entry.createdAt
			}
		}
	}

	if maxSeq == 0 {
		return 1, 1, nil
	}
	if maxSeqCount >= songsRecentBatchSize || maxSeqMaxPos >= songsRecentBatchSize {
		return maxSeq + 1, 1, nil
	}
	if !latestInMaxSeq.IsZero() && now.Sub(latestInMaxSeq) >= songsRecentBatchWindow {
		return maxSeq + 1, 1, nil
	}

	nextPos := maxSeqMaxPos + 1
	if nextPos > songsRecentBatchSize {
		return maxSeq + 1, 1, nil
	}

	return maxSeq, nextPos, nil
}

// inferArtistNameFromSpotifyID resolves an artist name from a Spotify ID.
// It checks PocketBase first (avoiding a network call if the artist already exists),
// then falls back to the Spotify oEmbed API.
func (h *Handler) inferArtistNameFromSpotifyID(ctx context.Context, spotifyID string) (string, int, error) {
	// Step 1: Check PocketBase for an existing artist with this spotify_id.
	records, findErr := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id = {:spotify_id}",
		"",
		1,
		0,
		dbx.Params{"spotify_id": spotifyID},
	)
	if findErr == nil && len(records) > 0 {
		name := records[0].GetString("name")
		if name != "" {
			log.Printf("[handleCreateSong] resolved artist %q from PocketBase (spotify_id=%s)", name, spotifyID)
			return name, 0, nil
		}
	}

	// Step 2: Fall back to Spotify oEmbed API.
	endpoint := "https://open.spotify.com/oembed?url=" + url.QueryEscape("spotify:artist:"+spotifyID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to infer artist name from spotify_id")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to reach spotify to infer artist name")
	}
	defer func() { _ = resp.Body.Close() }()

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

func (h *Handler) upsertAlbumForSong(albumName, primaryArtist, releaseType string, totalSongsFromUI int) error {
	records, err := h.app.FindRecordsByFilter(
		"albums",
		"title ~ {:title} && artist_name ~ {:artist_name}",
		"",
		1,
		0,
		dbx.Params{
			"title":       albumName,
			"artist_name": primaryArtist,
		},
	)
	if err != nil {
		return err
	}

	if len(records) > 0 {
		record := records[0]
		collectionSongs := record.GetInt("collection_songs") + 1
		record.Set("collection_songs", collectionSongs)

		// Update total_songs if user-provided value is larger.
		existingTotal := record.GetInt("total_songs")
		if totalSongsFromUI > existingTotal {
			record.Set("total_songs", totalSongsFromUI)
		} else if collectionSongs > existingTotal {
			record.Set("total_songs", collectionSongs)
		}

		// Set release_type if not already set.
		if record.GetString("release_type") == "" && releaseType != "" {
			record.Set("release_type", releaseType)
		}

		return h.app.Save(record)
	}

	collection, err := h.app.FindCollectionByNameOrId("albums")
	if err != nil {
		return err
	}

	newTotal := totalSongsFromUI
	if newTotal < 1 {
		newTotal = 1
	}

	record := core.NewRecord(collection)
	record.Set("title", albumName)
	record.Set("artist_name", primaryArtist)
	record.Set("collection_songs", 1)
	record.Set("total_songs", newTotal)
	record.Set("release_type", releaseType)
	record.Set("status", "waiting")
	return h.app.Save(record)
}

type songNewArtistTarget struct {
	ID        string
	Name      string
	SpotifyID string
}

func (h *Handler) upsertArtistsForSong(artists []string, artistSpotifyIDs []string, newArtistGenre string) ([]songNewArtistTarget, error) {
	collection, err := h.app.FindCollectionByNameOrId("artists")
	if err != nil {
		return nil, err
	}

	newArtists := make([]songNewArtistTarget, 0, len(artists))
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
			return nil, findErr
		}

		if len(records) == 0 {
			records, findErr = h.app.FindRecordsByFilter(
				"artists",
				"name ~ {:name}",
				"",
				1,
				0,
				dbx.Params{"name": artistName},
			)
			if findErr != nil {
				return nil, findErr
			}
		}

		if len(records) > 0 {
			record := records[0]
			record.Set("collection_songs", record.GetInt("collection_songs")+1)
			if record.GetString("spotify_id") == "" {
				record.Set("spotify_id", artistSpotifyID)
			}
			if err := h.app.Save(record); err != nil {
				return nil, err
			}
			continue
		}

		record := core.NewRecord(collection)
		record.Set("name", artistName)
		record.Set("spotify_id", artistSpotifyID)
		record.Set("monthly_listeners", 0)
		record.Set("genre_group", newArtistGenre)
		record.Set("list_status", "not_added")
		record.Set("fetch_status", "idle")
		record.Set("collection_songs", 1)
		record.Set("total_songs", 0)
		record.Set("last_updated", time.Now())

		if err := h.app.Save(record); err != nil {
			return nil, err
		}
		newArtists = append(newArtists, songNewArtistTarget{
			ID:        record.Id,
			Name:      artistName,
			SpotifyID: artistSpotifyID,
		})
	}

	return newArtists, nil
}

func (h *Handler) queueArtistRefreshFromSong(ctx context.Context, target songNewArtistTarget) error {
	if target.ID == "" || target.SpotifyID == "" {
		return nil
	}

	requestID := strconv.FormatInt(time.Now().UnixNano(), 10)
	req := messaging.NewScrapeRequested(
		target.ID,
		target.SpotifyID,
		target.Name,
		requestID,
	)

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ack, err := h.publishScrapeRequest(pubCtx, req)
	if err != nil {
		return err
	}
	if ack != nil && ack.Duplicate {
		return nil
	}

	correlation.Associate(target.ID, requestID)
	h.createScrapeJobRecord(requestID, target.ID)

	record, err := h.app.FindRecordById("artists", target.ID)
	if err != nil {
		return err
	}
	record.Set("fetch_status", "pending")
	if err := h.app.Save(record); err != nil {
		return err
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
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, err := h.buildSongPageData(playlistSort)
	if err != nil {
		log.Printf("[handleSongs] buildSongPageData failed: %v", err)
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	return renderTempl(e, templates.SongsPage(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleUpdateSongRecent(e *core.RequestEvent) error {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))

	songID := strings.TrimSpace(e.Request.PathValue("songId"))
	if songID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "song ID required"})
	}

	value := strings.ToLower(strings.TrimSpace(e.Request.PathValue("value")))
	var isRecent bool
	switch value {
	case "true", "1", "yes", "on":
		isRecent = true
	case "false", "0", "no", "off":
		isRecent = false
	default:
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "value must be true or false"})
	}

	record, err := h.app.FindRecordById("songs", songID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "song not found"})
	}

	oldRecent := record.GetBool("is_recent")
	record.Set("is_recent", isRecent)
	if isRecent {
		if !oldRecent || record.GetInt("recent_batch_seq") <= 0 || record.GetInt("recent_batch_pos") <= 0 {
			batchSeq, batchPos, batchErr := h.nextRecentBatchAssignment(time.Now())
			if batchErr != nil {
				log.Printf("[handleUpdateSongRecent] nextRecentBatchAssignment failed: %v", batchErr)
				return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to assign recent batch"})
			}
			record.Set("recent_batch_seq", batchSeq)
			record.Set("recent_batch_pos", batchPos)
		}
	} else {
		record.Set("recent_batch_seq", 0)
		record.Set("recent_batch_pos", 0)
	}

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update song"})
	}

	pageData, err := h.buildSongPageData(playlistSort)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	return renderDatastar(e, templates.SongsSections(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleSongsCurrentPlaylistAPI(e *core.RequestEvent) error {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, err := h.buildSongPageData(playlistSort)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	return renderDatastar(e, templates.CurrentPlaylistSection(
		pageData.CurrentPlaylist,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleSongsSectionsAPI(e *core.RequestEvent) error {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, err := h.buildSongPageData(playlistSort)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	return renderDatastar(e, templates.SongsSections(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleSongsNotRecentAPI(e *core.RequestEvent) error {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))

	offset := 0
	if raw := e.Request.URL.Query().Get("offset"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	limit := songsDefaultPageSize
	if raw := e.Request.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= songsMaxPageSize {
			limit = parsed
		}
	}

	songs, totalCount, err := h.listNotRecentSongs(offset, limit)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	nextOffset := offset + len(songs)
	hasMore := nextOffset < totalCount

	return renderDatastar(e, templates.NotRecentSongRows(songs, nextOffset, hasMore, playlistSort))
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

func wantsJSONResponse(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/json") &&
		!strings.Contains(r.Header.Get("Accept"), "text/event-stream") &&
		r.Header.Get("Datastar-Request") == "" &&
		r.Header.Get("X-Datastar-Request") == ""
}

func (h *Handler) publishScrapeRequest(ctx context.Context, req messaging.ScrapeRequested) (*jetstream.PubAck, error) {
	msgID := messaging.ScrapeRequestMsgID(req.ArtistID)

	ack, err := messaging.PublishScrapeRequested(ctx, h.js, req, msgID)
	if err != nil {
		return nil, fmt.Errorf("failed to publish scrape request: %w", err)
	}
	return ack, nil
}

func (h *Handler) createScrapeJobRecord(requestID, artistID string) {
	if requestID == "" || artistID == "" {
		return
	}
	jobs, err := h.app.FindCollectionByNameOrId("scrape_jobs")
	if err != nil {
		log.Printf("[handlers] Warning: scrape_jobs collection not found: %v", err)
		return
	}

	job := core.NewRecord(jobs)
	job.Set("request_id", requestID)
	job.Set("artist", artistID)
	job.Set("status", "queued")
	job.Set("attempts", 0)
	job.Set("queued_at", time.Now())
	job.Set("error", "")
	job.Set("started_at", nil)
	job.Set("finished_at", nil)
	if err := h.app.Save(job); err != nil {
		log.Printf("[handlers] Warning: failed to create scrape job record: %v", err)
	}
}

type queueRetryStats struct {
	Candidates     int `json:"candidates"`
	Retried        int `json:"retried"`
	Duplicate      int `json:"duplicate"`
	PendingSkipped int `json:"pending_skipped"`
	PublishFailed  int `json:"publish_failed"`
	InvalidArtist  int `json:"invalid_artist"`
}

func (h *Handler) scrapeJobsByStatus(status string, limit int) ([]*core.Record, error) {
	if strings.TrimSpace(status) == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 250
	}
	records, err := h.app.FindRecordsByFilter(
		"scrape_jobs",
		"status = {:status}",
		"-queued_at",
		limit,
		0,
		dbx.Params{"status": status},
	)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (h *Handler) retryFailedAndQueuedJobs(ctx context.Context, limit int) (queueRetryStats, error) {
	if limit <= 0 {
		limit = 250
	}

	failedRecords, err := h.scrapeJobsByStatus("failed", limit)
	if err != nil {
		return queueRetryStats{}, fmt.Errorf("query failed jobs: %w", err)
	}

	remaining := limit - len(failedRecords)
	if remaining < 0 {
		remaining = 0
	}

	queuedRecords, err := h.scrapeJobsByStatus("queued", remaining)
	if err != nil {
		return queueRetryStats{}, fmt.Errorf("query queued jobs: %w", err)
	}

	records := make([]*core.Record, 0, len(failedRecords)+len(queuedRecords))
	records = append(records, failedRecords...)
	records = append(records, queuedRecords...)

	stats := queueRetryStats{Candidates: len(records)}
	seenArtist := make(map[string]struct{}, len(records))

	for _, job := range records {
		artistID := strings.TrimSpace(job.GetString("artist"))
		if artistID == "" {
			stats.InvalidArtist++
			continue
		}
		if _, seen := seenArtist[artistID]; seen {
			continue
		}
		seenArtist[artistID] = struct{}{}

		artist, findErr := h.app.FindRecordById("artists", artistID)
		if findErr != nil || artist == nil {
			stats.InvalidArtist++
			continue
		}
		if artist.GetString("fetch_status") == "pending" {
			stats.PendingSkipped++
			continue
		}

		spotifyID := strings.TrimSpace(artist.GetString("spotify_id"))
		if spotifyID == "" {
			stats.InvalidArtist++
			continue
		}

		requestID := strings.TrimSpace(job.GetString("request_id"))
		if requestID == "" {
			requestID = strconv.FormatInt(time.Now().UnixNano(), 10)
			job.Set("request_id", requestID)
		}

		req := messaging.NewScrapeRequested(
			artistID,
			spotifyID,
			artist.GetString("name"),
			requestID,
		)

		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ack, pubErr := h.publishScrapeRequest(pubCtx, req)
		cancel()
		if pubErr != nil {
			stats.PublishFailed++
			job.Set("status", "failed")
			job.Set("error", fmt.Sprintf("retry publish failed: %v", pubErr))
			job.Set("finished_at", time.Now())
			if saveErr := h.app.Save(job); saveErr != nil {
				log.Printf("[queue-retry] Warning: failed to save publish error for job %s: %v", job.Id, saveErr)
			}
			continue
		}

		if ack != nil && ack.Duplicate {
			stats.Duplicate++
			job.Set("status", "succeeded")
			job.Set("error", "deduped_existing_request")
			job.Set("finished_at", time.Now())
			if saveErr := h.app.Save(job); saveErr != nil {
				log.Printf("[queue-retry] Warning: failed to save deduped status for job %s: %v", job.Id, saveErr)
			}
			continue
		}

		job.Set("status", "queued")
		job.Set("queued_at", time.Now())
		job.Set("error", "")
		job.Set("started_at", nil)
		job.Set("finished_at", nil)
		if saveErr := h.app.Save(job); saveErr != nil {
			log.Printf("[queue-retry] Warning: failed to save queued status for job %s: %v", job.Id, saveErr)
		}

		correlation.Associate(artistID, requestID)
		artist.Set("fetch_status", "pending")
		if saveErr := h.app.Save(artist); saveErr != nil {
			log.Printf("[queue-retry] Warning: failed to mark artist %s pending: %v", artistID, saveErr)
		}
		stats.Retried++
	}

	return stats, nil
}

func (h *Handler) handleQueue(e *core.RequestEvent) error {
	h.ensureBatchProgressSubscriber()

	ctx, cancel := context.WithTimeout(e.Request.Context(), 3*time.Second)
	defer cancel()

	var (
		streamInfo        *jetstream.StreamInfo
		consumerAvailable bool
		queueAckPending   uint64
		queueRedelivered  uint64
	)
	stream, err := h.js.Stream(ctx, messaging.ScrapeRequestsStreamName)
	if err != nil {
		log.Printf("[queue] Warning: failed to load stream handle: %v", err)
	} else {
		if info, infoErr := stream.Info(ctx); infoErr != nil {
			log.Printf("[queue] Warning: failed to load stream info: %v", infoErr)
		} else {
			streamInfo = info
		}

		for _, consumerName := range messaging.ScrapeWorkerConsumerNames() {
			consumer, consumerErr := stream.Consumer(ctx, consumerName)
			if consumerErr != nil {
				continue
			}
			if info, infoErr := consumer.Info(ctx); infoErr != nil {
				log.Printf("[queue] Warning: failed to load consumer info for %s: %v", consumerName, infoErr)
			} else {
				consumerAvailable = true
				queueAckPending += uint64(info.NumAckPending)
				queueRedelivered += uint64(info.NumRedelivered)
			}
		}
	}

	jobsQueued, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "queued"})
	jobsProcessing, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "processing"})
	jobsFailed, _ := h.app.CountRecords("scrape_jobs", dbx.HashExp{"status": "failed"})
	artistsPending, _ := h.app.CountRecords("artists", dbx.HashExp{"fetch_status": "pending"})

	activeBatchRemaining := int64(0)
	if snapshot, ok := h.getActiveBatchSnapshot(); ok {
		remaining := snapshot.Total - snapshot.Completed
		if remaining > 0 {
			activeBatchRemaining = int64(remaining)
		}
	}

	if artistsPending < activeBatchRemaining {
		artistsPending = activeBatchRemaining
	}
	if jobsProcessing < artistsPending {
		jobsProcessing = artistsPending
	}

	queuePending := uint64(0)
	if streamInfo != nil {
		queuePending = streamInfo.State.Msgs
	}

	if queuePending < uint64(jobsQueued) {
		queuePending = uint64(jobsQueued)
	}
	if queueAckPending < uint64(artistsPending) {
		queueAckPending = uint64(artistsPending)
	}

	wantsJSON := wantsJSONResponse(e.Request)

	if !wantsJSON {
		signals := map[string]any{
			"queuePending":     queuePending,
			"queueAckPending":  queueAckPending,
			"queueRedelivered": queueRedelivered,
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
			"available": streamInfo != nil,
			"messages":  queuePending,
		},
		"consumer": map[string]any{
			"available":       consumerAvailable,
			"num_ack_pending": queueAckPending,
			"num_redelivered": queueRedelivered,
		},
		"jobs": map[string]any{
			"queued":     jobsQueued,
			"processing": jobsProcessing,
			"failed":     jobsFailed,
		},
		"artists_pending": artistsPending,
	})
}

func (h *Handler) handleQueueRetry(e *core.RequestEvent) error {
	checker := quota.NewChecker(h.cfg)
	if !checker.HasAvailableQuota(e.Request.Context()) {
		return e.JSON(http.StatusTooManyRequests, map[string]string{
			"error": "No scraping quota available.",
		})
	}

	stats, err := h.retryFailedAndQueuedJobs(e.Request.Context(), 250)
	if err != nil {
		log.Printf("[queue-retry] retry loop failed: %v", err)
		if wantsJSONResponse(e.Request) {
			return e.JSON(http.StatusOK, map[string]any{
				"status": "error",
				"error":  err.Error(),
			})
		}
		return h.handleQueue(e)
	}

	log.Printf(
		"[queue-retry] candidates=%d retried=%d duplicate=%d pending_skipped=%d publish_failed=%d invalid_artist=%d",
		stats.Candidates,
		stats.Retried,
		stats.Duplicate,
		stats.PendingSkipped,
		stats.PublishFailed,
		stats.InvalidArtist,
	)

	if wantsJSONResponse(e.Request) {
		return e.JSON(http.StatusOK, map[string]any{
			"status": "ok",
			"stats":  stats,
		})
	}

	return h.handleQueue(e)
}

// handleAppHealth returns a lightweight JSON health check with app name and uptime.
func (h *Handler) handleAppHealth(e *core.RequestEvent) error {
	uptime := time.Since(h.startedAt)
	return e.JSON(http.StatusOK, map[string]any{
		"status":     "ok",
		"app":        "ListenLedger",
		"version":    buildinfo.Version,
		"uptime_s":   int(uptime.Seconds()),
		"started_at": h.startedAt.UTC().Format(time.RFC3339),
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
		if strings.Contains(e.Request.Header.Get("Accept"), "application/json") {
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

	if strings.Contains(e.Request.Header.Get("Accept"), "application/json") {
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
	records, err := h.app.FindRecordsByFilter(
		"artists",
		"spotify_id != '' && spotify_id != null && (last_updated = '' || last_updated < {:cutoff})",
		"-monthly_listeners",
		0, 0,
		dbx.Params{"cutoff": fourHoursAgo.Format("2006-01-02 15:04:05.000Z")},
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	h.ensureBatchProgressSubscriber()

	w := e.Response
	r := e.Request

	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Use sseStreamOpts (no compression) for the persistent SSE connection.
	sse := datastar.NewSSE(w, r, sseStreamOpts...)

	if snapshot, ok := h.getLatestBatchSnapshot(); ok {
		payload := fmt.Appendf(
			nil,
			`{"batchID":%q,"batchTotal":%d,"batchCompleted":%d,"batchDone":%t}`,
			snapshot.ID,
			snapshot.Total,
			snapshot.Completed,
			snapshot.Done,
		)
		if err := sse.PatchSignals(payload); err != nil {
			return err
		}
	}

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
		signals := map[string]any{
			"artistListeners": map[string]int{
				update.ArtistID: update.MonthlyListeners,
			},
			"artistUpdated": map[string]string{
				update.ArtistID: formatUpdatedAt(update.UpdatedAt),
			},
			"artistFetchStatus": map[string]string{
				update.ArtistID: update.FetchStatus,
			},
		}

		if snapshot, ok := h.getLatestBatchSnapshot(); ok {
			signals["batchID"] = snapshot.ID
			signals["batchTotal"] = snapshot.Total
			signals["batchCompleted"] = snapshot.Completed
			signals["batchDone"] = snapshot.Done
		}

		payload, err := json.Marshal(signals)
		if err != nil {
			log.Printf("[sse] Failed to marshal signals: %v", err)
			return
		}

		if err := sse.PatchSignals(payload); err != nil {
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
