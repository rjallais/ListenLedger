//go:build goexperiment.jsonv2

package handlers

import (
	"net/http"
	"os"
	"strconv"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/templates"
)

// handleStatic serves static files from the static directory.
func (h *Handler) handleStatic(e *core.RequestEvent) error {
	path := e.Request.PathValue("path")
	return e.FileFS(os.DirFS(h.staticDir), path)
}

// handleIndex redirects to albums view.
func (h *Handler) handleIndex(e *core.RequestEvent) error {
	return e.Redirect(http.StatusFound, "/albums")
}

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
