//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/templates"
)

const (
	defaultAlbumPageSize     = 50
	maxAlbumPageSize         = 100
	defaultWaitingAlbumLimit = 1
)

type albumListParams struct {
	offset int
	limit  int
}

type albumFilterConfig struct {
	filter dbx.Expression
	order  string
}

type albumCreateInput struct {
	title           string
	artistName      string
	statusDB        string
	collectionSongs int
	totalSongs      int
}

func (h *Handler) handleStatic(e *core.RequestEvent) error {
	path := e.Request.PathValue("path")
	return e.FileFS(os.DirFS(h.staticDir), path)
}

func (h *Handler) handleIndex(e *core.RequestEvent) error {
	return e.Redirect(http.StatusFound, "/albums")
}

func (h *Handler) handleAlbums(e *core.RequestEvent) error {
	ctx := e.Request.Context()

	type albumStatusCount struct {
		Status string `db:"status"`
		Count  int    `db:"cnt"`
	}
	var rows []albumStatusCount
	if err := h.app.RecordQuery("albums").
		WithContext(ctx).
		Select("status", "COUNT(*) AS cnt").
		GroupBy("status").
		All(&rows); err != nil {
		log.Printf("[albums] count by status: %v", err)
		return e.String(http.StatusInternalServerError, "Failed to load albums")
	}
	var fullCount, processedCount, waitingCount int
	for _, r := range rows {
		switch r.Status {
		case "full":
			fullCount = r.Count
		case "processed_once":
			processedCount = r.Count
		default:
			waitingCount += r.Count
		}
	}

	return renderTempl(e, templates.AlbumsPage(fullCount, processedCount, waitingCount))
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
		log.Printf("[albums] unexpected album status %q; falling back to waiting", status)
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

func parseAlbumListParams(r *http.Request, status string) albumListParams {
	params := albumListParams{
		offset: parseNonNegativeInt(r.URL.Query().Get("offset")),
		limit:  parseBoundedPositiveInt(r.URL.Query().Get("limit"), defaultAlbumPageSize, maxAlbumPageSize),
	}
	if status == templates.StatusWaiting && r.URL.Query().Get("limit") == "" {
		params.limit = defaultWaitingAlbumLimit
	}
	return params
}

func albumFilterConfigForStatus(status string) (albumFilterConfig, error) {
	switch status {
	case "full":
		return albumFilterConfig{
			filter: dbx.HashExp{"status": "full"},
			order:  "`total_songs` DESC, LOWER(`title`) ASC",
		}, nil
	case "processed":
		return albumFilterConfig{
			filter: dbx.HashExp{"status": "processed_once"},
			order: "(`total_songs` - `collection_songs`) DESC, " +
				"CASE WHEN `total_songs` > 0 THEN CAST(`collection_songs` AS REAL) / `total_songs` ELSE 0 END DESC, " +
				"LOWER(`title`) ASC",
		}, nil
	case "waiting":
		return albumFilterConfig{
			filter: dbx.NewExp(
				"status != {:full} AND status != {:processed}",
				dbx.Params{"full": "full", "processed": "processed_once"},
			),
			order: "CASE WHEN `total_songs` > 0 THEN CAST(`collection_songs` AS REAL) / `total_songs` ELSE 0 END DESC, " +
				"`collection_songs` DESC, LOWER(`title`) ASC",
		}, nil
	default:
		return albumFilterConfig{}, fmt.Errorf("invalid status: %s", status)
	}
}

func (h *Handler) fetchAlbumRecords(ctx context.Context, cfg albumFilterConfig, offset, limit int) ([]*core.Record, int, error) {
	var totalCount int
	err := h.app.RecordQuery("albums").
		WithContext(ctx).
		Select("COUNT(*)").
		AndWhere(cfg.filter).
		Row(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("counting albums: %w", err)
	}

	records := make([]*core.Record, 0, limit)
	if totalCount > 0 {
		if err := h.app.RecordQuery("albums").
			WithContext(ctx).
			AndWhere(cfg.filter).
			OrderBy(cfg.order).
			Offset(int64(offset)).
			Limit(int64(limit)).
			All(&records); err != nil {
			return nil, 0, fmt.Errorf("querying albums: %w", err)
		}
	}

	return records, totalCount, nil
}

func albumsFromRecords(records []*core.Record) []templates.Album {
	albums := make([]templates.Album, 0, len(records))
	for _, r := range records {
		albums = append(albums, albumFromRecord(r))
	}
	return albums
}

func renderAlbumResponse(e *core.RequestEvent, album templates.Album) error {
	if album.Status == templates.StatusWaiting {
		return renderDatastar(e, templates.AlbumCard(album))
	}
	return renderDatastar(e, templates.AlbumRow(album))
}

func (h *Handler) handleAlbumsAPI(e *core.RequestEvent) error {
	status := e.Request.PathValue("status")

	cfg, err := albumFilterConfigForStatus(status)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status"})
	}

	params := parseAlbumListParams(e.Request, status)

	records, totalCount, err := h.fetchAlbumRecords(e.Request.Context(), cfg, params.offset, params.limit)
	if err != nil {
		log.Printf("[albums] handleAlbumsAPI fetch error (status=%q): %v", status, err)
		return e.String(http.StatusInternalServerError, "Failed to load albums")
	}

	hasMore := params.offset+len(records) < totalCount
	return renderDatastar(e, templates.AlbumRows(albumsFromRecords(records), status, params.offset+len(records), hasMore))
}

func parseAlbumCreateInput(r *http.Request) (albumCreateInput, error) {
	if err := r.ParseForm(); err != nil {
		return albumCreateInput{}, fmt.Errorf("invalid form data: %w", err)
	}

	title := strings.TrimSpace(r.FormValue("title"))
	if title == "" {
		return albumCreateInput{}, fmt.Errorf("album title is required")
	}

	artistName := strings.TrimSpace(r.FormValue("artist_name"))
	if artistName == "" {
		return albumCreateInput{}, fmt.Errorf("artist name is required")
	}

	statusValue := strings.TrimSpace(r.FormValue("status"))
	if statusValue == "" {
		statusValue = "waiting"
	}
	statusDB, ok := albumStatusForDB(statusValue)
	if !ok {
		return albumCreateInput{}, fmt.Errorf("invalid status value")
	}

	collectionSongs := parseNonNegativeInt(r.FormValue("collection_songs"))
	totalSongs := parseNonNegativeInt(r.FormValue("total_songs"))
	if totalSongs > 0 && collectionSongs > totalSongs {
		totalSongs = collectionSongs
	}

	return albumCreateInput{
		title:           title,
		artistName:      artistName,
		statusDB:        statusDB,
		collectionSongs: collectionSongs,
		totalSongs:      totalSongs,
	}, nil
}

func (h *Handler) handleCreateAlbum(e *core.RequestEvent) error {
	input, err := parseAlbumCreateInput(e.Request)
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	collection, err := h.app.FindCollectionByNameOrId("albums")
	if err != nil {
		log.Printf("[albums] handleCreateAlbum FindCollectionByNameOrId error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "albums collection not found"})
	}

	record := core.NewRecord(collection)
	record.Set("title", input.title)
	record.Set("artist_name", input.artistName)
	record.Set("status", input.statusDB)
	record.Set("collection_songs", input.collectionSongs)
	record.Set("total_songs", input.totalSongs)

	if err := h.app.Save(record); err != nil {
		log.Printf("[albums] handleCreateAlbum Save error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create album"})
	}

	return renderDatastar(e, templates.NewAlbumCreateResponse(albumFromRecord(record)))
}

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

	record, oldStatus, err := h.atomicUpdateAlbumStatus(e.Request.Context(), albumID, statusDB)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
		}
		log.Printf("[albums] atomicUpdateAlbumStatus error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
	}

	album := albumFromRecord(record)
	if oldStatus == album.Status {
		return renderAlbumResponse(e, album)
	}

	return renderDatastar(e, templates.AlbumStatusTransition(oldStatus, album))
}

func (h *Handler) atomicUpdateAlbumStatus(ctx context.Context, albumID string, statusDB string) (*core.Record, string, error) {
	var record *core.Record
	var oldStatus string
	err := h.app.RunInTransaction(func(txApp core.App) error {
		var txErr error
		record, txErr = txApp.FindRecordById("albums", albumID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if txErr != nil {
			return txErr
		}

		oldStatus = albumStatusForUI(record.GetString("status"))
		record.Set("status", statusDB)

		return txApp.Save(record)
	})
	if err != nil {
		return nil, "", err
	}
	return record, oldStatus, nil
}

func applyAlbumSongDelta(record *core.Record, field string, delta int) {
	current := record.GetInt(field)
	next := current + delta
	if next < 0 {
		next = 0
	}
	record.Set(field, next)
}

type albumSongAdjuster func(record *core.Record, delta int)

func clampAlbumCollectionSongs(record *core.Record, delta int) {
	applyAlbumSongDelta(record, "collection_songs", delta)

	total := record.GetInt("total_songs")
	collection := record.GetInt("collection_songs")
	if total > 0 && collection > total {
		record.Set("total_songs", collection)
	}
}

func clampAlbumTotalSongs(record *core.Record, delta int) {
	applyAlbumSongDelta(record, "total_songs", delta)

	total := record.GetInt("total_songs")
	collection := record.GetInt("collection_songs")
	if total < collection {
		record.Set("collection_songs", total)
	}
}

func (h *Handler) handleUpdateAlbumSongField(adjust albumSongAdjuster) func(*core.RequestEvent) error {
	return func(e *core.RequestEvent) error {
		albumID := e.Request.PathValue("albumId")
		if albumID == "" {
			return e.JSON(http.StatusBadRequest, map[string]string{"error": "album ID required"})
		}

		delta, err := parseSongCountAction(e.Request.PathValue("action"))
		if err != nil {
			return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		record, err := h.atomicUpdateAlbumSongField(e.Request.Context(), albumID, adjust, delta)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
			}
			log.Printf("[albums] atomicUpdateAlbumSongField error: %v", err)
			return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
		}

		return renderAlbumResponse(e, albumFromRecord(record))
	}
}

func (h *Handler) atomicUpdateAlbumSongField(ctx context.Context, albumID string, adjust albumSongAdjuster, delta int) (*core.Record, error) {
	var record *core.Record
	err := h.app.RunInTransaction(func(txApp core.App) error {
		var txErr error
		record, txErr = txApp.FindRecordById("albums", albumID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if txErr != nil {
			return txErr
		}

		adjust(record, delta)

		return txApp.Save(record)
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}
