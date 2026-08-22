//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/a-h/templ"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/config"
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

func (h *Handler) handleIndex(e *core.RequestEvent) error {
	return e.Redirect(http.StatusFound, "/albums")
}

func (h *Handler) handleRobots(e *core.RequestEvent) error {
	e.Response.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err := e.String(http.StatusOK, "User-agent: *\nAllow: /\n"); err != nil {
		return fmt.Errorf("write robots.txt response: %w", err)
	}
	return nil
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

func renderAlbumResponse(e *core.RequestEvent, cfg *config.Config, album templates.Album) error {
	if album.Status == templates.StatusWaiting {
		return renderDatastar(e, templates.AlbumCard(album),
			patchOpts(cfg, "#"+templates.AlbumCardID(album.ID), datastar.WithSelectorID(templates.AlbumCardID(album.ID)), datastar.WithModeOuter())...)
	}
	return renderDatastar(e, templates.AlbumRow(album),
		patchOpts(cfg, "#"+templates.AlbumRowID(album.ID), datastar.WithSelectorID(templates.AlbumRowID(album.ID)), datastar.WithModeOuter())...)
}

func patchAlbums(sse *datastar.ServerSentEventGenerator, targetID string, albums []templates.Album, isWaiting bool, cfg *config.Config) error {
	for _, album := range albums {
		var component templ.Component
		if isWaiting {
			component = templates.AlbumCard(album)
		} else {
			component = templates.AlbumRow(album)
		}
		if err := sse.PatchElementTempl(
			component,
			patchOpts(cfg, "#"+targetID, datastar.WithSelectorID(targetID), datastar.WithModeAppend())...,
		); err != nil {
			return fmt.Errorf("patch album %s into %s: %w", album.ID, targetID, err)
		}
	}
	return nil
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

	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	targetID := "albums-" + status

	// Append each album card/row inside the target container using helper
	if err := patchAlbums(sse, targetID, albumsFromRecords(records), status == templates.StatusWaiting, h.cfg); err != nil {
		return err
	}

	// Morph/replace the load-more control container
	return sse.PatchElementTempl(templates.AlbumLoadMore(status, params.offset+len(records), hasMore))
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

	collectionSongs, err := parseOptionalNonNegativeFormInt(r.FormValue("collection_songs"))
	if err != nil {
		return albumCreateInput{}, fmt.Errorf("invalid collection_songs: %w", err)
	}
	totalSongs, err := parseOptionalNonNegativeFormInt(r.FormValue("total_songs"))
	if err != nil {
		return albumCreateInput{}, fmt.Errorf("invalid total_songs: %w", err)
	}
	if collectionSongs > totalSongs {
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

func parseOptionalNonNegativeFormInt(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("must be a non-negative integer")
	}
	return n, nil
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

	album := albumFromRecord(record)
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)

	// 1. Prepend the new album inside the target container
	var component templ.Component
	var targetID string
	if album.Status == templates.StatusWaiting {
		component = templates.AlbumCard(album)
		targetID = "albums-waiting"
	} else {
		component = templates.AlbumRow(album)
		targetID = "albums-" + album.Status
	}

	if err := sse.PatchElementTempl(
		component,
		patchOpts(h.cfg, "#"+targetID, datastar.WithSelectorID(targetID), datastar.WithModePrepend())...,
	); err != nil {
		return fmt.Errorf("prepend album %s into %s: %w", album.ID, targetID, err)
	}

	// 2. Morph/replace the feedback notice in the modal
	return sse.PatchElementTempl(templates.AddAlbumSuccessNotice(album.Title))
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
	if errors.Is(err, sql.ErrNoRows) {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
	} else if err != nil {
		log.Printf("[albums] atomicUpdateAlbumStatus error: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
	}

	album := albumFromRecord(record)
	if oldStatus == album.Status {
		return renderAlbumResponse(e, h.cfg, album)
	}

	return h.patchAlbumStatusTransition(e, album, oldStatus)
}

// patchAlbumStatusTransition emits the Datastar SSE patches needed to move an
// album between the waiting list and a status tbody (or vice versa): first
// removes the old DOM element, then prepends the new one to its target container.
func (h *Handler) patchAlbumStatusTransition(e *core.RequestEvent, album templates.Album, oldStatus string) error {
	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)

	// 1. Remove the old element from the DOM cleanly using true Datastar remove mode.
	var removeID string
	if oldStatus == templates.StatusWaiting {
		removeID = "album-card-" + album.ID
	} else {
		removeID = "album-" + album.ID
	}
	if err := sse.PatchElements("", datastar.WithSelectorID(removeID), datastar.WithModeRemove()); err != nil {
		return fmt.Errorf("remove album element %s: %w", removeID, err)
	}

	// 2. Prepend the new element to its target container cleanly using true Datastar prepend mode.
	component, prependTargetID := albumReplacementElement(album)
	if err := sse.PatchElementTempl(
		component,
		patchOpts(h.cfg, "#"+prependTargetID, datastar.WithSelectorID(prependTargetID), datastar.WithModePrepend())...,
	); err != nil {
		return fmt.Errorf("prepend album %s into %s: %w", album.ID, prependTargetID, err)
	}

	return nil
}

// albumReplacementElement returns the templ component and target container ID
// for rendering an album that just transitioned into its new status.
func albumReplacementElement(album templates.Album) (templ.Component, string) {
	if album.Status == templates.StatusWaiting {
		return templates.AlbumCard(album), "albums-waiting"
	}
	return templates.AlbumRow(album), "albums-" + album.Status
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
			return fmt.Errorf("find albums record %s: %w", albumID, txErr)
		}

		oldStatus = albumStatusForUI(record.GetString("status"))
		record.Set("status", statusDB)

		if txErr := txApp.Save(record); txErr != nil {
			return fmt.Errorf("save albums record %s: %w", albumID, txErr)
		}
		return nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("atomic update album status %s: %w", albumID, err)
	}
	return record, oldStatus, nil
}

type albumSongField string

const (
	albumCollectionSongs albumSongField = "collection_songs"
	albumTotalSongs      albumSongField = "total_songs"
)

func (h *Handler) handleUpdateAlbumSongField(field albumSongField) func(*core.RequestEvent) error {
	return func(e *core.RequestEvent) error {
		albumID := e.Request.PathValue("albumId")
		if albumID == "" {
			return e.JSON(http.StatusBadRequest, map[string]string{"error": "album ID required"})
		}

		delta, err := parseSongCountAction(e.Request.PathValue("action"))
		if err != nil {
			return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		record, err := h.atomicUpdateAlbumSongField(e.Request.Context(), albumID, field, delta)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return e.JSON(http.StatusNotFound, map[string]string{"error": "album not found"})
			}
			log.Printf("[albums] atomicUpdateAlbumSongField error: %v", err)
			return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album"})
		}

		return renderAlbumResponse(e, h.cfg, albumFromRecord(record))
	}
}

func (h *Handler) atomicUpdateAlbumSongField(ctx context.Context, albumID string, field albumSongField, delta int) (*core.Record, error) {
	var record *core.Record
	err := h.app.RunInTransaction(func(txApp core.App) error {
		query, err := albumSongUpdateQuery(field)
		if err != nil {
			return err
		}

		result, txErr := txApp.DB().NewQuery(query).
			Bind(dbx.Params{"albumID": albumID, "delta": delta}).
			WithContext(ctx).
			Execute()
		if txErr != nil {
			return fmt.Errorf("update albums record %s field %s by delta %d: %w", albumID, field, delta, txErr)
		}
		rowsAffected, txErr := result.RowsAffected()
		if txErr != nil {
			return fmt.Errorf("read affected rows for albums record %s field %s: %w", albumID, field, txErr)
		}
		if rowsAffected == 0 {
			return sql.ErrNoRows
		}

		record, txErr = txApp.FindRecordById("albums", albumID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if txErr != nil {
			return fmt.Errorf("find updated albums record %s after field %s delta %d: %w", albumID, field, delta, txErr)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("atomic update album song field %s: %w", albumID, err)
	}
	return record, nil
}

func albumSongUpdateQuery(field albumSongField) (string, error) {
	switch field {
	case albumCollectionSongs:
		return `
			UPDATE albums
			SET collection_songs = MAX(COALESCE(collection_songs, 0) + {:delta}, 0),
				total_songs = CASE
					WHEN MAX(COALESCE(collection_songs, 0) + {:delta}, 0) > COALESCE(total_songs, 0) THEN MAX(COALESCE(collection_songs, 0) + {:delta}, 0)
					ELSE COALESCE(total_songs, 0)
				END
			WHERE id = {:albumID}
		`, nil
	case albumTotalSongs:
		return `
			UPDATE albums
			SET total_songs = MAX(COALESCE(total_songs, 0) + {:delta}, 0),
				collection_songs = CASE
					WHEN MAX(COALESCE(total_songs, 0) + {:delta}, 0) < COALESCE(collection_songs, 0) THEN MAX(COALESCE(total_songs, 0) + {:delta}, 0)
					ELSE COALESCE(collection_songs, 0)
				END
			WHERE id = {:albumID}
		`, nil
	default:
		return "", fmt.Errorf("unsupported album song field %q", field)
	}
}
