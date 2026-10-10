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
	"time"

	"github.com/a-h/templ"
	"github.com/delaneyj/toolbelt/id"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/eventsourcing"
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

// HandleIndex handles the root path and redirects to /albums.
func (h *Handler) HandleIndex(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/albums", http.StatusFound)
}

func (h *Handler) handleIndex(e *core.RequestEvent) error {
	h.HandleIndex(e.Response, e.Request)
	return nil
}

// HandleRobots serves robots.txt instructions.
func (h *Handler) HandleRobots(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
}

func (h *Handler) handleRobots(e *core.RequestEvent) error {
	h.HandleRobots(e.Response, e.Request)
	return nil
}

// HandleAlbums renders the full albums page with initial status counts.
func (h *Handler) HandleAlbums(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var fullCount, processedCount, waitingCount int
	if h.db != nil {
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT status, COUNT(*) FROM albums GROUP BY status;")
			defer func() { _ = stmt.Reset() }()
			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}
				st := stmt.ColumnText(0)
				cnt := int(stmt.ColumnInt64(1))
				switch st {
				case "full":
					fullCount = cnt
				case "processed_once":
					processedCount = cnt
				default:
					waitingCount += cnt
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("[albums] SQLite count by status: %v", err)
			http.Error(w, "Failed to load albums", http.StatusInternalServerError)
			return
		}
	} else if h.app != nil {
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
			http.Error(w, "Failed to load albums", http.StatusInternalServerError)
			return
		}
		for _, row := range rows {
			switch row.Status {
			case "full":
				fullCount = row.Count
			case "processed_once":
				processedCount = row.Count
			default:
				waitingCount += row.Count
			}
		}
	}

	_ = RenderTempl(w, r, templates.AlbumsPage(fullCount, processedCount, waitingCount))
}

func (h *Handler) handleAlbums(e *core.RequestEvent) error {
	h.HandleAlbums(e.Response, e.Request)
	return nil
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

func albumsFromRecords(records []*core.Record) []templates.Album {
	albums := make([]templates.Album, len(records))
	for i, r := range records {
		albums[i] = albumFromRecord(r)
	}
	return albums
}

func parseAlbumListParams(r *http.Request, status string) albumListParams {
	limit := getQueryParamInt(r, "limit", defaultAlbumPageSize, 1, maxAlbumPageSize)
	if status == templates.StatusWaiting && r.URL.Query().Get("limit") == "" {
		limit = defaultWaitingAlbumLimit
	}
	return albumListParams{
		offset: getQueryParamInt(r, "offset", 0, 0, 0),
		limit:  limit,
	}
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

func (h *Handler) renderAlbumResponseHTTP(w http.ResponseWriter, r *http.Request, albumView templates.Album) error {
	if albumView.Status == templates.StatusWaiting {
		return h.RenderDatastar(w, r, templates.AlbumCard(albumView))
	}
	return h.RenderDatastar(w, r, templates.AlbumRow(albumView))
}

func (h *Handler) renderAlbumResponse(e *core.RequestEvent, albumView templates.Album) error {
	return h.renderAlbumResponseHTTP(e.Response, e.Request, albumView)
}

func patchAlbums(sse *datastar.ServerSentEventGenerator, targetID string, albumViews []templates.Album, isWaiting bool) error {
	for _, albumView := range albumViews {
		var component templ.Component
		if isWaiting {
			component = templates.AlbumCard(albumView)
		} else {
			component = templates.AlbumRow(albumView)
		}
		if err := sse.PatchElementTempl(component, datastar.WithSelectorID(targetID), datastar.WithModeAppend()); err != nil {
			return fmt.Errorf("patch album %s into %s: %w", albumView.ID, targetID, err)
		}
	}
	return nil
}

func (h *Handler) fetchAlbumsFromDB(ctx context.Context, status string, offset, limit int) ([]templates.Album, int, error) {
	var countQuery, selectQuery string
	switch status {
	case "full":
		countQuery = "SELECT COUNT(*) FROM albums WHERE status = 'full';"
		selectQuery = "SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums WHERE status = 'full' ORDER BY total_songs DESC, LOWER(title) ASC LIMIT ? OFFSET ?;"
	case "processed":
		countQuery = "SELECT COUNT(*) FROM albums WHERE status = 'processed_once';"
		selectQuery = "SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums WHERE status = 'processed_once' ORDER BY (total_songs - collection_songs) DESC, CASE WHEN total_songs > 0 THEN CAST(collection_songs AS REAL) / total_songs ELSE 0 END DESC, LOWER(title) ASC LIMIT ? OFFSET ?;"
	case "waiting":
		countQuery = "SELECT COUNT(*) FROM albums WHERE status NOT IN ('full', 'processed_once');"
		selectQuery = "SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums WHERE status NOT IN ('full', 'processed_once') ORDER BY CASE WHEN total_songs > 0 THEN CAST(collection_songs AS REAL) / total_songs ELSE 0 END DESC, collection_songs DESC, LOWER(title) ASC LIMIT ? OFFSET ?;"
	default:
		return nil, 0, fmt.Errorf("invalid status: %s", status)
	}

	var totalCount int
	var albumViews []templates.Album

	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		cntStmt := tx.Prep(countQuery)
		defer func() { _ = cntStmt.Reset() }()
		if hasRow, err := cntStmt.Step(); err != nil {
			return err
		} else if hasRow {
			totalCount = int(cntStmt.ColumnInt64(0))
		}

		if totalCount == 0 {
			return nil
		}

		selStmt := tx.Prep(selectQuery)
		defer func() { _ = selStmt.Reset() }()
		selStmt.BindInt64(1, int64(limit))
		selStmt.BindInt64(2, int64(offset))

		for {
			hasRow, err := selStmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			albumViews = append(albumViews, templates.Album{
				ID:              selStmt.ColumnText(0),
				Title:           selStmt.ColumnText(1),
				ArtistName:      selStmt.ColumnText(2),
				CollectionSongs: int(selStmt.ColumnInt64(3)),
				TotalSongs:      int(selStmt.ColumnInt64(4)),
				Status:          albumStatusForUI(selStmt.ColumnText(5)),
			})
		}
		return nil
	})

	return albumViews, totalCount, err
}

// HandleAlbumsAPI returns lazy-loaded album items via Datastar SSE.
func (h *Handler) HandleAlbumsAPI(w http.ResponseWriter, r *http.Request) {
	status := getRouteParam(r, "status")

	cfg, err := albumFilterConfigForStatus(status)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid status")
		return
	}

	params := parseAlbumListParams(r, status)

	var albums []templates.Album
	var totalCount int
	if h.db != nil {
		var fetchErr error
		albums, totalCount, fetchErr = h.fetchAlbumsFromDB(r.Context(), status, params.offset, params.limit)
		if fetchErr != nil {
			log.Printf("[albums] handleAlbumsAPI fetch error (status=%q): %v", status, fetchErr)
			http.Error(w, "Failed to load albums", http.StatusInternalServerError)
			return
		}
	} else {
		records, pbTotal, err := h.fetchAlbumRecords(r.Context(), cfg, params.offset, params.limit)
		if err != nil {
			log.Printf("[albums] handleAlbumsAPI fetch error (status=%q): %v", status, err)
			http.Error(w, "Failed to load albums", http.StatusInternalServerError)
			return
		}
		totalCount = pbTotal
		albums = albumsFromRecords(records)
	}

	hasMore := params.offset+len(albums) < totalCount

	sse := datastar.NewSSE(w, r, sseOpts...)
	targetID := "albums-" + status

	if err := patchAlbums(sse, targetID, albums, status == templates.StatusWaiting); err != nil {
		log.Printf("[albums] patchAlbums error: %v", err)
		return
	}

	_ = sse.PatchElementTempl(templates.AlbumLoadMore(status, params.offset+len(albums), hasMore))
}

func (h *Handler) handleAlbumsAPI(e *core.RequestEvent) error {
	h.HandleAlbumsAPI(e.Response, e.Request)
	return nil
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
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("must be a non-negative integer")
	}
	return parsed, nil
}

// writeAlbumCreateError reports an album create failure as JSON or a Datastar notice.
func writeAlbumCreateError(w http.ResponseWriter, r *http.Request, msg string) {
	if wantsJSONResponse(r) {
		writeError(w, http.StatusInternalServerError, msg)
		return
	}
	sse := datastar.NewSSE(w, r, sseOpts...)
	_ = patchFeedbackNotice(sse, templates.AddAlbumErrorNotice(msg))
}

// deleteOrphanAlbumRecord removes the already-created PocketBase album row when
// the event-store write fails, mirroring the SQLite-branch rollback. Best-effort:
// a failed delete only logs, since the request already failed.
func deleteOrphanAlbumRecord(ctx context.Context, h *Handler, record *core.Record) {
	if h == nil || h.app == nil || record == nil {
		return
	}
	// Detached bounded context: rollback runs after a save failure, which
	// can stem from request cancellation — the request context alone would
	// abort the cleanup and leave the orphan behind.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if delErr := h.app.DeleteWithContext(ctx, record); delErr != nil {
		log.Printf("[albums] handleCreateAlbum rollback Delete error: %v", delErr)
	}
}

// HandleCreateAlbum creates a new album record and returns morphed fragments.
func (h *Handler) HandleCreateAlbum(w http.ResponseWriter, r *http.Request) {
	input, err := parseAlbumCreateInput(r)
	if err != nil {
		if wantsJSONResponse(r) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = patchFeedbackNotice(sse, templates.AddAlbumErrorNotice(err.Error()))
		return
	}

	albumID := "al_" + id.NextEncodedID()
	var record *core.Record
	if h.app != nil {
		collection, err := h.app.FindCollectionByNameOrId("albums")
		if err != nil {
			log.Printf("[albums] handleCreateAlbum FindCollection error: %v", err)
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to save album")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = patchFeedbackNotice(sse, templates.AddAlbumErrorNotice("failed to save album"))
			return
		}
		record = core.NewRecord(collection)
		record.Set("title", input.title)
		record.Set("artist_name", input.artistName)
		record.Set("status", input.statusDB)
		record.Set("collection_songs", input.collectionSongs)
		record.Set("total_songs", input.totalSongs)
		if saveErr := h.app.Save(record); saveErr != nil {
			log.Printf("[albums] handleCreateAlbum Save error: %v", saveErr)
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to save album")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = patchFeedbackNotice(sse, templates.AddAlbumErrorNotice("failed to save album"))
			return
		}
		albumID = record.Id
	}

	switch {
	case h.albumRepo != nil:
		agg, aggErr := album.NewAlbum(albumID, input.title, input.artistName, input.statusDB, int64(input.collectionSongs), int64(input.totalSongs))
		if aggErr != nil {
			log.Printf("[albums] NewAlbum %s error: %v", albumID, aggErr)
			deleteOrphanAlbumRecord(r.Context(), h, record)
			writeAlbumCreateError(w, r, "failed to save album")
			return
		}
		events, saveErr := h.albumRepo.Save(r.Context(), agg)
		if saveErr != nil {
			log.Printf("[albums] albumRepo.Save error for %s: %v", albumID, saveErr)
			deleteOrphanAlbumRecord(r.Context(), h, record)
			writeAlbumCreateError(w, r, "failed to save album")
			return
		}
		if h.catalogProjection != nil {
			if projErr := h.catalogProjection.Project(r.Context(), album.StreamTypeAlbum, events); projErr != nil {
				log.Printf("[albums] catalogProjection.Project error for %s: %v", albumID, projErr)
			}
		}
	case h.db != nil:
		err := h.db.WriteTX(r.Context(), func(tx *sqlite.Conn) error {
			stmt := tx.Prep("INSERT INTO albums (id, title, artist_name, collection_songs, total_songs, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?);")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, albumID)
			stmt.BindText(2, input.title)
			stmt.BindText(3, input.artistName)
			stmt.BindInt64(4, int64(input.collectionSongs))
			stmt.BindInt64(5, int64(input.totalSongs))
			stmt.BindText(6, input.statusDB)
			stmt.BindText(7, time.Now().UTC().Format(time.RFC3339Nano))
			_, err := stmt.Step()
			return err
		})
		if err != nil {
			log.Printf("[albums] handleCreateAlbum WriteTX error: %v", err)
			deleteOrphanAlbumRecord(r.Context(), h, record)
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to save album")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = patchFeedbackNotice(sse, templates.AddAlbumErrorNotice("failed to save album"))
			return
		}

	}

	var albumView templates.Album
	if record != nil {
		albumView = albumFromRecord(record)
	} else {
		albumView = templates.Album{
			ID:              albumID,
			Title:           input.title,
			ArtistName:      input.artistName,
			CollectionSongs: input.collectionSongs,
			TotalSongs:      input.totalSongs,
			Status:          albumStatusForUI(input.statusDB),
		}
	}
	if wantsJSONResponse(r) {
		_ = writeJSON(w, http.StatusCreated, albumView)
		return
	}
	sse := datastar.NewSSE(w, r, sseOpts...)

	// 1. Prepend the new album inside the target container
	var component templ.Component
	var targetID string
	if albumView.Status == templates.StatusWaiting {
		component = templates.AlbumCard(albumView)
		targetID = "albums-waiting"
	} else {
		component = templates.AlbumRow(albumView)
		targetID = "albums-" + albumView.Status
	}

	if err := sse.PatchElementTempl(component, datastar.WithSelectorID(targetID), datastar.WithModePrepend()); err != nil {
		log.Printf("prepend album %s into %s error: %v", albumView.ID, targetID, err)
		return
	}

	// 2. Morph/replace the feedback notice in the modal
	_ = patchFeedbackNotice(sse, templates.AddAlbumSuccessNotice(albumView.Title))

	// 3. Reset form cleanly via morphing
	_ = sse.PatchElementTempl(templates.AddAlbumForm())
}

func (h *Handler) handleCreateAlbum(e *core.RequestEvent) error {
	h.HandleCreateAlbum(e.Response, e.Request)
	return nil
}

// HandleUpdateAlbumStatus changes album status and moves the element to the new list container.
func (h *Handler) HandleUpdateAlbumStatus(w http.ResponseWriter, r *http.Request) {
	albumID := getRouteParam(r, "albumId")
	if albumID == "" {
		writeError(w, http.StatusBadRequest, "album ID required")
		return
	}

	newStatus := getRouteParam(r, "status")
	statusDB, ok := albumStatusForDB(newStatus)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid status value")
		return
	}

	albumView, oldStatus, err := h.atomicUpdateAlbumStatus(r.Context(), albumID, statusDB)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "album not found")
		return
	} else if err != nil {
		log.Printf("[albums] atomicUpdateAlbumStatus error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to update album")
		return
	}

	if oldStatus != albumView.Status && h.albumRepo != nil {
		if err := h.recordAlbumStatusEvent(r.Context(), albumID, statusDB, albumView, oldStatus); err != nil {
			log.Printf("[albums] recordAlbumStatusEvent error for %s: %v", albumID, err)
		}
	}

	if oldStatus == albumView.Status {
		_ = h.renderAlbumResponseHTTP(w, r, albumView)
		return
	}

	sse := datastar.NewSSE(w, r, sseOpts...)

	// 1. Remove the old element from the DOM cleanly using true Datastar remove mode
	var removeID string
	if oldStatus == templates.StatusWaiting {
		removeID = "album-card-" + albumView.ID
	} else {
		removeID = "album-" + albumView.ID
	}
	if err := sse.PatchElements("", datastar.WithSelectorID(removeID), datastar.WithModeRemove()); err != nil {
		log.Printf("[albums] remove album element error: %v", err)
		return
	}

	// 2. Prepend the new element to its target container cleanly using true Datastar prepend mode
	var component templ.Component
	var prependTargetID string
	if albumView.Status == templates.StatusWaiting {
		component = templates.AlbumCard(albumView)
		prependTargetID = "albums-waiting"
	} else {
		component = templates.AlbumRow(albumView)
		prependTargetID = "albums-" + albumView.Status
	}

	if err := sse.PatchElementTempl(component, datastar.WithSelectorID(prependTargetID), datastar.WithModePrepend()); err != nil {
		log.Printf("[albums] prepend album element error: %v", err)
		return
	}
}

// recordAlbumStatusEvent appends an AlbumStatusChanged event and projects it.
// Legacy stream-less rows seed an AlbumCreated fact from the committed view
// first so the transition is never lost. Seeding needs no PocketBase read:
// counts are untouched by a status change, so post-state counts plus the old
// (DB-mapped) status reconstruct the legacy row exactly.
func (h *Handler) recordAlbumStatusEvent(ctx context.Context, albumID, newStatus string, committed templates.Album, oldStatus string) error {
	agg, err := h.albumRepo.Load(ctx, albumID)
	if err != nil {
		if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
			return err
		}
		seedStatus, ok := albumStatusForDB(oldStatus)
		if !ok {
			seedStatus = "waiting"
		}
		agg, err = h.loadOrCreateAlbum(ctx, albumView{
			id:              albumID,
			title:           committed.Title,
			artistName:      committed.ArtistName,
			status:          seedStatus,
			collectionSongs: int64(committed.CollectionSongs),
			totalSongs:      int64(committed.TotalSongs),
		})
		if err != nil {
			return err
		}
	}
	if err := agg.ChangeStatus(newStatus); err != nil {
		return err
	}
	return h.saveAndProjectAlbum(ctx, agg)
}

// saveAndProjectAlbum persists uncommitted album events and projects them.
func (h *Handler) saveAndProjectAlbum(ctx context.Context, agg *album.Album) error {
	events, err := h.albumRepo.Save(ctx, agg)
	if err != nil {
		return err
	}
	if h.catalogProjection != nil && len(events) > 0 {
		if err := h.catalogProjection.Project(ctx, album.StreamTypeAlbum, events); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) handleUpdateAlbumStatus(e *core.RequestEvent) error {
	h.HandleUpdateAlbumStatus(e.Response, e.Request)
	return nil
}

// upsertAlbumRowFromView backfills a missing SQLite albums row from the
// committed PocketBase post-state view (statusDB is the DB status value).
// Legacy PB-only rows converge instead of failing the call after PB already
// committed. INSERT OR IGNORE keeps it idempotent under retries.
func upsertAlbumRowFromView(tx *sqlite.Conn, albumID string, view templates.Album, statusDB string) error {
	stmt := tx.Prep(`INSERT OR IGNORE INTO albums (id, title, artist_name, collection_songs, total_songs, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	defer func() { _ = stmt.Reset() }()
	stmt.BindText(1, albumID)
	stmt.BindText(2, view.Title)
	stmt.BindText(3, view.ArtistName)
	stmt.BindInt64(4, int64(view.CollectionSongs))
	stmt.BindInt64(5, int64(view.TotalSongs))
	stmt.BindText(6, statusDB)
	stmt.BindText(7, time.Now().UTC().Format(time.RFC3339Nano))
	_, err := stmt.Step()
	return err
}

func (h *Handler) atomicUpdateAlbumStatus(ctx context.Context, albumID string, statusDB string) (templates.Album, string, error) {
	if h.db == nil && h.app == nil {
		return templates.Album{}, "", sql.ErrNoRows
	}

	var oldStatus string
	var albumView templates.Album

	// PocketBase first — if it fails we abort before touching SQLite.
	if h.app != nil {
		err := h.app.RunInTransaction(func(txApp core.App) error {
			rec, txErr := txApp.FindRecordById("albums", albumID, func(q *dbx.SelectQuery) error {
				q.WithContext(ctx)
				return nil
			})
			if txErr != nil {
				return txErr
			}
			if oldStatus == "" {
				oldStatus = albumStatusForUI(rec.GetString("status"))
			}
			rec.Set("status", statusDB)
			if txErr := txApp.Save(rec); txErr != nil {
				return txErr
			}
			albumView = albumFromRecord(rec)
			return nil
		})
		if err != nil {
			return templates.Album{}, "", err
		}
	}

	if h.db != nil {
		err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
			readStmt := tx.Prep("SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums WHERE id = ?;")
			defer func() { _ = readStmt.Reset() }()
			readStmt.BindText(1, albumID)
			hasRow, err := readStmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				if h.app == nil {
					return sql.ErrNoRows
				}
				// Legacy PB-only row: backfill from the committed PB
				// post-state (albumView), then fall through — the status
				// UPDATE below is idempotent.
				if err := upsertAlbumRowFromView(tx, albumID, albumView, statusDB); err != nil {
					return err
				}
				_ = readStmt.Reset()
				readStmt.BindText(1, albumID)
				hasRow, err = readStmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					return sql.ErrNoRows
				}
			}
			// PB-derived oldStatus wins when both stores ran; SQLite only
			// supplies it when PB is unconfigured.
			if oldStatus == "" {
				oldStatus = albumStatusForUI(readStmt.ColumnText(5))
			}

			updStmt := tx.Prep("UPDATE albums SET status = ? WHERE id = ?;")
			defer func() { _ = updStmt.Reset() }()
			updStmt.BindText(1, statusDB)
			updStmt.BindText(2, albumID)
			if _, err := updStmt.Step(); err != nil {
				return err
			}

			albumView = templates.Album{
				ID:              readStmt.ColumnText(0),
				Title:           readStmt.ColumnText(1),
				ArtistName:      readStmt.ColumnText(2),
				CollectionSongs: int(readStmt.ColumnInt64(3)),
				TotalSongs:      int(readStmt.ColumnInt64(4)),
				Status:          albumStatusForUI(statusDB),
			}
			return nil
		})
		if err != nil {
			return templates.Album{}, "", err
		}
	}

	return albumView, oldStatus, nil
}

type albumSongField string

const (
	albumCollectionSongs albumSongField = "collection_songs"
	albumTotalSongs      albumSongField = "total_songs"
)

// HandleAlbumCollectionSongs is the Chi endpoint handler for updating album collection songs.
func (h *Handler) HandleAlbumCollectionSongs(w http.ResponseWriter, r *http.Request) {
	h.HandleUpdateAlbumSongField(albumCollectionSongs)(w, r)
}

// HandleAlbumTotalSongs is the Chi endpoint handler for updating album total songs.
func (h *Handler) HandleAlbumTotalSongs(w http.ResponseWriter, r *http.Request) {
	h.HandleUpdateAlbumSongField(albumTotalSongs)(w, r)
}

// HandleUpdateAlbumSongField returns an http.HandlerFunc that adjusts album song counters.
func (h *Handler) HandleUpdateAlbumSongField(field albumSongField) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		albumID := getRouteParam(r, "albumId")
		if albumID == "" {
			writeError(w, http.StatusBadRequest, "album ID required")
			return
		}

		delta, err := parseSongCountAction(getRouteParam(r, "action"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		albumTpl, err := h.atomicUpdateAlbumSongField(r.Context(), albumID, field, delta)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusNotFound, "album not found")
				return
			}
			log.Printf("[albums] atomicUpdateAlbumSongField error: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to update album")
			return
		}

		if h.albumRepo != nil {
			// loadOrCreateAlbum seeds legacy stream-less rows so the count
			// delta below always lands on a fact instead of vanishing.
			// Seeds take DB statuses; the template view carries UI ones.
			dbStatus, ok := albumStatusForDB(albumTpl.Status)
			if !ok {
				dbStatus = "waiting"
			}
			agg, loadErr := h.loadOrCreateAlbum(r.Context(), albumView{
				id:              albumID,
				title:           albumTpl.Title,
				artistName:      albumTpl.ArtistName,
				status:          dbStatus,
				collectionSongs: int64(albumTpl.CollectionSongs),
				totalSongs:      int64(albumTpl.TotalSongs),
			})
			if loadErr != nil {
				log.Printf("[albums] loadOrCreateAlbum song counts error for %s: %v", albumID, loadErr)
			} else {
				// The SQL update already clamped both fields; record the resulting values.
				if adjErr := agg.AdjustSongCounts(int64(albumTpl.CollectionSongs), int64(albumTpl.TotalSongs)); adjErr != nil {
					log.Printf("[albums] AdjustSongCounts song counts error for %s: %v", albumID, adjErr)
				} else if saveErr := h.saveAndProjectAlbum(r.Context(), agg); saveErr != nil {
					log.Printf("[albums] saveAndProjectAlbum song counts error for %s: %v", albumID, saveErr)
				}
			}
		}

		_ = h.renderAlbumResponseHTTP(w, r, albumTpl)
	}
}

func (h *Handler) handleUpdateAlbumSongField(field albumSongField) func(*core.RequestEvent) error {
	handler := h.HandleUpdateAlbumSongField(field)
	return func(e *core.RequestEvent) error {
		handler(e.Response, e.Request)
		return nil
	}
}

func (h *Handler) atomicUpdateAlbumSongField(ctx context.Context, albumID string, field albumSongField, delta int) (templates.Album, error) {
	if h.db == nil && h.app == nil {
		return templates.Album{}, sql.ErrNoRows
	}

	var albumView templates.Album
	var found bool

	// PocketBase first — if it fails we abort before touching SQLite.
	if h.app != nil {
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
			if rowsAffected == 0 && !found {
				return sql.ErrNoRows
			}

			record, txErr := txApp.FindRecordById("albums", albumID, func(q *dbx.SelectQuery) error {
				q.WithContext(ctx)
				return nil
			})
			if txErr != nil {
				return fmt.Errorf("find updated albums record %s after field %s delta %d: %w", albumID, field, delta, txErr)
			}
			albumView = albumFromRecord(record)
			return nil
		})
		if err != nil {
			return templates.Album{}, err
		}
	}

	if h.db != nil {
		err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
			// Existence check first: a legacy PB-only row is backfilled
			// from the committed PB post-state (albumView, delta already
			// applied there) instead of failing after PB committed. The
			// delta UPDATE below is skipped in that case — running it
			// would double-apply.
			probe := tx.Prep("SELECT 1 FROM albums WHERE id = ?;")
			defer func() { _ = probe.Reset() }()
			probe.BindText(1, albumID)
			hasRow, err := probe.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				if h.app == nil {
					return sql.ErrNoRows
				}
				dbStatus, ok := albumStatusForDB(albumView.Status)
				if !ok {
					dbStatus = "waiting"
				}
				if err := upsertAlbumRowFromView(tx, albumID, albumView, dbStatus); err != nil {
					return err
				}
				found = true
				return nil
			}
			rawQuery, err := albumSongUpdateQuery(field)
			if err != nil {
				return err
			}
			deltaCount := strings.Count(rawQuery, "{:delta}")
			query := strings.ReplaceAll(rawQuery, "{:delta}", "?")
			query = strings.ReplaceAll(query, "{:albumID}", "?")
			stmt := tx.Prep(strings.TrimSpace(query))
			defer func() { _ = stmt.Reset() }()
			for i := 1; i <= deltaCount; i++ {
				stmt.BindInt64(i, int64(delta))
			}
			stmt.BindText(deltaCount+1, albumID)
			if _, err := stmt.Step(); err != nil {
				return err
			}

			readStmt := tx.Prep("SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums WHERE id = ?;")
			defer func() { _ = readStmt.Reset() }()
			readStmt.BindText(1, albumID)
			hasRow, err = readStmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return sql.ErrNoRows
			}
			found = true
			albumView = templates.Album{
				ID:              readStmt.ColumnText(0),
				Title:           readStmt.ColumnText(1),
				ArtistName:      readStmt.ColumnText(2),
				CollectionSongs: int(readStmt.ColumnInt64(3)),
				TotalSongs:      int(readStmt.ColumnInt64(4)),
				Status:          albumStatusForUI(readStmt.ColumnText(5)),
			}
			return nil
		})
		if err != nil {
			return templates.Album{}, err
		}
	}

	return albumView, nil
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
