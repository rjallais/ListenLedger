package handlers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/templates"
)

func parseBoolValue(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("value must be true or false")
	}
}

func songReleaseNameFromRecord(record *core.Record) string {
	releaseName := strings.TrimSpace(record.GetString("album"))
	if releaseName != "" {
		return releaseName
	}
	return "—"
}

func songFromRecord(record *core.Record) templates.Song {
	recentBatchSeq := max(record.GetInt("recent_batch_seq"), 0)
	recentBatchPos := max(record.GetInt("recent_batch_pos"), 0)

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

type songPageData struct {
	CurrentPlaylist []templates.Song
	WaitingRemoval  []templates.Song
	PlaylistSort    string
	NotRecentCount  int
}

func (h *Handler) listSongEntries(ctx context.Context) ([]songListEntry, error) {
	if h.db != nil {
		var entries []songListEntry
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT id, title, artist_name, album, release_date, release_type, is_recent, recent_batch_seq, recent_batch_pos, created_at FROM songs;")
			defer func() { _ = stmt.Reset() }()

			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}

				releaseDateRaw := stmt.ColumnText(4)
				rd, valid := parseSongReleaseDate(releaseDateRaw)
				createdAtRaw := stmt.ColumnText(9)
				createdAt, _ := time.Parse(time.RFC3339Nano, createdAtRaw)
				if createdAt.IsZero() {
					createdAt, _ = time.Parse("2006-01-02 15:04:05", createdAtRaw)
				}

				recentBatchSeq := max(int(stmt.ColumnInt64(7)), 0)
				recentBatchPos := max(int(stmt.ColumnInt64(8)), 0)

				albumName := strings.TrimSpace(stmt.ColumnText(3))
				if albumName == "" {
					albumName = "—"
				}

				entries = append(entries, songListEntry{
					song: templates.Song{
						ID:          stmt.ColumnText(0),
						Title:       stmt.ColumnText(1),
						ArtistName:  stmt.ColumnText(2),
						ReleaseDate: formatReleaseDateForUI(releaseDateRaw),
						ReleaseType: stmt.ColumnText(5),
						Album:       albumName,
						IsRecent:    stmt.ColumnInt64(6) != 0,
						BatchSeq:    recentBatchSeq,
						BatchPos:    recentBatchPos,
					},
					createdAt:        createdAt,
					releaseDate:      rd,
					releaseDateValid: valid,
				})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return entries, nil
	}

	return h.listSongEntriesWithApp(ctx, h.app)
}

func (h *Handler) listSongEntriesWithApp(ctx context.Context, app core.App) ([]songListEntry, error) {
	collection, err := app.FindCollectionByNameOrId("songs")
	if err != nil {
		return nil, err
	}

	var records []*core.Record
	err = app.RecordQuery(collection.Id).
		WithContext(ctx).
		All(&records)
	if err != nil {
		return nil, err
	}

	entries := make([]songListEntry, 0, len(records))
	for _, record := range records {
		rd, valid := parseSongReleaseDate(record.GetString("release_date"))
		entries = append(entries, songListEntry{
			song:             songFromRecord(record),
			createdAt:        record.GetDateTime("created").Time(),
			releaseDate:      rd,
			releaseDateValid: valid,
		})
	}

	return entries, nil
}

func (h *Handler) buildSongPageData(ctx context.Context, playlistSort string) (songPageData, error) {
	playlistSort = normalizePlaylistSort(playlistSort)

	entries, err := h.listSongEntries(ctx)
	if err != nil {
		return songPageData{}, err
	}

	recent, notRecent := partitionRecentEntries(entries)
	sortRecentSongEntries(recent)
	sortNotRecentSongEntries(notRecent)

	currentPlaylistEntries, waitingRemovalEntries := splitPlaylistBuckets(recent)
	sortEntriesByMode(currentPlaylistEntries, playlistSort, playlistSortMode)
	sortEntriesByMode(waitingRemovalEntries, playlistSort, waitingRemovalSortMode)

	currentPlaylist := make([]templates.Song, 0, len(currentPlaylistEntries))
	for _, entry := range currentPlaylistEntries {
		currentPlaylist = append(currentPlaylist, entry.song)
	}

	waitingRemoval := make([]templates.Song, 0, len(waitingRemovalEntries))
	for _, entry := range waitingRemovalEntries {
		waitingRemoval = append(waitingRemoval, entry.song)
	}

	return songPageData{
		CurrentPlaylist: currentPlaylist,
		WaitingRemoval:  waitingRemoval,
		NotRecentCount:  len(notRecent),
		PlaylistSort:    playlistSort,
	}, nil
}

func (h *Handler) listNotRecentSongs(ctx context.Context, offset, limit int) ([]templates.Song, int, error) {
	offset = clampOffset(offset)
	limit = clampPageSize(limit)

	entries, err := h.listSongEntries(ctx)
	if err != nil {
		return nil, 0, err
	}

	notRecent := filterNotRecentEntries(entries)
	sortNotRecentSongEntries(notRecent)

	page := paginateEntries(notRecent, offset, limit)
	return page, len(notRecent), nil
}

func clampOffset(offset int) int {
	return max(offset, 0)
}

func clampPageSize(limit int) int {
	if limit <= 0 {
		return songsDefaultPageSize
	}
	return min(limit, songsMaxPageSize)
}

func (h *Handler) nextRecentBatchAssignment(ctx context.Context, now time.Time) (int, int, error) {
	entries, err := h.listSongEntries(ctx)
	if err != nil {
		return 0, 0, err
	}

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now)
	return seq, pos, nil
}

func (h *Handler) nextRecentBatchAssignmentWithApp(ctx context.Context, app core.App, now time.Time) (int, int, error) {
	entries, err := h.listSongEntriesWithApp(ctx, app)
	if err != nil {
		return 0, 0, err
	}

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now)
	return seq, pos, nil
}

func nextRecentBatchAssignmentFromEntries(entries []songListEntry, now time.Time) (int, int) {
	stats := findMaxBatchStats(entries)
	return computeNextBatchPosition(stats, now)
}

type batchStats struct {
	maxSeq      int
	count       int
	minPos      int
	latestAdded time.Time
}

func findMaxBatchStats(entries []songListEntry) batchStats {
	stats := batchStats{minPos: songsRecentBatchSize + 1}
	for _, entry := range entries {
		if !entry.song.IsRecent {
			continue
		}
		stats = accumulateBatchStats(stats, entry)
	}
	return stats
}

func accumulateBatchStats(stats batchStats, entry songListEntry) batchStats {
	seq := max(entry.song.BatchSeq, 1)
	if seq > stats.maxSeq {
		return batchStats{
			maxSeq:      seq,
			count:       1,
			minPos:      clampRecentBatchPos(entry.song.BatchPos),
			latestAdded: entry.createdAt,
		}
	}
	if seq == stats.maxSeq {
		stats.count++
		stats.minPos = min(stats.minPos, clampRecentBatchPos(entry.song.BatchPos))
		if entry.createdAt.After(stats.latestAdded) {
			stats.latestAdded = entry.createdAt
		}
	}
	return stats
}

func computeNextBatchPosition(stats batchStats, now time.Time) (int, int) {
	if stats.maxSeq == 0 {
		return 1, songsRecentBatchSize
	}
	if stats.count >= songsRecentBatchSize || stats.minPos <= 1 {
		return stats.maxSeq + 1, songsRecentBatchSize
	}
	if !stats.latestAdded.IsZero() && now.Sub(stats.latestAdded) >= songsRecentBatchWindow {
		return stats.maxSeq + 1, songsRecentBatchSize
	}
	nextPos := stats.minPos - 1
	if nextPos < 1 {
		return stats.maxSeq + 1, songsRecentBatchSize
	}
	return stats.maxSeq, nextPos
}

func clampRecentBatchPos(pos int) int {
	switch {
	case pos < 1:
		return songsRecentBatchSize
	case pos > songsRecentBatchSize:
		return songsRecentBatchSize
	default:
		return pos
	}
}

// loadSongPageDataHTTP builds song page data for the given sort key, returning an
// HTTP 500 response on error. The bool return is false on failure.
func (h *Handler) loadSongPageDataHTTP(w http.ResponseWriter, r *http.Request, caller string) (songPageData, bool) {
	playlistSort := normalizePlaylistSort(getQueryParam(r, "playlist_sort", ""))
	pageData, err := h.buildSongPageData(r.Context(), playlistSort)
	if err != nil {
		if caller != "" {
			log.Printf("[%s] buildSongPageData failed: %v", caller, err)
		}
		http.Error(w, "Failed to load songs", http.StatusInternalServerError)
		return songPageData{}, false
	}
	return pageData, true
}

func (h *Handler) loadSongPageData(e *core.RequestEvent, caller string) (songPageData, bool) {
	return h.loadSongPageDataHTTP(e.Response, e.Request, caller)
}

// HandleSongs serves the main songs page.
func (h *Handler) HandleSongs(w http.ResponseWriter, r *http.Request) {
	pageData, ok := h.loadSongPageDataHTTP(w, r, "HandleSongs")
	if !ok {
		return
	}
	_ = RenderTempl(w, r, templates.SongsPage(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleSongs(e *core.RequestEvent) error {
	h.HandleSongs(e.Response, e.Request)
	return nil
}

// songRecentError reports a recent-toggle failure while restoring the
// current sections on browser requests, so morph resets the optimistically
// disabled checkbox to committed state. JSON API clients keep the JSON error
// contract. Server-side 5xx failures are logged by callers' context via msg.
func (h *Handler) songRecentError(w http.ResponseWriter, r *http.Request, playlistSort string, status int, msg string) {
	if wantsJSONResponse(r) {
		writeError(w, status, msg)
		return
	}
	if pageData, err := h.buildSongPageData(r.Context(), playlistSort); err == nil {
		_ = h.RenderDatastar(w, r, templates.SongsSections(
			pageData.CurrentPlaylist,
			pageData.WaitingRemoval,
			pageData.NotRecentCount,
			pageData.PlaylistSort,
		))
		return
	}
	writeError(w, status, msg)
}

// HandleUpdateSongRecent toggles or updates a song's is_recent status.
func (h *Handler) HandleUpdateSongRecent(w http.ResponseWriter, r *http.Request) {
	playlistSort := normalizePlaylistSort(getQueryParam(r, "playlist_sort", ""))

	songID := strings.TrimSpace(getRouteParam(r, "songId"))
	if songID == "" {
		h.songRecentError(w, r, playlistSort, http.StatusBadRequest, "song ID required")
		return
	}

	isRecent, err := parseBoolValue(getRouteParam(r, "value"))
	if err != nil {
		h.songRecentError(w, r, playlistSort, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	var batchSeq, batchPos int
	var songRow *legacySongRow

	if h.app != nil {
		record, err := h.app.FindRecordById("songs", songID)
		if err != nil {
			h.songRecentError(w, r, playlistSort, http.StatusNotFound, "song not found")
			return
		}

		if err := h.applyRecentUpdate(ctx, record, isRecent); err != nil {
			h.songRecentError(w, r, playlistSort, http.StatusInternalServerError, err.Error())
			return
		}

		if err := h.app.Save(record); err != nil {
			h.songRecentError(w, r, playlistSort, http.StatusInternalServerError, "failed to update song")
			return
		}
		// Backfill source for songs without an event stream: the PB record
		// carries the converged post-state loadOrCreateSong needs.
		songRow = &legacySongRow{
			title:       record.GetString("title"),
			artistName:  record.GetString("artist_name"),
			album:       record.GetString("album"),
			releaseDate: record.GetString("release_date"),
			releaseYear: int64(record.GetInt("release_year")),
			releaseType: record.GetString("release_type"),
			spotifyID:   record.GetString("spotify_id"),
		}
		batchSeq = record.GetInt("recent_batch_seq")
		batchPos = record.GetInt("recent_batch_pos")
	} else if h.db != nil {
		var oldRecent int
		var existingSeq, existingPos int
		var found bool
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT is_recent, recent_batch_seq, recent_batch_pos, title, artist_name, album, release_date, release_year, release_type, spotify_id FROM songs WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, songID)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				found = true
				oldRecent = int(stmt.ColumnInt64(0))
				existingSeq = int(stmt.ColumnInt64(1))
				existingPos = int(stmt.ColumnInt64(2))
				songRow = &legacySongRow{
					title:       stmt.ColumnText(3),
					artistName:  stmt.ColumnText(4),
					album:       stmt.ColumnText(5),
					releaseDate: stmt.ColumnText(6),
					releaseYear: stmt.ColumnInt64(7),
					releaseType: stmt.ColumnText(8),
					spotifyID:   stmt.ColumnText(9),
				}
			}
			return nil
		})
		if err != nil {
			h.songRecentError(w, r, playlistSort, http.StatusInternalServerError, "failed to query song")
			return
		}
		if !found {
			h.songRecentError(w, r, playlistSort, http.StatusNotFound, "song not found")
			return
		}

		if isRecent {
			if oldRecent != 0 && existingSeq > 0 && existingPos > 0 {
				batchSeq = existingSeq
				batchPos = existingPos
			} else {
				var err error
				batchSeq, batchPos, err = h.nextRecentBatchAssignment(ctx, time.Now())
				if err != nil {
					h.songRecentError(w, r, playlistSort, http.StatusInternalServerError, "failed to assign recent batch")
					return
				}
			}
		}
	} else {
		h.songRecentError(w, r, playlistSort, http.StatusInternalServerError, "no database configured")
		return
	}

	if h.db != nil {
		err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE songs SET is_recent = ?, recent_batch_seq = ?, recent_batch_pos = ? WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			var isRecInt int64
			if isRecent {
				isRecInt = 1
			}
			stmt.BindInt64(1, isRecInt)
			stmt.BindInt64(2, int64(batchSeq))
			stmt.BindInt64(3, int64(batchPos))
			stmt.BindText(4, songID)
			_, err := stmt.Step()
			return err
		})
		if err != nil {
			h.songRecentError(w, r, playlistSort, http.StatusInternalServerError, "failed to update song in sqlite")
			return
		}
	}

	if h.songRepo != nil {
		agg, loadErr := h.loadOrCreateSong(ctx, songID, songRow)
		if loadErr != nil {
			log.Printf("[songs] loadOrCreateSong error for %s: %v", songID, loadErr)
		} else if setErr := agg.SetRecent(isRecent, int64(batchSeq), int64(batchPos)); setErr != nil {
			log.Printf("[songs] SetRecent error for %s: %v", songID, setErr)
		} else if events, saveErr := h.songRepo.Save(ctx, agg); saveErr != nil {
			log.Printf("[songs] songRepo.Save error for %s: %v", songID, saveErr)
		} else if h.catalogProjection != nil && len(events) > 0 {
			if projErr := h.catalogProjection.Project(ctx, song.StreamTypeSong, events); projErr != nil {
				log.Printf("[songs] catalogProjection.Project error for %s: %v", songID, projErr)
			}
		}
	}

	pageData, err := h.buildSongPageData(ctx, playlistSort)
	if err != nil {
		http.Error(w, "Failed to load songs", http.StatusInternalServerError)
		return
	}

	_ = h.RenderDatastar(w, r, templates.SongsSections(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleUpdateSongRecent(e *core.RequestEvent) error {
	h.HandleUpdateSongRecent(e.Response, e.Request)
	return nil
}

// legacySongRow carries the song fields read from SQLite for stream backfill.
type legacySongRow struct {
	title       string
	artistName  string
	album       string
	releaseDate string
	releaseYear int64
	releaseType string
	spotifyID   string
}

// loadOrCreateSong replays the song stream, synthesizing a SongCreated
// event for legacy rows. Recent-flag updates are applied by the caller via SetRecent.
func (h *Handler) loadOrCreateSong(ctx context.Context, songID string, legacy *legacySongRow) (*song.Song, error) {
	agg, loadErr := h.songRepo.Load(ctx, songID)
	if !errors.Is(loadErr, eventsourcing.ErrStreamNotFound) || legacy == nil {
		return agg, loadErr
	}

	createAgg, createErr := song.NewSong(songID, legacy.title, legacy.artistName, legacy.album,
		legacy.releaseDate, legacy.releaseType, legacy.spotifyID, legacy.releaseYear, 0, 0, false)
	if createErr != nil {
		return nil, createErr
	}
	if _, err := h.songRepo.Save(ctx, createAgg); err != nil {
		return nil, err
	}
	return h.songRepo.Load(ctx, songID)
}

func (h *Handler) applyRecentUpdate(ctx context.Context, record *core.Record, isRecent bool) error {
	oldRecent := record.GetBool("is_recent")
	record.Set("is_recent", isRecent)
	if !isRecent {
		record.Set("recent_batch_seq", 0)
		record.Set("recent_batch_pos", 0)
		return nil
	}
	if !needsBatchAssignment(oldRecent, record) {
		return nil
	}
	batchSeq, batchPos, err := h.nextRecentBatchAssignment(ctx, time.Now())
	if err != nil {
		log.Printf("[handleUpdateSongRecent] nextRecentBatchAssignment failed: %v", err)
		return fmt.Errorf("failed to assign recent batch")
	}
	record.Set("recent_batch_seq", batchSeq)
	record.Set("recent_batch_pos", batchPos)
	return nil
}

// HandleSongsCurrentPlaylistAPI returns the current playlist section fragments.
func (h *Handler) HandleSongsCurrentPlaylistAPI(w http.ResponseWriter, r *http.Request) {
	pageData, ok := h.loadSongPageDataHTTP(w, r, "HandleSongsCurrentPlaylistAPI")
	if !ok {
		return
	}
	_ = h.RenderDatastar(w, r, templates.CurrentPlaylistSection(pageData.CurrentPlaylist, pageData.PlaylistSort))
}

func (h *Handler) handleSongsCurrentPlaylistAPI(e *core.RequestEvent) error {
	h.HandleSongsCurrentPlaylistAPI(e.Response, e.Request)
	return nil
}

// HandleSongsSectionsAPI returns all songs sections fragments.
func (h *Handler) HandleSongsSectionsAPI(w http.ResponseWriter, r *http.Request) {
	pageData, ok := h.loadSongPageDataHTTP(w, r, "HandleSongsSectionsAPI")
	if !ok {
		return
	}
	_ = h.RenderDatastar(w, r, templates.SongsSections(pageData.CurrentPlaylist, pageData.WaitingRemoval, pageData.NotRecentCount, pageData.PlaylistSort))
}

func (h *Handler) handleSongsSectionsAPI(e *core.RequestEvent) error {
	h.HandleSongsSectionsAPI(e.Response, e.Request)
	return nil
}

// needsBatchAssignment reports whether a song being marked recent requires a
// fresh batch sequence/position assignment.
func needsBatchAssignment(wasRecent bool, record *core.Record) bool {
	return !wasRecent || record.GetInt("recent_batch_seq") <= 0 || record.GetInt("recent_batch_pos") <= 0
}

// HandleSongsNotRecentAPI returns lazy-loaded not-recent songs.
func (h *Handler) HandleSongsNotRecentAPI(w http.ResponseWriter, r *http.Request) {
	playlistSort := normalizePlaylistSort(getQueryParam(r, "playlist_sort", ""))
	offset := getQueryParamInt(r, "offset", 0, 0, 0)
	limit := getQueryParamInt(r, "limit", songsDefaultPageSize, 1, songsMaxPageSize)

	ctx := r.Context()
	songViews, totalCount, err := h.listNotRecentSongs(ctx, offset, limit)
	if err != nil {
		http.Error(w, "Failed to load songs", http.StatusInternalServerError)
		return
	}

	nextOffset := offset + len(songViews)
	hasMore := nextOffset < totalCount

	sse := datastar.NewSSE(w, r, sseOpts...)

	// Append each archived song row inside "#songs-not-recent"
	for _, songView := range songViews {
		if err := sse.PatchElementTempl(templates.SongRow(songView, playlistSort), datastar.WithSelectorID("songs-not-recent"), datastar.WithModeAppend()); err != nil {
			return
		}
	}

	// Morph/replace the load-more button container "#load-more-songs-not-recent"
	_ = sse.PatchElementTempl(templates.NotRecentSongsLoadMore(nextOffset, hasMore, playlistSort))
}

func (h *Handler) handleSongsNotRecentAPI(e *core.RequestEvent) error {
	h.HandleSongsNotRecentAPI(e.Response, e.Request)
	return nil
}
