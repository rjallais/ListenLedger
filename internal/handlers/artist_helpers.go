// Package handlers provides HTTP request handlers and helper functions for
// managing artist data and related dashboard workflows.
package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"zombiezen.com/go/sqlite"

	"ListenLedger/config"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/priority"
	"ListenLedger/internal/quota"
	"ListenLedger/templates"
)

const (
	defaultArtistGenreGroup      = "rock_metal"
	defaultArtistListStatus      = "recently_added"
	defaultArtistPage            = 1
	defaultArtistPageSize        = 50
	maxArtistPageSize            = 100
	defaultWaitingArtistPageSize = 1
	maxWaitingArtistPageSize     = 10
	waitingArtistStatus          = "waiting"

	// maxBatchRefreshCount caps the user-supplied count in batch refresh requests.
	maxBatchRefreshCount = 200
)

type artistCreateInput struct {
	name             string
	spotifyID        string
	genreGroup       string
	listStatus       string
	monthlyListeners int
	collectionSongs  int
}

type artistListParams struct {
	page  int
	limit int
	genre string
}

type waitingArtistListParams struct {
	offset int
	limit  int
}

// artistRankCache provides O(1) rank lookup for artists by genre.
// Built once per request and reused to avoid O(N²) behavior.
type artistRankCache struct {
	genre      string
	totalCount int
	ranks      map[string]int // record.ID -> rank (1-indexed)
}

// buildArtistRankMap creates a rank cache for the given genre by fetching all
// non-waiting artists sorted by monthly_listeners descending.
func (h *Handler) buildArtistRankMap(ctx context.Context, genre string) (*artistRankCache, error) {
	totalCount, err := h.countArtistsByGenreExcludingWaiting(ctx, genre)
	if err != nil {
		return nil, fmt.Errorf("buildArtistRankMap: count artists for genre %s: %w", genre, err)
	}
	if totalCount == 0 {
		return &artistRankCache{genre: genre, totalCount: totalCount, ranks: make(map[string]int)}, nil
	}

	ranks := make(map[string]int, totalCount)
	if h.db != nil {
		err = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT id FROM artists WHERE genre_group = ? AND list_status != 'waiting' ORDER BY monthly_listeners DESC, id ASC;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, genre)

			rank := 1
			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}
				ranks[stmt.ColumnText(0)] = rank
				rank++
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch artists for rank map: %w", err)
		}
		return &artistRankCache{genre: genre, totalCount: totalCount, ranks: ranks}, nil
	}

	filterParams := nonWaitingArtistParams(genre)

	records := make([]*core.Record, 0)
	err = h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(nonWaitingArtistFilter, filterParams)).
		OrderBy("monthly_listeners DESC", "id ASC").
		Limit(int64(totalCount)).
		All(&records)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch artists for rank map: %w", err)
	}

	for i, record := range records {
		ranks[record.Id] = i + 1 // 1-indexed rank
	}

	return &artistRankCache{genre: genre, totalCount: totalCount, ranks: ranks}, nil
}

// rank returns the 1-indexed position for the artist, or 0 if not found.
func (c *artistRankCache) rank(recordID string) int {
	return c.ranks[recordID]
}

// dynamicTotalSongs returns the dynamic total songs count using the rank cache.
// If cache is nil, falls back to computing rank via query (for backward compatibility).
func (h *Handler) dynamicTotalSongs(ctx context.Context, record *core.Record, cache *artistRankCache) int {
	collectionSongs := record.GetInt("collection_songs")
	if record.GetString("list_status") == waitingArtistStatus {
		return collectionSongs
	}

	// Use cached rank if available (O(1))
	if cache != nil {
		genre := record.GetString("genre_group")
		if genre != cache.genre {
			log.Printf("[handlers] warning: record %s genre %q != cache genre %q, falling back to collection_songs", record.Id, genre, cache.genre)
			return collectionSongs
		}
		r := cache.rank(record.Id)
		if r > 0 {
			return rankedArtistTotalSongs(cache.totalCount, 0, r-1)
		}
		return collectionSongs
	}

	// Fallback: compute rank via query (for backward compatibility)
	return h.dynamicArtistTotalSongs(ctx, record)
}

func parseArtistCreateInput(r *http.Request) (artistCreateInput, error) {
	if err := r.ParseForm(); err != nil {
		return artistCreateInput{}, fmt.Errorf("parsing form data: %w", err)
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		return artistCreateInput{}, fmt.Errorf("artist name is required")
	}

	spotifyID := strings.TrimSpace(r.FormValue("spotify_id"))
	if spotifyID == "" {
		return artistCreateInput{}, fmt.Errorf("artist ID is required")
	}

	genreGroup, ok := normalizeArtistGenreGroup(r.FormValue("genre_group"))
	if !ok {
		return artistCreateInput{}, fmt.Errorf("genre_group must be rock_metal or everything_else")
	}

	listStatus, ok := normalizeArtistListStatus(r.FormValue("list_status"))
	if !ok {
		return artistCreateInput{}, fmt.Errorf("list_status must be included, recently_added, not_added, or waiting")
	}

	monthlyListeners, err := parseOptionalNonNegativeFormInt(r.FormValue("monthly_listeners"))
	if err != nil {
		return artistCreateInput{}, fmt.Errorf("invalid monthly_listeners: %w", err)
	}
	collectionSongs, err := parseOptionalNonNegativeFormInt(r.FormValue("collection_songs"))
	if err != nil {
		return artistCreateInput{}, fmt.Errorf("invalid collection_songs: %w", err)
	}

	return artistCreateInput{
		name:             name,
		spotifyID:        spotifyID,
		genreGroup:       genreGroup,
		listStatus:       listStatus,
		monthlyListeners: monthlyListeners,
		collectionSongs:  collectionSongs,
	}, nil
}

func parseArtistListParams(r *http.Request) artistListParams {
	return artistListParams{
		page:  getQueryParamInt(r, "page", defaultArtistPage, 1, 0),
		limit: getQueryParamInt(r, "limit", defaultArtistPageSize, 1, maxArtistPageSize),
		genre: normalizeArtistGenreFilter(getQueryParam(r, "genre", "")),
	}
}

func parseBatchRefreshCount(r *http.Request) (int, error) {
	countValue := strings.TrimSpace(r.FormValue("count"))
	if countValue == "" {
		return 0, fmt.Errorf("count required")
	}

	count, err := strconv.Atoi(countValue)
	if err != nil || count < 1 {
		return 0, fmt.Errorf("count must be a positive integer")
	}

	return min(count, maxBatchRefreshCount), nil
}

func parseSongCountAction(action string) (int, error) {
	switch action {
	case "inc":
		return 1, nil
	case "dec":
		return -1, nil
	default:
		return 0, fmt.Errorf("action must be 'inc' or 'dec'")
	}
}

func parseWaitingArtistListParams(r *http.Request) waitingArtistListParams {
	return waitingArtistListParams{
		offset: parseNonNegativeInt(r.URL.Query().Get("offset")),
		limit:  parseBoundedPositiveInt(r.URL.Query().Get("limit"), defaultWaitingArtistPageSize, maxWaitingArtistPageSize),
	}
}

func normalizeArtistGenreGroup(raw string) (string, bool) {
	genreGroup := strings.TrimSpace(raw)
	if genreGroup == "" {
		return defaultArtistGenreGroup, true
	}

	return genreGroup, allowedGenreGroups[genreGroup]
}

func normalizeArtistListStatus(raw string) (string, bool) {
	listStatus := strings.TrimSpace(raw)
	if listStatus == "" {
		return defaultArtistListStatus, true
	}

	return listStatus, allowedListStatuses[listStatus]
}

func normalizeArtistGenreFilter(raw string) string {
	genre := strings.TrimSpace(raw)
	if allowedGenreGroups[genre] {
		return genre
	}

	return defaultArtistGenreGroup
}

func parseBoundedPositiveInt(raw string, defaultValue, maxValue int) int {
	return parseBoundedInt(raw, defaultValue, 1, maxValue)
}

func parseNonNegativeInt(raw string) int {
	return parseBoundedInt(raw, 0, 0, 0)
}

func artistFromRecord(record *core.Record, totalSongs int) templates.Artist {
	return templates.Artist{
		ID:               record.Id,
		Name:             record.GetString("name"),
		SpotifyID:        record.GetString("spotify_id"),
		MonthlyListeners: record.GetInt("monthly_listeners"),
		GenreGroup:       record.GetString("genre_group"),
		ListStatus:       record.GetString("list_status"),
		FetchStatus:      record.GetString("fetch_status"),
		CollectionSongs:  record.GetInt("collection_songs"),
		TotalSongs:       totalSongs,
		LastUpdated:      formatUpdatedAt(record.GetString("last_updated")),
	}
}

// artistsFromRecords converts Records to Artist structs.
func artistsFromRecords(records []*core.Record) []templates.Artist {
	artists := make([]templates.Artist, 0, len(records))
	for _, record := range records {
		total := record.GetInt("collection_songs")
		artists = append(artists, artistFromRecord(record, total))
	}

	return artists
}

func artistsFromRankedRecords(records []*core.Record, totalCount, offset int) []templates.Artist {
	artists := make([]templates.Artist, 0, len(records))
	for i, record := range records {
		artists = append(artists, artistFromRecord(record, rankedArtistTotalSongs(totalCount, offset, i)))
	}

	return artists
}

type artistStatusUpdateParams struct {
	Writer       http.ResponseWriter
	Request      *http.Request
	Event        *core.RequestEvent
	Cfg          *config.Config
	OldStatus    string
	NewStatus    string
	CurrentGenre string
	Artist       templates.Artist
}

func renderUpdatedArtistStatus(params artistStatusUpdateParams) error {
	w := params.Writer
	r := params.Request
	if (w == nil || r == nil) && params.Event != nil {
		w = params.Event.Response
		r = params.Event.Request
	}
	if w == nil || r == nil {
		return errors.New("renderUpdatedArtistStatus: missing writer or request")
	}

	if isWaitingListStatusTransition(params.OldStatus, params.NewStatus) {
		sse := datastar.NewSSE(w, r, sseOpts...)

		// 1. Remove the old element from the DOM cleanly using true Datastar remove mode
		var removeID string
		if params.OldStatus == waitingArtistStatus {
			removeID = templates.ArtistCardID(params.Artist.ID)
		} else {
			removeID = templates.ArtistRowID(params.Artist.ID)
		}
		if err := sse.PatchElements("", datastar.WithSelectorID(removeID), datastar.WithModeRemove()); err != nil {
			return fmt.Errorf("renderUpdatedArtistStatus: remove old element %s: %w", removeID, err)
		}

		// 2. Prepend the new element to its target container cleanly using true Datastar prepend mode
		if params.NewStatus == waitingArtistStatus {
			if err := sse.PatchElementTempl(templates.WaitingArtistCard(params.Artist), datastar.WithSelectorID("artists-waiting"), datastar.WithModePrepend()); err != nil {
				return fmt.Errorf("renderUpdatedArtistStatus: prepend waiting artist %s: %w", params.Artist.ID, err)
			}
		} else if params.Artist.GenreGroup == params.CurrentGenre {
			targetID := templates.ArtistsTBodyID(params.CurrentGenre)
			if err := sse.PatchElementTempl(templates.ArtistRow(params.Artist), datastar.WithSelectorID(targetID), datastar.WithModePrepend()); err != nil {
				return fmt.Errorf("renderUpdatedArtistStatus: prepend artist row %s to %s: %w", params.Artist.ID, targetID, err)
			}
		}

		return nil
	}

	if params.NewStatus == waitingArtistStatus {
		return RenderDatastarWithConfig(w, r, params.Cfg, templates.WaitingArtistCard(params.Artist))
	}

	return RenderDatastarWithConfig(w, r, params.Cfg, templates.ArtistRow(params.Artist))
}

func rankedArtistTotalSongs(totalCount, offset, index int) int {
	position := offset + index + 1
	return totalCount - position + 1
}

func isWaitingListStatusTransition(oldStatus, newStatus string) bool {
	return oldStatus != newStatus && (oldStatus == waitingArtistStatus || newStatus == waitingArtistStatus)
}

const nonWaitingArtistFilter = "genre_group = {:genre} AND list_status != {:waiting}"

func nonWaitingArtistParams(genre string) dbx.Params {
	return dbx.Params{
		"genre":   genre,
		"waiting": waitingArtistStatus,
	}
}

func nonWaitingArtistCountExpr(genre string) dbx.Expression {
	return dbx.NewExp(nonWaitingArtistFilter, nonWaitingArtistParams(genre))
}

func (h *Handler) countArtistsByGenreExcludingWaiting(ctx context.Context, genre string) (int, error) {
	if h.db != nil {
		var count int
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT COUNT(*) FROM artists WHERE genre_group = ? AND list_status != 'waiting';")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, genre)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				count = int(stmt.ColumnInt64(0))
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("countArtistsByGenreExcludingWaiting SQLite: %w", err)
		}
		return count, nil
	}

	var count int64
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		Select("COUNT(*)").
		AndWhere(nonWaitingArtistCountExpr(genre)).
		Limit(1).
		Row(&count)
	if err != nil {
		return 0, fmt.Errorf("countArtistsByGenreExcludingWaiting: query artists count failed for genre %s: %w", genre, err)
	}
	return int(count), nil
}

func (h *Handler) countWaitingArtists(ctx context.Context) (int, error) {
	if h.db != nil {
		var count int
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT COUNT(*) FROM artists WHERE list_status = 'waiting';")
			defer func() { _ = stmt.Reset() }()
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				count = int(stmt.ColumnInt64(0))
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("countWaitingArtists SQLite: %w", err)
		}
		return count, nil
	}

	var count int64
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		Select("COUNT(*)").
		AndWhere(dbx.HashExp{"list_status": waitingArtistStatus}).
		Limit(1).
		Row(&count)
	if err != nil {
		return 0, fmt.Errorf("countWaitingArtists: query artists count failed: %w", err)
	}
	return int(count), nil
}

func (h *Handler) hasAvailableQuota() bool {
	checker := quota.NewChecker(h.cfg)
	return checker.HasAvailableQuota()
}

func (h *Handler) findArtistRecord(ctx context.Context, artistID string) (*core.Record, error) {
	return h.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
}

func (h *Handler) resumableBatchRefreshSnapshot(ctx context.Context, requestedBatchID string) (batchProgressSnapshot, bool) {
	batchID := strings.TrimSpace(requestedBatchID)
	if batchID != "" {
		if snapshot, ok := h.getBatchSnapshot(ctx, batchID); ok {
			log.Printf("[batch] Resuming batch %s (%d/%d complete)", snapshot.ID, snapshot.Completed, snapshot.Total)
			return snapshot, true
		}
	}

	if snapshot, ok := h.getActiveBatchSnapshot(ctx); ok {
		log.Printf(
			"[batch] Active batch %s already running (%d/%d complete); returning current state",
			snapshot.ID,
			snapshot.Completed,
			snapshot.Total,
		)
		return snapshot, true
	}

	return batchProgressSnapshot{}, false
}

func batchRefreshCutoff(now time.Time) string {
	fourHoursAgo := now.Add(-4 * time.Hour)
	return fourHoursAgo.UTC().Format("2006-01-02 15:04:05.000Z")
}

func prioritizeArtistJobs(records []*core.Record) []priority.Job {
	jobs := make([]priority.Job, 0, len(records))
	for _, record := range records {
		jobs = append(jobs, priority.Job{
			Record:   record,
			Priority: priority.Determine(record),
		})
	}

	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority < jobs[j].Priority
		}
		return jobs[i].Record.GetInt("monthly_listeners") > jobs[j].Record.GetInt("monthly_listeners")
	})

	return jobs
}

func (h *Handler) batchRefreshJobs(ctx context.Context, cutoff string) ([]priority.Job, error) {
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp("spotify_id != '' AND spotify_id IS NOT NULL AND (fetch_status IS NULL OR fetch_status != 'pending') AND (last_updated IS NULL OR last_updated = '' OR last_updated < {:cutoff})", dbx.Params{"cutoff": cutoff})).
		All(&records)
	if err != nil {
		return nil, fmt.Errorf("batchRefreshJobs: failed to fetch artists: %w", err)
	}

	return prioritizeArtistJobs(records), nil
}

// emitArtistFetchStatus folds a fetch-status fact into the artist stream and
// projection (best-effort, warn-only). Read-model row writes below are compat
// mirrors; the aggregate is the source of truth for isArtistRefreshPending.
// requestID links the fact to its saga instance in metadata.
func (h *Handler) emitArtistFetchStatus(ctx context.Context, artistID, status, reason, requestID string) {
	if h.artistRepo == nil || strings.TrimSpace(artistID) == "" {
		return
	}
	agg, err := h.artistRepo.Load(ctx, artistID)
	if err != nil {
		return
	}
	if agg.FetchStatus == status {
		return
	}
	if err := agg.SetFetchStatus(status, reason, eventsourcing.Correlation{RequestID: requestID}); err != nil {
		return
	}
	events, err := h.artistRepo.Save(ctx, agg)
	if err != nil {
		log.Printf("[handlers] Warning: fetch-status event %s for %s not saved: %v", status, artistID, err)
		return
	}
	if h.artistProjection != nil && len(events) > 0 {
		if err := h.artistProjection.Project(ctx, agg, events); err != nil {
			log.Printf("[handlers] Warning: fetch-status projection %s for %s failed: %v", status, artistID, err)
		}
	}
}

func (h *Handler) markArtistRefreshQueued(ctx context.Context, record *core.Record, requestID string) error {
	// Event first: aggregate owns pending state, projection updates the
	// SQLite read model. Direct row writes below are compat mirrors.
	h.emitArtistFetchStatus(ctx, record.Id, "pending", "queue refresh", requestID)
	record.Set("fetch_status", "pending")
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return fmt.Errorf("markArtistRefreshQueued: save fetch_status: %w", err)
	}
	// SQLite-first: keep the SQLite read model in sync (source of truth for
	// artists list/counts/getByID reads). PocketBase remains compat for now.
	if h.db != nil {
		if err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE artists SET fetch_status = 'pending' WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, record.Id)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[handlers] Warning: failed to mirror fetch_status pending to SQLite for artist %s: %v", record.Id, err)
		}
	}
	return nil
}

func (h *Handler) unmarkArtistRefreshQueued(ctx context.Context, record *core.Record, previousStatus, requestID string) error {
	status := strings.TrimSpace(previousStatus)
	if status == "" {
		status = "idle"
	}

	h.emitArtistFetchStatus(ctx, record.Id, status, "queue rollback", requestID)
	record.Set("fetch_status", status)
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return fmt.Errorf("unmarkArtistRefreshQueued: save fetch_status: %w", err)
	}
	if h.db != nil {
		if err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE artists SET fetch_status = ? WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, status)
			stmt.BindText(2, record.Id)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[handlers] Warning: failed to mirror fetch_status %s to SQLite for artist %s: %v", status, record.Id, err)
		}
	}
	return nil
}

func respondArtistRefreshQueuedHTTP(w http.ResponseWriter, r *http.Request, artistID, status string) error {
	if wantsJSONResponse(r) {
		return writeJSON(w, http.StatusOK, map[string]string{"status": status})
	}

	sse := datastar.NewSSE(w, r, sseOpts...)
	payload, err := json.Marshal(map[string]map[string]string{"artistFetchStatus": {artistID: status}})
	if err != nil {
		return fmt.Errorf("marshal artistFetchStatus payload: %w", err)
	}
	return sse.PatchSignals(payload)
}

func updateArtistCollectionSongs(record *core.Record, delta int) {
	currentCount := record.GetInt("collection_songs")
	nextCount := max(currentCount+delta, 0)

	record.Set("collection_songs", nextCount)
}

func (h *Handler) dynamicArtistTotalSongs(ctx context.Context, record *core.Record) int {
	collectionSongs := record.GetInt("collection_songs")
	if record.GetString("list_status") == waitingArtistStatus {
		return collectionSongs
	}

	genre := record.GetString("genre_group")
	filterParams := nonWaitingArtistParams(genre)

	totalCount, err := h.countArtistsByGenreExcludingWaiting(ctx, genre)
	if err != nil {
		log.Printf("[handlers] dynamicArtistTotalSongs: count failed for genre %s, artist %s: %v, falling back to collection_songs", genre, record.Id, err)
		return collectionSongs
	}
	if totalCount == 0 {
		return collectionSongs
	}

	records := make([]*core.Record, 0)
	err = h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(nonWaitingArtistFilter, filterParams)).
		OrderBy("monthly_listeners DESC", "id ASC").
		Limit(int64(totalCount)).
		All(&records)
	if err != nil {
		log.Printf("[handlers] dynamicArtistTotalSongs: query failed for genre %s, artist %s: %v, falling back to collection_songs", genre, record.Id, err)
		return collectionSongs
	}

	for i, candidate := range records {
		if candidate.Id == record.Id {
			return rankedArtistTotalSongs(totalCount, 0, i)
		}
	}

	return collectionSongs
}

// getArtistByID fetches a single artist by ID from SQLite or PocketBase.
func (h *Handler) getArtistByID(ctx context.Context, artistID string) (templates.Artist, error) {
	if h.db != nil {
		var artist templates.Artist
		var found bool
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated FROM artists WHERE id = ? LIMIT 1;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, artistID)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			found = true
			artist = templates.Artist{
				ID:               stmt.ColumnText(0),
				Name:             stmt.ColumnText(1),
				SpotifyID:        stmt.ColumnText(2),
				MonthlyListeners: int(stmt.ColumnInt64(3)),
				GenreGroup:       stmt.ColumnText(4),
				ListStatus:       stmt.ColumnText(5),
				FetchStatus:      stmt.ColumnText(6),
				CollectionSongs:  int(stmt.ColumnInt64(7)),
				TotalSongs:       int(stmt.ColumnInt64(8)),
				LastUpdated:      formatUpdatedAt(stmt.ColumnText(9)),
			}
			return nil
		})
		if err != nil {
			return templates.Artist{}, err
		}
		if !found {
			return templates.Artist{}, sql.ErrNoRows
		}
		return artist, nil
	}

	if h.app != nil {
		record, err := h.app.FindRecordById("artists", artistID)
		if err != nil {
			return templates.Artist{}, err
		}
		return artistFromRecord(record, record.GetInt("total_songs")), nil
	}

	return templates.Artist{}, sql.ErrNoRows
}
