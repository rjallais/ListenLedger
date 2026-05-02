//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"encoding/json"
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
	maxBatchRefreshCount = 100
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

	filterParams := nonWaitingArtistParams(genre)

	records := make([]*core.Record, 0)
	err = h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(nonWaitingArtistFilter, filterParams)).
		OrderBy("monthly_listeners DESC").
		Limit(int64(totalCount)).
		All(&records)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch artists for rank map: %w", err)
	}

	ranks := make(map[string]int, len(records))
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

	return artistCreateInput{
		name:             name,
		spotifyID:        spotifyID,
		genreGroup:       genreGroup,
		listStatus:       listStatus,
		monthlyListeners: parseNonNegativeInt(r.FormValue("monthly_listeners")),
		collectionSongs:  parseNonNegativeInt(r.FormValue("collection_songs")),
	}, nil
}

func parseArtistListParams(r *http.Request) artistListParams {
	return artistListParams{
		page:  parsePositiveInt(r.URL.Query().Get("page"), defaultArtistPage),
		limit: parseBoundedPositiveInt(r.URL.Query().Get("limit"), defaultArtistPageSize, maxArtistPageSize),
		genre: normalizeArtistGenreFilter(r.URL.Query().Get("genre")),
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

func parseCollectionSongsAction(action string) (int, error) {
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

func parsePositiveInt(raw string, defaultValue int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultValue
	}

	return parsed
}

func parseBoundedPositiveInt(raw string, defaultValue, maxValue int) int {
	parsed := parsePositiveInt(raw, defaultValue)
	if parsed > maxValue {
		return maxValue
	}

	return parsed
}

func parseNonNegativeInt(raw string) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0
	}

	return parsed
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
// If cache is provided, uses O(1) rank lookup from the cache.
// For backward compatibility when cache is nil, expects totalSongs closure.
func artistsFromRecords(records []*core.Record, cache *artistRankCache) []templates.Artist {
	artists := make([]templates.Artist, 0, len(records))
	for _, record := range records {
		var total int
		if cache != nil {
			genre := record.GetString("genre_group")
			if genre != cache.genre {
				log.Printf("[handlers] warning: record %s genre %q != cache genre %q, falling back to collection_songs", record.Id, genre, cache.genre)
				total = record.GetInt("collection_songs")
			} else if r := cache.rank(record.Id); r > 0 {
				total = rankedArtistTotalSongs(cache.totalCount, 0, r-1)
			} else {
				total = record.GetInt("collection_songs")
			}
		} else {
			// Backward compatibility: should not reach here in normal usage
			total = record.GetInt("collection_songs")
		}
		artists = append(artists, artistFromRecord(record, total))
	}

	return artists
}

func renderUpdatedArtistStatus(
	e *core.RequestEvent,
	oldStatus, newStatus, currentGenre string,
	artist templates.Artist,
) error {
	if isWaitingListStatusTransition(oldStatus, newStatus) {
		return renderDatastar(e, templates.ArtistStatusTransition(oldStatus, artist, currentGenre))
	}

	if newStatus == waitingArtistStatus {
		return renderDatastar(e, templates.WaitingArtistCard(artist))
	}

	return renderDatastar(e, templates.ArtistRow(artist))
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
	return dbx.NewExp(
		"genre_group = {:genre} AND list_status != {:waiting}",
		nonWaitingArtistParams(genre),
	)
}

func (h *Handler) countArtistsByGenreExcludingWaiting(ctx context.Context, genre string) (int, error) {
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

func (h *Handler) hasAvailableQuota(ctx context.Context) bool {
	checker := quota.NewChecker(h.cfg)
	return checker.HasAvailableQuota(ctx)
}

func (h *Handler) findArtistRecord(ctx context.Context, artistID string) (*core.Record, error) {
	return h.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
}

func (h *Handler) resumableBatchRefreshSnapshot(requestedBatchID string) (batchProgressSnapshot, bool) {
	batchID := strings.TrimSpace(requestedBatchID)
	if batchID != "" {
		if snapshot, ok := h.getBatchSnapshot(batchID); ok {
			log.Printf("[batch] Resuming batch %s (%d/%d complete)", snapshot.ID, snapshot.Completed, snapshot.Total)
			return snapshot, true
		}
	}

	if snapshot, ok := h.getActiveBatchSnapshot(); ok {
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
		AndWhere(dbx.NewExp("spotify_id != '' AND spotify_id IS NOT NULL AND (last_updated = '' OR last_updated < {:cutoff})", dbx.Params{"cutoff": cutoff})).
		All(&records)
	if err != nil {
		return nil, fmt.Errorf("batchRefreshJobs: failed to fetch artists: %w", err)
	}

	return prioritizeArtistJobs(records), nil
}

func (h *Handler) queueArtistRefresh(ctx context.Context, record *core.Record) (string, bool, error) {
	requestID := strconv.FormatInt(time.Now().UnixNano(), 10)
	previousFetchStatus := record.GetString("fetch_status")

	if err := h.markArtistRefreshQueued(ctx, record); err != nil {
		return "", false, fmt.Errorf("queueArtistRefresh: mark queued: %w", err)
	}

	correlation.Associate(record.Id, requestID)
	if err := h.createScrapeJobRecord(ctx, requestID, record.Id); err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		correlation.Clear(record.Id)
		_ = h.deleteScrapeJobRecordByRequestID(cleanupCtx, requestID, record.Id)
		if rollbackErr := h.unmarkArtistRefreshQueued(cleanupCtx, record, previousFetchStatus); rollbackErr != nil {
			log.Printf("[queueArtistRefresh] failed to create scrape job for artist %s: %v (rollback also failed: %v)", record.Id, err, rollbackErr)
		} else {
			log.Printf("[queueArtistRefresh] failed to create scrape job for artist %s: %v (rolled back)", record.Id, err)
		}
		return "", false, fmt.Errorf("failed to create scrape job record: %w", err)
	}

	req := messaging.NewScrapeRequested(
		record.Id,
		record.GetString("spotify_id"),
		record.GetString("name"),
		requestID,
	)

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ack, err := h.publishScrapeRequest(pubCtx, req)
	if err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		correlation.Clear(record.Id)
		_ = h.deleteScrapeJobRecordByRequestID(cleanupCtx, requestID, record.Id)
		if rollbackErr := h.unmarkArtistRefreshQueued(cleanupCtx, record, previousFetchStatus); rollbackErr != nil {
			return "", false, fmt.Errorf("queueArtistRefresh: publish scrape request: %w (rollback queued state failed: %v)", err, rollbackErr)
		}
		return "", false, fmt.Errorf("queueArtistRefresh: publish scrape request: %w", err)
	}

	if ack != nil && ack.Duplicate {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		correlation.Clear(record.Id)
		if delErr := h.deleteScrapeJobRecordByRequestID(cleanupCtx, requestID, record.Id); delErr != nil {
			log.Printf("[queueArtistRefresh] duplicate ack cleanup failed for artist %s request %s: %v", record.Id, requestID, delErr)
		}
		return requestID, true, nil
	}

	return requestID, false, nil
}

func (h *Handler) enqueueBatchRefreshJobs(ctx context.Context, jobs []priority.Job, count int) ([]string, map[string]int) {
	queuedArtistIDs := make([]string, 0, len(jobs))
	stats := make(map[string]int)

	for _, job := range jobs {
		if len(queuedArtistIDs) >= count {
			break
		}

		record := job.Record
		_, duplicate, err := h.queueArtistRefresh(ctx, record)
		if err != nil {
			log.Printf("[batch] Failed to queue %s: %v", record.GetString("name"), err)
			continue
		}
		if duplicate {
			log.Printf("[batch] Duplicate request for %s skipped", record.GetString("name"))
			continue
		}

		queuedArtistIDs = append(queuedArtistIDs, record.Id)
		stats[job.Priority.String()]++
	}

	return queuedArtistIDs, stats
}

func (h *Handler) markArtistRefreshQueued(ctx context.Context, record *core.Record) error {
	record.Set("fetch_status", "pending")
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return fmt.Errorf("markArtistRefreshQueued: save fetch_status: %w", err)
	}
	return nil
}

func (h *Handler) unmarkArtistRefreshQueued(ctx context.Context, record *core.Record, previousStatus string) error {
	status := strings.TrimSpace(previousStatus)
	if status == "" {
		status = "idle"
	}

	record.Set("fetch_status", status)
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return fmt.Errorf("unmarkArtistRefreshQueued: save fetch_status: %w", err)
	}
	return nil
}

func respondArtistRefreshQueued(e *core.RequestEvent, artistID, status string) error {
	if wantsJSONResponse(e.Request) {
		return e.JSON(http.StatusOK, map[string]string{"status": status})
	}

	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)
	payload, err := json.Marshal(map[string]map[string]string{"artistFetchStatus": {artistID: "pending"}})
	if err != nil {
		return fmt.Errorf("marshal artistFetchStatus payload: %w", err)
	}
	return sse.PatchSignals(payload)
}

func updateArtistCollectionSongs(record *core.Record, delta int) {
	currentCount := record.GetInt("collection_songs")
	nextCount := currentCount + delta
	if nextCount < 0 {
		nextCount = 0
	}

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
		OrderBy("monthly_listeners DESC").
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
