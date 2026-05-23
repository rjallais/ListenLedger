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

	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/templates"
)

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
	song             templates.Song
	createdAt        time.Time
	releaseDate      time.Time
	releaseDateValid bool
}

type songPageData struct {
	CurrentPlaylist []templates.Song
	WaitingRemoval  []templates.Song
	PlaylistSort    string
	NotRecentCount  int
}

func parseSongReleaseDate(stored string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02", stored)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func normalizePlaylistSort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case playlistSortReleaseAsc:
		return playlistSortReleaseAsc
	default:
		return playlistSortAddedDesc
	}
}

func (h *Handler) listSongEntries(ctx context.Context) ([]songListEntry, error) {
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

func sortRecentSongEntries(entries []songListEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		left := entries[i]
		right := entries[j]

		if left.song.BatchSeq != right.song.BatchSeq {
			return left.song.BatchSeq > right.song.BatchSeq
		}
		// Within each batch, older insertions keep higher positions and stay
		// longer in the playlist.
		if left.song.BatchPos != right.song.BatchPos {
			return left.song.BatchPos > right.song.BatchPos
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

func sortNotRecentSongEntries(entries []songListEntry) {
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

// compareByReleaseDateAsc returns true when left should sort before right by
// ascending release date, falling through to createdAt then title then ID.
// Invalid/legacy dates are treated as "unknown" and sort after valid dates.
func compareByReleaseDateAsc(left, right songListEntry) bool {
	// Invalid dates sort after valid dates
	if left.releaseDateValid != right.releaseDateValid {
		return left.releaseDateValid
	}
	if !left.releaseDate.Equal(right.releaseDate) {
		return left.releaseDate.Before(right.releaseDate)
	}
	if !left.createdAt.Equal(right.createdAt) {
		return left.createdAt.Before(right.createdAt)
	}
	return compareTitleThenID(left, right)
}

// compareByBatchSeq sorts by batch seq (desc when descending=true, else asc),
// then pos ascending, then createdAt (desc/asc), then song ID ascending.
func compareInt(a, b int, descending bool) bool {
	if descending {
		return a > b
	}
	return a < b
}

func compareTime(a, b time.Time, descending bool) bool {
	if descending {
		return a.After(b)
	}
	return a.Before(b)
}

func compareByBatchSeq(left, right songListEntry, descending bool) bool {
	if left.song.BatchSeq != right.song.BatchSeq {
		return compareInt(left.song.BatchSeq, right.song.BatchSeq, descending)
	}
	if left.song.BatchPos != right.song.BatchPos {
		return left.song.BatchPos < right.song.BatchPos
	}
	if !left.createdAt.Equal(right.createdAt) {
		return compareTime(left.createdAt, right.createdAt, descending)
	}
	return left.song.ID < right.song.ID
}

// compareByBatchSeqDesc sorts by batch seq descending, pos ascending, then
// createdAt descending — used for the current-playlist "added-desc" view.
func compareByBatchSeqDesc(left, right songListEntry) bool {
	return compareByBatchSeq(left, right, true)
}

// compareByBatchSeqAsc sorts by batch seq ascending, pos ascending, then
// createdAt ascending — used for the waiting-removal "added-desc" view.
func compareByBatchSeqAsc(left, right songListEntry) bool {
	return compareByBatchSeq(left, right, false)
}

// compareByWaitingReleaseAsc sorts waiting-removal entries by release date asc,
// then batch seq/pos asc, createdAt asc, title, ID.
// Invalid/legacy dates are treated as "unknown" and sort after valid dates.
func compareByWaitingReleaseAsc(left, right songListEntry) bool {
	// Invalid dates sort after valid dates
	if left.releaseDateValid != right.releaseDateValid {
		return left.releaseDateValid
	}
	if !left.releaseDate.Equal(right.releaseDate) {
		return left.releaseDate.Before(right.releaseDate)
	}
	return compareByBatchSeqAsc(left, right)
}

func compareTitleThenID(left, right songListEntry) bool {
	lt := strings.ToLower(left.song.Title)
	rt := strings.ToLower(right.song.Title)
	if lt != rt {
		return lt < rt
	}
	return left.song.ID < right.song.ID
}

type songSortMode struct {
	releaseAsc func(left, right songListEntry) bool
	defaultCmp func(left, right songListEntry) bool
}

var playlistSortMode = songSortMode{
	releaseAsc: compareByReleaseDateAsc,
	defaultCmp: compareByBatchSeqDesc,
}

var waitingRemovalSortMode = songSortMode{
	releaseAsc: compareByWaitingReleaseAsc,
	defaultCmp: compareByBatchSeqAsc,
}

func sortEntriesByMode(entries []songListEntry, playlistSort string, mode songSortMode) {
	cmp := mode.defaultCmp
	if normalizePlaylistSort(playlistSort) == playlistSortReleaseAsc {
		cmp = mode.releaseAsc
	}
	sort.SliceStable(entries, func(i, j int) bool { return cmp(entries[i], entries[j]) })
}

// partitionRecentEntries splits entries into recent and not-recent slices.
func partitionRecentEntries(entries []songListEntry) (recent, notRecent []songListEntry) {
	recent = make([]songListEntry, 0, len(entries))
	notRecent = make([]songListEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.song.IsRecent {
			recent = append(recent, entry)
		} else {
			notRecent = append(notRecent, entry)
		}
	}
	return
}

// splitPlaylistBuckets partitions sorted recent entries into current-playlist
// and waiting-removal buckets based on songsCurrentPlaylistSize.
func splitPlaylistBuckets(recent []songListEntry) (current, waiting []songListEntry) {
	current = make([]songListEntry, 0, min(len(recent), songsCurrentPlaylistSize))
	waiting = make([]songListEntry, 0, max(0, len(recent)-songsCurrentPlaylistSize))
	for i, entry := range recent {
		if i < songsCurrentPlaylistSize {
			current = append(current, entry)
		} else {
			waiting = append(waiting, entry)
		}
	}
	return
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

func filterNotRecentEntries(entries []songListEntry) []songListEntry {
	notRecent := make([]songListEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.song.IsRecent {
			notRecent = append(notRecent, entry)
		}
	}
	return notRecent
}

func paginateEntries(entries []songListEntry, offset, limit int) []templates.Song {
	total := len(entries)
	if offset >= total {
		return []templates.Song{}
	}
	end := min(offset+limit, total)
	page := make([]templates.Song, 0, end-offset)
	for _, entry := range entries[offset:end] {
		page = append(page, entry.song)
	}
	return page
}

func (h *Handler) nextRecentBatchAssignment(ctx context.Context, now time.Time) (int, int, error) {
	return h.nextRecentBatchAssignmentWithApp(ctx, h.app, now)
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

// updateMaxSeqStats accumulates count, minimum batch position, and latest
// created-at for all entries sharing the current maxSeq.
func updateMaxSeqStats(count, minPos int, latest time.Time, entry songListEntry) (int, int, time.Time) {
	count++
	if pos := clampRecentBatchPos(entry.song.BatchPos); pos < minPos {
		minPos = pos
	}
	if entry.createdAt.After(latest) {
		latest = entry.createdAt
	}
	return count, minPos, latest
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

// loadSongPageData builds song page data for the given sort key, returning an
// HTTP 500 response on error. The bool return is false on failure.
func (h *Handler) loadSongPageData(e *core.RequestEvent, caller string) (songPageData, bool) {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, err := h.buildSongPageData(e.Request.Context(), playlistSort)
	if err != nil {
		if caller != "" {
			log.Printf("[%s] buildSongPageData failed: %v", caller, err)
		}
		_ = e.String(http.StatusInternalServerError, "Failed to load songs")
		return songPageData{}, false
	}
	return pageData, true
}

func (h *Handler) handleSongs(e *core.RequestEvent) error {
	pageData, ok := h.loadSongPageData(e, "handleSongs")
	if !ok {
		return nil
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

	isRecent, err := parseBoolValue(e.Request.PathValue("value"))
	if err != nil {
		return e.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	record, err := h.app.FindRecordById("songs", songID)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "song not found"})
	}

	ctx := e.Request.Context()
	if err := h.applyRecentUpdate(ctx, record, isRecent); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if err := h.app.Save(record); err != nil {
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update song"})
	}

	pageData, err := h.buildSongPageData(ctx, playlistSort)
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

func (h *Handler) handleSongsCurrentPlaylistAPI(e *core.RequestEvent) error {
	return h.renderSongPageData(e, func(d songPageData) error {
		return renderDatastar(e, templates.CurrentPlaylistSection(d.CurrentPlaylist, d.PlaylistSort))
	})
}

func (h *Handler) handleSongsSectionsAPI(e *core.RequestEvent) error {
	return h.renderSongPageData(e, func(d songPageData) error {
		return renderDatastar(e, templates.SongsSections(d.CurrentPlaylist, d.WaitingRemoval, d.NotRecentCount, d.PlaylistSort))
	})
}

func (h *Handler) renderSongPageData(e *core.RequestEvent, render func(songPageData) error) error {
	pageData, ok := h.loadSongPageData(e, "")
	if !ok {
		return nil
	}
	return render(pageData)
}

// needsBatchAssignment reports whether a song being marked recent requires a
// fresh batch sequence/position assignment.
func needsBatchAssignment(wasRecent bool, record *core.Record) bool {
	return !wasRecent || record.GetInt("recent_batch_seq") <= 0 || record.GetInt("recent_batch_pos") <= 0
}

type intParamSpec struct {
	Name    string
	Default int
	Min     int
	Max     int
}

func parseQueryIntParam(r *http.Request, spec intParamSpec) int {
	raw := r.URL.Query().Get(spec.Name)
	if raw == "" {
		return spec.Default
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || outOfRange(parsed, spec) {
		return spec.Default
	}
	return parsed
}

func outOfRange(val int, spec intParamSpec) bool {
	return val < spec.Min || (spec.Max > 0 && val > spec.Max)
}

func (h *Handler) handleSongsNotRecentAPI(e *core.RequestEvent) error {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	offset := parseQueryIntParam(e.Request, intParamSpec{Name: "offset", Default: 0, Min: 0, Max: 0})
	limit := parseQueryIntParam(e.Request, intParamSpec{Name: "limit", Default: songsDefaultPageSize, Min: 1, Max: songsMaxPageSize})

	ctx := e.Request.Context()
	songs, totalCount, err := h.listNotRecentSongs(ctx, offset, limit)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	nextOffset := offset + len(songs)
	hasMore := nextOffset < totalCount

	sse := datastar.NewSSE(e.Response, e.Request, sseOpts...)

	// Append each archived song row inside "#songs-not-recent"
	for _, song := range songs {
		if err := sse.PatchElementTempl(templates.SongRow(song, playlistSort), datastar.WithSelectorID("songs-not-recent"), datastar.WithModeAppend()); err != nil {
			return err
		}
	}

	// Morph/replace the load-more button container "#load-more-songs-not-recent"
	return sse.PatchElementTempl(templates.NotRecentSongsLoadMore(nextOffset, hasMore, playlistSort))
}
