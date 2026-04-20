//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

// songFormInput holds validated fields parsed from the create-song form.
type songFormInput struct {
	SongName         string
	AlbumName        string
	ReleaseType      string
	ReleaseDateRaw   string
	ReleaseYear      int
	TotalSongsOnAlbum int
	NewArtistGenre   string
	ArtistSpotifyIDs []string
}

// validateSongForm parses and validates the create-song HTTP form, returning a
// populated songFormInput or an HTTP status + error message on failure.
// parseTotalSongs parses the "total_songs" form field. Returns 0 if absent or invalid.
func parseTotalSongs(r *http.Request) int {
	ts := strings.TrimSpace(r.FormValue("total_songs"))
	if ts == "" {
		return 0
	}
	if parsed, err := strconv.Atoi(ts); err == nil && parsed > 0 {
		return parsed
	}
	return 0
}

// parseArtistIDsField reads artist_spotify_ids (falling back to "artists") and
// validates each Spotify ID. Returns an error and status code on failure.
func parseArtistIDsField(r *http.Request) ([]string, int, string) {
	raw := r.FormValue("artist_spotify_ids")
	if strings.TrimSpace(raw) == "" {
		raw = r.FormValue("artists")
	}
	ids, err := parseSpotifyIDs(raw)
	if err != nil {
		return nil, http.StatusBadRequest, err.Error()
	}
	return ids, 0, ""
}

func validateSongForm(r *http.Request) (songFormInput, int, string) {
	if err := r.ParseForm(); err != nil {
		return songFormInput{}, http.StatusBadRequest, "invalid form data"
	}

	songName := strings.TrimSpace(r.FormValue("name"))
	if songName == "" {
		return songFormInput{}, http.StatusBadRequest, "song name is required"
	}
	albumName := strings.TrimSpace(r.FormValue("album"))
	if albumName == "" {
		return songFormInput{}, http.StatusBadRequest, "album is required"
	}
	releaseType := strings.TrimSpace(r.FormValue("release_type"))
	switch releaseType {
	case "album", "ep", "single":
	case "":
		return songFormInput{}, http.StatusBadRequest, "release type is required"
	default:
		return songFormInput{}, http.StatusBadRequest, "release_type must be album, ep, or single"
	}
	releaseDateRaw := strings.TrimSpace(r.FormValue("release_date"))
	if releaseDateRaw == "" {
		return songFormInput{}, http.StatusBadRequest, "release date is required"
	}
	parsedDate, err := time.Parse("2006-01-02", releaseDateRaw)
	if err != nil {
		return songFormInput{}, http.StatusBadRequest, "release_date must be in YYYY-MM-DD format"
	}

	newArtistGenre := strings.TrimSpace(r.FormValue("new_artist_genre"))
	if newArtistGenre == "" {
		newArtistGenre = "rock_metal"
	}
	if !isValidGenreGroup(newArtistGenre) {
		return songFormInput{}, http.StatusBadRequest, "new_artist_genre must be rock_metal or everything_else"
	}

	artistSpotifyIDs, status, errMsg := parseArtistIDsField(r)
	if errMsg != "" {
		return songFormInput{}, status, errMsg
	}

	return songFormInput{
		SongName:          songName,
		AlbumName:         albumName,
		ReleaseType:       releaseType,
		ReleaseDateRaw:    releaseDateRaw,
		ReleaseYear:       parsedDate.Year(),
		TotalSongsOnAlbum: parseTotalSongs(r),
		NewArtistGenre:    newArtistGenre,
		ArtistSpotifyIDs:  artistSpotifyIDs,
	}, 0, ""
}

// resolveArtistNames looks up each Spotify ID and returns the ordered artist
// display names. Returns an HTTP status and error on the first failure.
func (h *Handler) resolveArtistNames(ctx context.Context, spotifyIDs []string) ([]string, int, error) {
	artists := make([]string, 0, len(spotifyIDs))
	for _, id := range spotifyIDs {
		tctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		name, code, err := h.inferArtistNameFromSpotifyID(tctx, id)
		cancel()
		if err != nil {
			return nil, code, err
		}
		artists = append(artists, name)
	}
	return artists, 0, nil
}

func (h *Handler) handleCreateSong(e *core.RequestEvent) error {
	input, status, errMsg := validateSongForm(e.Request)
	if errMsg != "" {
		return e.JSON(status, map[string]string{"error": errMsg})
	}

	artists, code, err := h.resolveArtistNames(e.Request.Context(), input.ArtistSpotifyIDs)
	if err != nil {
		return e.JSON(code, map[string]string{"error": err.Error()})
	}

	if err := h.upsertAlbumForSong(albumUpsertParams{
		AlbumName:     input.AlbumName,
		PrimaryArtist: artists[0],
		ReleaseType:   input.ReleaseType,
		TotalSongs:    input.TotalSongsOnAlbum,
	}); err != nil {
		log.Printf("[handleCreateSong] upsertAlbumForSong failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update album metadata"})
	}
	newArtists, err := h.upsertArtistsForSong(artists, input.ArtistSpotifyIDs, input.NewArtistGenre)
	if err != nil {
		log.Printf("[handleCreateSong] upsertArtistsForSong failed: %v", err)
		return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to update artist metadata"})
	}
	for _, target := range newArtists {
		if queueErr := h.queueArtistRefreshFromSong(e.Request.Context(), target); queueErr != nil {
			log.Printf("[handleCreateSong] Warning: failed to queue refresh for new artist %s (%s): %v", target.Name, target.ID, queueErr)
		}
	}

	record, saveErr := h.createSongRecord(input, artists)
	if saveErr != nil {
		return e.JSON(saveErr.status, map[string]string{"error": saveErr.Error()})
	}

	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, buildErr := h.buildSongPageData(playlistSort)
	if buildErr != nil {
		log.Printf("[handleCreateSong] buildSongPageData failed: %v", buildErr)
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

// songSaveError carries an HTTP status alongside a standard error for use
// within createSongRecord.
type songSaveError struct {
	status int
	msg    string
}

func (e *songSaveError) Error() string { return e.msg }

// createSongRecord builds and saves the songs record, assigning a recent batch.
func (h *Handler) createSongRecord(input songFormInput, artists []string) (*core.Record, *songSaveError) {
	collection, err := h.app.FindCollectionByNameOrId("songs")
	if err != nil {
		return nil, &songSaveError{http.StatusInternalServerError, "songs collection not found"}
	}
	batchSeq, batchPos, err := h.nextRecentBatchAssignment(time.Now())
	if err != nil {
		log.Printf("[handleCreateSong] nextRecentBatchAssignment failed: %v", err)
		return nil, &songSaveError{http.StatusInternalServerError, "failed to assign recent batch"}
	}

	record := core.NewRecord(collection)
	record.Set("title", input.SongName)
	record.Set("artist_name", strings.Join(artists, ", "))
	record.Set("album", input.AlbumName)
	record.Set("release_type", input.ReleaseType)
	record.Set("release_year", input.ReleaseYear)
	record.Set("release_date", input.ReleaseDateRaw)
	record.Set("artist_spotify_ids", strings.Join(input.ArtistSpotifyIDs, ","))
	record.Set("spotify_id", "")
	record.Set("is_recent", true)
	record.Set("recent_batch_seq", batchSeq)
	record.Set("recent_batch_pos", batchPos)

	if err := h.app.Save(record); err != nil {
		log.Printf("[handleCreateSong] song save failed: %v", err)
		return nil, &songSaveError{http.StatusInternalServerError, "failed to create song"}
	}
	return record, nil
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
	PlaylistSort    string
	NotRecentCount  int
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

// compareByReleaseDateAsc returns true when left should sort before right by
// ascending release date, falling through to createdAt then title then ID.
func compareByReleaseDateAsc(left, right songListEntry) bool {
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
func compareByBatchSeq(left, right songListEntry, descending bool) bool {
	if left.song.BatchSeq != right.song.BatchSeq {
		if descending {
			return left.song.BatchSeq > right.song.BatchSeq
		}
		return left.song.BatchSeq < right.song.BatchSeq
	}
	if left.song.BatchPos != right.song.BatchPos {
		return left.song.BatchPos < right.song.BatchPos
	}
	if !left.createdAt.Equal(right.createdAt) {
		if descending {
			return left.createdAt.After(right.createdAt)
		}
		return left.createdAt.Before(right.createdAt)
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
func compareByWaitingReleaseAsc(left, right songListEntry) bool {
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

func (h *Handler) sortPlaylistEntries(entries []songListEntry, playlistSort string) {
	switch normalizePlaylistSort(playlistSort) {
	case playlistSortReleaseAsc:
		sort.SliceStable(entries, func(i, j int) bool {
			return compareByReleaseDateAsc(entries[i], entries[j])
		})
	default:
		sort.SliceStable(entries, func(i, j int) bool {
			return compareByBatchSeqDesc(entries[i], entries[j])
		})
	}
}

func (h *Handler) sortWaitingRemovalEntries(entries []songListEntry, playlistSort string) {
	switch normalizePlaylistSort(playlistSort) {
	case playlistSortReleaseAsc:
		sort.SliceStable(entries, func(i, j int) bool {
			return compareByWaitingReleaseAsc(entries[i], entries[j])
		})
	default:
		sort.SliceStable(entries, func(i, j int) bool {
			return compareByBatchSeqAsc(entries[i], entries[j])
		})
	}
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

func (h *Handler) buildSongPageData(playlistSort string) (songPageData, error) {
	playlistSort = normalizePlaylistSort(playlistSort)

	entries, err := h.listSongEntries()
	if err != nil {
		return songPageData{}, err
	}

	recent, notRecent := partitionRecentEntries(entries)
	h.sortRecentSongEntries(recent)
	h.sortNotRecentSongEntries(notRecent)

	currentPlaylistEntries, waitingRemovalEntries := splitPlaylistBuckets(recent)
	h.sortPlaylistEntries(currentPlaylistEntries, playlistSort)
	h.sortWaitingRemovalEntries(waitingRemovalEntries, playlistSort)

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

	seq, pos := nextRecentBatchAssignmentFromEntries(entries, now)
	return seq, pos, nil
}

func nextRecentBatchAssignmentFromEntries(entries []songListEntry, now time.Time) (int, int) {
	maxSeq := 0
	maxSeqCount := 0
	maxSeqMinPos := songsRecentBatchSize + 1
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
			maxSeqMinPos = clampRecentBatchPos(entry.song.BatchPos)
			latestInMaxSeq = entry.createdAt
			continue
		}

		if seq == maxSeq {
			maxSeqCount, maxSeqMinPos, latestInMaxSeq = updateMaxSeqStats(maxSeqCount, maxSeqMinPos, latestInMaxSeq, entry)
		}
	}

	if maxSeq == 0 {
		return 1, songsRecentBatchSize
	}
	if maxSeqCount >= songsRecentBatchSize || maxSeqMinPos <= 1 {
		return maxSeq + 1, songsRecentBatchSize
	}
	if !latestInMaxSeq.IsZero() && now.Sub(latestInMaxSeq) >= songsRecentBatchWindow {
		return maxSeq + 1, songsRecentBatchSize
	}

	nextPos := maxSeqMinPos - 1
	if nextPos < 1 {
		return maxSeq + 1, songsRecentBatchSize
	}

	return maxSeq, nextPos
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

// albumUpsertParams bundles the fields needed to upsert an album record,
// replacing the former 4-argument function signature.
type albumUpsertParams struct {
	AlbumName     string
	PrimaryArtist string
	ReleaseType   string
	TotalSongs    int
}

func (h *Handler) upsertAlbumForSong(p albumUpsertParams) error {
	albumName, primaryArtist, releaseType, totalSongsFromUI := p.AlbumName, p.PrimaryArtist, p.ReleaseType, p.TotalSongs
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
		target, isNew, err := h.findOrCreateArtist(collection, artistName, artistSpotifyID, newArtistGenre)
		if err != nil {
			return nil, err
		}
		if isNew {
			newArtists = append(newArtists, target)
		}
	}
	return newArtists, nil
}

// findOrCreateArtist looks up an artist by spotify_id (then by name) and
// increments its collection_songs count. If no record exists it creates one.
// Returns the target, a flag indicating whether the record was newly created,
// and any error.
// lookupArtistRecord searches for an existing artist first by spotify_id,
// then by name. Returns nil records (and no error) if not found.
func (h *Handler) lookupArtistRecord(artistName, artistSpotifyID string) ([]*core.Record, error) {
	records, err := h.app.FindRecordsByFilter(
		"artists", "spotify_id = {:spotify_id}", "", 1, 0,
		dbx.Params{"spotify_id": artistSpotifyID},
	)
	if err != nil {
		return nil, err
	}
	if len(records) > 0 {
		return records, nil
	}
	return h.app.FindRecordsByFilter(
		"artists", "name ~ {:name}", "", 1, 0,
		dbx.Params{"name": artistName},
	)
}

// updateExistingArtist increments collection_songs and back-fills spotify_id
// if missing, then persists the record.
func (h *Handler) updateExistingArtist(record *core.Record, artistSpotifyID string) error {
	record.Set("collection_songs", record.GetInt("collection_songs")+1)
	if record.GetString("spotify_id") == "" {
		record.Set("spotify_id", artistSpotifyID)
	}
	return h.app.Save(record)
}

func (h *Handler) findOrCreateArtist(collection *core.Collection, artistName, artistSpotifyID, newArtistGenre string) (songNewArtistTarget, bool, error) {
	records, err := h.lookupArtistRecord(artistName, artistSpotifyID)
	if err != nil {
		return songNewArtistTarget{}, false, err
	}

	if len(records) > 0 {
		return songNewArtistTarget{}, false, h.updateExistingArtist(records[0], artistSpotifyID)
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
		return songNewArtistTarget{}, false, err
	}
	return songNewArtistTarget{ID: record.Id, Name: artistName, SpotifyID: artistSpotifyID}, true, nil
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

// loadSongPageData builds song page data for the given sort key, returning an
// HTTP 500 response on error. The bool return is false on failure.
func (h *Handler) loadSongPageData(e *core.RequestEvent, caller string) (songPageData, bool) {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	pageData, err := h.buildSongPageData(playlistSort)
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
	if isRecent && needsBatchAssignment(oldRecent, record) {
		batchSeq, batchPos, batchErr := h.nextRecentBatchAssignment(time.Now())
		if batchErr != nil {
			log.Printf("[handleUpdateSongRecent] nextRecentBatchAssignment failed: %v", batchErr)
			return e.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to assign recent batch"})
		}
		record.Set("recent_batch_seq", batchSeq)
		record.Set("recent_batch_pos", batchPos)
	} else if !isRecent {
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
	pageData, ok := h.loadSongPageData(e, "")
	if !ok {
		return nil
	}
	return renderDatastar(e, templates.CurrentPlaylistSection(
		pageData.CurrentPlaylist,
		pageData.PlaylistSort,
	))
}

func (h *Handler) handleSongsSectionsAPI(e *core.RequestEvent) error {
	pageData, ok := h.loadSongPageData(e, "")
	if !ok {
		return nil
	}
	return renderDatastar(e, templates.SongsSections(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	))
}

// needsBatchAssignment reports whether a song being marked recent requires a
// fresh batch sequence/position assignment.
func needsBatchAssignment(wasRecent bool, record *core.Record) bool {
	return !wasRecent || record.GetInt("recent_batch_seq") <= 0 || record.GetInt("recent_batch_pos") <= 0
}

// parseQueryIntParam reads an integer query parameter by name. Returns
// defaultVal when the param is absent, zero, or out of [min, max].
func parseQueryIntParam(r *http.Request, name string, defaultVal, min, max int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return defaultVal
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < min || (max > 0 && parsed > max) {
		return defaultVal
	}
	return parsed
}

func (h *Handler) handleSongsNotRecentAPI(e *core.RequestEvent) error {
	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	offset := parseQueryIntParam(e.Request, "offset", 0, 0, 0)
	limit := parseQueryIntParam(e.Request, "limit", songsDefaultPageSize, 1, songsMaxPageSize)

	songs, totalCount, err := h.listNotRecentSongs(offset, limit)
	if err != nil {
		return e.String(http.StatusInternalServerError, "Failed to load songs")
	}

	nextOffset := offset + len(songs)
	hasMore := nextOffset < totalCount

	return renderDatastar(e, templates.NotRecentSongRows(songs, nextOffset, hasMore, playlistSort))
}
