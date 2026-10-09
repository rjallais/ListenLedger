package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/commands"
	"ListenLedger/internal/correlation"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

var scrapeRequestSeq atomic.Uint64

func newScrapeRequestID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), scrapeRequestSeq.Add(1))
}

type songFormInput struct {
	SongName          string
	AlbumName         string
	ReleaseType       string
	ReleaseDateRaw    string
	ReleaseYear       int
	TotalSongsOnAlbum int
	NewArtistGenre    string
	ArtistSpotifyIDs  []string
}

func parseTotalSongs(r *http.Request) (int, int, string) {
	ts := strings.TrimSpace(r.FormValue("total_songs"))
	if ts == "" {
		return 0, 0, ""
	}
	parsed, err := strconv.Atoi(ts)
	if err != nil || parsed < 1 {
		return 0, http.StatusBadRequest, "total_songs must be a positive integer"
	}
	return parsed, 0, ""
}

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

	releaseType, status, errMsg := validateReleaseType(r.FormValue("release_type"))
	if errMsg != "" {
		return songFormInput{}, status, errMsg
	}

	releaseDateRaw, releaseYear, status, errMsg := validateReleaseDate(r.FormValue("release_date"))
	if errMsg != "" {
		return songFormInput{}, status, errMsg
	}

	newArtistGenre := normalizeGenreGroup(r.FormValue("new_artist_genre"))

	artistSpotifyIDs, status, errMsg := parseArtistIDsField(r)
	if errMsg != "" {
		return songFormInput{}, status, errMsg
	}

	totalSongsOnAlbum, status, errMsg := parseTotalSongs(r)
	if errMsg != "" {
		return songFormInput{}, status, errMsg
	}

	albumName := strings.TrimSpace(r.FormValue("album"))
	if albumName == "" {
		return songFormInput{}, http.StatusBadRequest, "album is required"
	}

	return songFormInput{
		SongName:          songName,
		AlbumName:         albumName,
		ReleaseType:       releaseType,
		ReleaseDateRaw:    releaseDateRaw,
		ReleaseYear:       releaseYear,
		TotalSongsOnAlbum: totalSongsOnAlbum,
		NewArtistGenre:    newArtistGenre,
		ArtistSpotifyIDs:  artistSpotifyIDs,
	}, 0, ""
}

func validateReleaseType(value string) (string, int, string) {
	releaseType := strings.TrimSpace(value)
	switch releaseType {
	case "album", "ep", "single":
		return releaseType, 0, ""
	case "":
		return "", http.StatusBadRequest, "release type is required"
	default:
		return "", http.StatusBadRequest, "release_type must be album, ep, or single"
	}
}

func validateReleaseDate(value string) (string, int, int, string) {
	releaseDateRaw := strings.TrimSpace(value)
	if releaseDateRaw == "" {
		return "", 0, http.StatusBadRequest, "release date is required"
	}
	parsedDate, err := time.Parse("2006-01-02", releaseDateRaw)
	if err != nil {
		return "", 0, http.StatusBadRequest, "release_date must be in YYYY-MM-DD format"
	}
	return releaseDateRaw, parsedDate.Year(), 0, ""
}

func normalizeGenreGroup(value string) string {
	newArtistGenre := strings.TrimSpace(value)
	if newArtistGenre == "" {
		return "rock_metal"
	}
	if isValidGenreGroup(newArtistGenre) {
		return newArtistGenre
	}
	return "rock_metal"
}

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

// HandleCreateSong creates a new song record and updates the page via Datastar SSE or JSON.
func (h *Handler) HandleCreateSong(w http.ResponseWriter, r *http.Request) {
	input, status, errMsg := validateSongForm(r)
	if errMsg != "" {
		if wantsJSONResponse(r) {
			writeError(w, status, errMsg)
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.AddSongErrorNotice(errMsg))
		return
	}

	artists, code, err := h.resolveArtistNames(r.Context(), input.ArtistSpotifyIDs)
	if err != nil {
		if code >= http.StatusInternalServerError {
			log.Printf("[handleCreateSong] resolveArtistNames failed: %v", err)
			errMsg = "failed to resolve artist metadata"
		} else {
			errMsg = err.Error()
		}
		if wantsJSONResponse(r) {
			writeError(w, code, errMsg)
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.AddSongErrorNotice(errMsg))
		return
	}

	record, saveErr := h.persistSongWithMetadata(r.Context(), input, artists)
	if saveErr != nil {
		if wantsJSONResponse(r) {
			writeError(w, saveErr.status, saveErr.Error())
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.AddSongErrorNotice(saveErr.Error()))
		return
	}

	if h.songRepo != nil {
		if songErr := h.recordSongCreatedEvent(r.Context(), record.song); songErr != nil {
			log.Printf("[handleCreateSong] recordSongCreatedEvent error for %s: %v", record.song.Id, songErr)
		}
	}

	// Fold the PB side effects (album count bump/creation, artist count
	// bump/creation) as facts too. Best-effort like the song event above:
	// the transaction already committed, so failures only log — audit
	// flags rows without streams for convergence on the next write.
	if sideErr := h.recordSongSideEffectEvents(r.Context(), record.album, record.artists); sideErr != nil {
		log.Printf("[handleCreateSong] recordSongSideEffectEvents error: %v", sideErr)
	}

	playlistSort := normalizePlaylistSort(r.URL.Query().Get("playlist_sort"))
	ctx := r.Context()
	pageData, buildErr := h.buildSongPageData(ctx, playlistSort)
	if buildErr != nil {
		log.Printf("[handleCreateSong] buildSongPageData failed: %v", buildErr)
		_ = writeJSON(w, http.StatusOK, map[string]any{
			"id":    record.song.Id,
			"title": record.song.GetString("title"),
		})
		return
	}

	sse := datastar.NewSSE(w, r, sseOpts...)

	// 1. Patch the updated songs sections
	if err := sse.PatchElementTempl(templates.SongsSections(
		pageData.CurrentPlaylist,
		pageData.WaitingRemoval,
		pageData.NotRecentCount,
		pageData.PlaylistSort,
	)); err != nil {
		log.Printf("[handleCreateSong] patch songs sections failed: %v", err)
		return
	}

	// 2. Patch success feedback notice into the modal
	if err := sse.PatchElementTempl(templates.AddSongSuccessNotice(record.song.GetString("title"))); err != nil {
		log.Printf("[handleCreateSong] patch song feedback failed: %v", err)
		return
	}

	// 3. Reset form cleanly via morphing
	_ = sse.PatchElementTempl(templates.AddSongForm())
}

func (h *Handler) handleCreateSong(e *core.RequestEvent) error {
	h.HandleCreateSong(e.Response, e.Request)
	return nil
}

// recordSongCreatedEvent appends the SongCreated event for a persisted song.
func (h *Handler) recordSongCreatedEvent(ctx context.Context, record *core.Record) error {
	agg, err := song.NewSong(record.Id, record.GetString("title"), record.GetString("artist_name"),
		record.GetString("album"), record.GetString("release_date"), record.GetString("release_type"),
		record.GetString("spotify_id"), int64(record.GetInt("release_year")),
		int64(record.GetInt("recent_batch_seq")), int64(record.GetInt("recent_batch_pos")),
		record.GetBool("is_recent"))
	if err != nil {
		return err
	}
	events, err := h.songRepo.Save(ctx, agg)
	if err != nil {
		return err
	}
	if h.catalogProjection != nil && len(events) > 0 {
		return h.catalogProjection.Project(ctx, song.StreamTypeSong, events)
	}
	return nil
}

// recordSongSideEffectEvents folds song creation's PocketBase side effects as
// facts: the album count bump (or creation) and each artist's count bump (or
// creation). Joined with recordSongCreatedEvent, the full creation — song,
// album delta, artist deltas — is replayable from the log, closing the last
// PB-only write path in the main web flow. Best-effort: the PB transaction
// already committed, so each item logs and continues; the first error is
// returned for the caller's warning log.
func (h *Handler) recordSongSideEffectEvents(ctx context.Context, albumRec *core.Record, artistRecs []*core.Record) error {
	var firstErr error
	fail := func(err error) {
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}

	if albumRec != nil {
		if h.albumRepo == nil {
			log.Printf("[handleCreateSong] albumRepo not configured, skipping album event for %s", albumRec.Id)
		} else if err := h.recordAlbumCountAdjusted(ctx, albumRec); err != nil {
			log.Printf("[handleCreateSong] album side-effect event error for %s: %v", albumRec.Id, err)
			fail(err)
		}
	}
	if h.artistRepo == nil && len(artistRecs) > 0 {
		log.Printf("[handleCreateSong] artistRepo not configured, skipping %d artist event(s)", len(artistRecs))
	}
	for _, artistRec := range artistRecs {
		if artistRec == nil || h.artistRepo == nil {
			continue
		}
		if err := h.recordArtistCountAdjusted(ctx, artistRec); err != nil {
			log.Printf("[handleCreateSong] artist side-effect event error for %s: %v", artistRec.Id, err)
			fail(err)
		}
	}
	return firstErr
}

// recordAlbumCountAdjusted loads (seeding legacy PB-only rows) and folds the
// album's committed post-state counts as an AlbumSongCountsAdjusted fact.
func (h *Handler) recordAlbumCountAdjusted(ctx context.Context, record *core.Record) error {
	return h.foldAlbumCounts(ctx, albumView{
		id:              record.Id,
		title:           record.GetString("title"),
		artistName:      record.GetString("artist_name"),
		status:          record.GetString("status"),
		collectionSongs: max(int64(record.GetInt("collection_songs")), 0),
		totalSongs:      max(int64(record.GetInt("total_songs")), 0),
	})
}

// albumView is the post-state needed to fold album count facts without a
// PocketBase record (tests build it directly).
type albumView struct {
	id              string
	title           string
	artistName      string
	status          string
	collectionSongs int64
	totalSongs      int64
}

func (h *Handler) foldAlbumCounts(ctx context.Context, view albumView) error {
	agg, err := h.loadOrCreateAlbum(ctx, view)
	if err != nil {
		return err
	}
	if err := agg.AdjustSongCounts(view.collectionSongs, view.totalSongs); err != nil {
		return err
	}
	return h.saveAndProjectAlbum(ctx, agg)
}

// loadOrCreateAlbum replays the album stream, seeding an AlbumCreated fact
// from the committed post-state for legacy rows that predate the event log.
func (h *Handler) loadOrCreateAlbum(ctx context.Context, view albumView) (*album.Album, error) {
	agg, loadErr := h.albumRepo.Load(ctx, view.id)
	if !errors.Is(loadErr, eventsourcing.ErrStreamNotFound) {
		return agg, loadErr
	}
	seed, seedErr := album.NewAlbum(view.id, view.title, view.artistName, view.status, view.collectionSongs, view.totalSongs)
	if seedErr != nil {
		return nil, seedErr
	}
	if err := h.saveAndProjectAlbum(ctx, seed); err != nil {
		return nil, err
	}
	return h.albumRepo.Load(ctx, view.id)
}

// recordArtistCountAdjusted loads (seeding legacy PB-only rows) and folds the
// artist's committed collection_songs count as an
// ArtistCollectionSongsAdjusted fact. The rank column (total_songs) stays
// derived: existing streams keep their logged total, only backfill seeds
// carry the row's current value for immediate convergence.
func (h *Handler) recordArtistCountAdjusted(ctx context.Context, record *core.Record) error {
	return h.foldArtistCounts(ctx, artistView{
		id:              record.Id,
		name:            record.GetString("name"),
		spotifyID:       record.GetString("spotify_id"),
		genreGroup:      record.GetString("genre_group"),
		listStatus:      record.GetString("list_status"),
		collectionSongs: max(int64(record.GetInt("collection_songs")), 0),
	})
}

// artistView is the post-state needed to fold artist count facts without a
// PocketBase record (tests build it directly).
type artistView struct {
	id              string
	name            string
	spotifyID       string
	genreGroup      string
	listStatus      string
	collectionSongs int64
}

func (h *Handler) foldArtistCounts(ctx context.Context, view artistView) error {
	agg, _, err := h.loadOrCreateArtist(ctx, view)
	if err != nil {
		return err
	}
	if err := agg.AdjustCollectionSongs(view.collectionSongs, agg.TotalSongs); err != nil {
		return err
	}
	h.persistArtist(ctx, view.id, agg)
	return nil
}

// loadOrCreateArtist replays the artist stream, seeding a creation fact for
// legacy PB-only rows. Counts start at zero in the stream: collection
// advances with the first adjustment, and rank (total_songs) is never logged.
func (h *Handler) loadOrCreateArtist(ctx context.Context, view artistView) (*artist.Artist, bool, error) {
	agg, loadErr := h.artistRepo.Load(ctx, view.id)
	if loadErr == nil {
		return agg, false, nil
	}
	if !errors.Is(loadErr, eventsourcing.ErrStreamNotFound) {
		return nil, false, loadErr
	}
	seed, seedErr := artist.NewArtist(view.id, view.name, view.spotifyID, view.genreGroup, view.listStatus)
	if seedErr != nil {
		return nil, false, seedErr
	}
	events, err := h.artistRepo.Save(ctx, seed)
	if err != nil {
		return nil, false, err
	}
	if h.artistProjection != nil && len(events) > 0 {
		if err := h.artistProjection.Project(ctx, seed, events); err != nil {
			return nil, false, err
		}
	}
	agg, err = h.artistRepo.Load(ctx, view.id)
	if err != nil {
		return nil, false, err
	}
	return agg, true, nil
}

func (h *Handler) persistSongWithMetadata(ctx context.Context, input songFormInput, artists []string) (*songPersisted, *songSaveError) {
	if len(artists) == 0 {
		return nil, &songSaveError{http.StatusBadRequest, "at least one artist is required"}
	}

	persisted := &songPersisted{}
	var newArtistsToQueue []songNewArtistTarget

	txErr := h.app.RunInTransaction(func(txApp core.App) error {
		albumRec, err := h.upsertAlbumForSong(txApp, albumUpsertParams{
			AlbumName:     input.AlbumName,
			PrimaryArtist: artists[0],
			ReleaseType:   input.ReleaseType,
			TotalSongs:    input.TotalSongsOnAlbum,
		})
		if err != nil {
			log.Printf("[handleCreateSong] upsertAlbumForSong failed: %v", err)
			return fmt.Errorf("failed to update album metadata: %w", err)
		}
		persisted.album = albumRec

		newArtists, touched, err := h.upsertArtistsForSong(txApp, artists, input.ArtistSpotifyIDs, input.NewArtistGenre)
		if err != nil {
			log.Printf("[handleCreateSong] upsertArtistsForSong failed: %v", err)
			return fmt.Errorf("failed to update artist metadata: %w", err)
		}
		newArtistsToQueue = newArtists
		persisted.artists = touched

		songRecord, err := h.createSongRecord(ctx, txApp, input, artists)
		if err != nil {
			log.Printf("[handleCreateSong] createSongRecord failed: %v", err)
			return fmt.Errorf("failed to create song record: %w", err)
		}
		persisted.song = songRecord
		return nil
	})

	if txErr != nil {
		log.Printf("[handleCreateSong] persistSongWithMetadata failed: %v", txErr)
		return nil, &songSaveError{http.StatusInternalServerError, "failed to save song"}
	}

	for _, target := range newArtistsToQueue {
		queueCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		queueErr := h.queueArtistRefreshFromSong(queueCtx, target)
		cancel()
		if queueErr != nil {
			log.Printf("[handleCreateSong] Warning: failed to queue refresh for new artist %s (%s): %v", target.Name, target.ID, queueErr)
		}
	}

	return persisted, nil
}

// songPersisted carries a song creation's committed PocketBase rows so
// follow-up event appends fold the same post-state as facts.
type songPersisted struct {
	song    *core.Record
	album   *core.Record
	artists []*core.Record
}

type songSaveError struct {
	status int
	msg    string
}

func (e *songSaveError) Error() string { return e.msg }

func (h *Handler) createSongRecord(ctx context.Context, txApp core.App, input songFormInput, artists []string) (*core.Record, error) {
	collection, err := txApp.FindCollectionByNameOrId("songs")
	if err != nil {
		return nil, &songSaveError{http.StatusInternalServerError, "songs collection not found"}
	}

	batchSeq, batchPos, err := h.nextRecentBatchAssignmentWithApp(ctx, txApp, time.Now())
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

	if err := txApp.Save(record); err != nil {
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
		if seen[spotifyID] {
			continue
		}
		seen[spotifyID] = true
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
		if !isBase62Char(c) {
			return false
		}
	}
	return true
}

func isBase62Char(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func (h *Handler) inferArtistNameFromSpotifyID(ctx context.Context, spotifyID string) (string, int, error) {
	name, ok, err := h.lookupArtistLocally(ctx, spotifyID)
	if err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("failed to lookup artist locally: %w", err)
	}
	if ok {
		return name, 0, nil
	}
	return h.fetchArtistNameFromSpotify(ctx, spotifyID)
}

func (h *Handler) lookupArtistLocally(ctx context.Context, spotifyID string) (string, bool, error) {
	records := make([]*core.Record, 0)
	err := h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp("spotify_id = {:spotify_id}", dbx.Params{"spotify_id": spotifyID})).
		Limit(1).
		All(&records)
	if err != nil {
		return "", false, err
	}
	if len(records) == 0 {
		return "", false, nil
	}
	name := records[0].GetString("name")
	if name == "" {
		return "", false, nil
	}
	log.Printf("[handleCreateSong] resolved artist %q from PocketBase (spotify_id=%s)", name, spotifyID)
	return name, true, nil
}

func (h *Handler) fetchArtistNameFromSpotify(ctx context.Context, spotifyID string) (string, int, error) {
	endpoint := "https://open.spotify.com/oembed?url=" + url.QueryEscape("spotify:artist:"+spotifyID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to create spotify request: %w", err)
	}

	if h.httpClient == nil {
		return "", http.StatusInternalServerError, fmt.Errorf("spotify http client is not configured")
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to reach spotify: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	return decodeSpotifyArtistName(resp)
}

func decodeSpotifyArtistName(resp *http.Response) (string, int, error) {
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
		return "", http.StatusBadGateway, fmt.Errorf("could not infer artist name from spotify response: %w", err)
	}

	artistName := strings.TrimSpace(payload.Title)
	if artistName == "" {
		return "", http.StatusBadGateway, fmt.Errorf("could not infer artist name from spotify response")
	}

	return artistName, 0, nil
}

type albumUpsertParams struct {
	AlbumName     string
	PrimaryArtist string
	ReleaseType   string
	TotalSongs    int
}

func (h *Handler) upsertAlbumForSong(txApp core.App, p albumUpsertParams) (*core.Record, error) {
	filter := "title = {:title} && artist_name = {:artist_name}"
	params := dbx.Params{"title": p.AlbumName, "artist_name": p.PrimaryArtist}
	if p.ReleaseType != "" {
		filter += " && (release_type = {:release_type} || release_type = '')"
		params["release_type"] = p.ReleaseType
	}
	records, err := txApp.FindRecordsByFilter(
		"albums", filter, "", 1, 0, params,
	)
	if err != nil {
		return nil, err
	}
	if len(records) > 0 {
		mutateAlbumRecord(records[0], p)
		if err := txApp.Save(records[0]); err != nil {
			return nil, err
		}
		return records[0], nil
	}
	return h.createAlbumRecord(txApp, p)
}

func mutateAlbumRecord(record *core.Record, p albumUpsertParams) {
	collectionSongs := record.GetInt("collection_songs") + 1
	record.Set("collection_songs", collectionSongs)
	existingTotal := record.GetInt("total_songs")
	if p.TotalSongs > existingTotal {
		record.Set("total_songs", p.TotalSongs)
	} else if collectionSongs > existingTotal {
		record.Set("total_songs", collectionSongs)
	}
	if record.GetString("release_type") == "" && p.ReleaseType != "" {
		record.Set("release_type", p.ReleaseType)
	}
}

func (h *Handler) createAlbumRecord(txApp core.App, p albumUpsertParams) (*core.Record, error) {
	collection, err := txApp.FindCollectionByNameOrId("albums")
	if err != nil {
		return nil, err
	}

	newTotal := max(p.TotalSongs, 1)

	record := core.NewRecord(collection)
	record.Set("title", p.AlbumName)
	record.Set("artist_name", p.PrimaryArtist)
	record.Set("collection_songs", 1)
	record.Set("total_songs", newTotal)
	record.Set("release_type", p.ReleaseType)
	record.Set("status", "waiting")
	if err := txApp.Save(record); err != nil {
		return nil, err
	}
	return record, nil
}

type songNewArtistTarget struct {
	ID        string
	Name      string
	SpotifyID string
}

func (h *Handler) upsertArtistsForSong(txApp core.App, artists []string, artistSpotifyIDs []string, newArtistGenre string) ([]songNewArtistTarget, []*core.Record, error) {
	if len(artists) != len(artistSpotifyIDs) {
		return nil, nil, fmt.Errorf("artists and artistSpotifyIDs length mismatch: %d vs %d", len(artists), len(artistSpotifyIDs))
	}

	results := make([]songNewArtistTarget, 0, len(artists))
	touched := make([]*core.Record, 0, len(artists))
	for i, artistName := range artists {
		target, record, isNew, err := h.findOrCreateArtist(txApp, artistName, artistSpotifyIDs[i], newArtistGenre)
		if err != nil {
			return nil, nil, err
		}
		if record != nil {
			touched = append(touched, record)
		}
		if isNew {
			results = append(results, target)
		}
	}
	return results, touched, nil
}

func (h *Handler) lookupArtistRecord(txApp core.App, artistName, artistSpotifyID string) ([]*core.Record, error) {
	artistSpotifyID = strings.TrimSpace(artistSpotifyID)

	if artistSpotifyID != "" {
		records, err := txApp.FindRecordsByFilter(
			"artists", "spotify_id = {:spotify_id}", "", 1, 0,
			dbx.Params{"spotify_id": artistSpotifyID},
		)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 {
			return records, nil
		}
		return []*core.Record{}, nil
	}

	return txApp.FindRecordsByFilter(
		"artists", "name = {:name}", "", 1, 0,
		dbx.Params{"name": artistName},
	)
}

func mutateArtistRecord(record *core.Record, artistSpotifyID string) {
	record.Set("collection_songs", record.GetInt("collection_songs")+1)
	if record.GetString("spotify_id") == "" {
		record.Set("spotify_id", artistSpotifyID)
	}
}

func (h *Handler) findOrCreateArtist(txApp core.App, artistName, artistSpotifyID, newArtistGenre string) (songNewArtistTarget, *core.Record, bool, error) {
	records, err := h.lookupArtistRecord(txApp, artistName, artistSpotifyID)
	if err != nil {
		return songNewArtistTarget{}, nil, false, err
	}

	if len(records) > 0 {
		mutateArtistRecord(records[0], artistSpotifyID)
		if err := txApp.Save(records[0]); err != nil {
			return songNewArtistTarget{}, nil, false, err
		}
		return songNewArtistTarget{}, records[0], false, nil
	}

	collection, err := txApp.FindCollectionByNameOrId("artists")
	if err != nil {
		return songNewArtistTarget{}, nil, false, err
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
	if err := txApp.Save(record); err != nil {
		return songNewArtistTarget{}, nil, false, err
	}
	return songNewArtistTarget{ID: record.Id, Name: artistName, SpotifyID: artistSpotifyID}, record, true, nil
}

type rollbackState struct {
	Record              *core.Record
	PreviousFetchStatus string
	RequestID           string
	ArtistID            string
}

func (h *Handler) deleteScrapeJobRecordByRequestID(ctx context.Context, requestID, artistID string) error {
	requestID = strings.TrimSpace(requestID)
	artistID = strings.TrimSpace(artistID)
	if requestID == "" || artistID == "" {
		return nil
	}

	records := make([]*core.Record, 0, 1)
	err := h.app.RecordQuery("scrape_jobs").
		WithContext(ctx).
		AndWhere(dbx.NewExp("request_id = {:request_id} AND artist = {:artist}", dbx.Params{
			"request_id": requestID,
			"artist":     artistID,
		})).
		Limit(1).
		All(&records)
	if err != nil {
		return fmt.Errorf("query scrape job for rollback: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	if err := h.app.Delete(records[0]); err != nil {
		return fmt.Errorf("delete scrape job %s: %w", records[0].Id, err)
	}
	return nil
}

func (h *Handler) rollbackSongArtistRefreshQueue(ctx context.Context, rb rollbackState) error {
	correlation.Clear(rb.ArtistID)

	cleanupFailures := make([]string, 0, 2)
	if err := h.deleteScrapeJobRecordByRequestID(ctx, rb.RequestID, rb.ArtistID); err != nil {
		cleanupFailures = append(cleanupFailures, fmt.Sprintf("delete scrape job: %v", err))
	}
	if err := h.unmarkArtistRefreshQueued(ctx, rb.Record, rb.PreviousFetchStatus, rb.RequestID); err != nil {
		cleanupFailures = append(cleanupFailures, fmt.Sprintf("restore artist fetch_status: %v", err))
	}
	if len(cleanupFailures) > 0 {
		return fmt.Errorf("%s", strings.Join(cleanupFailures, "; "))
	}

	return nil
}

func (h *Handler) queueArtistRefreshFromSong(ctx context.Context, target songNewArtistTarget) error {
	if target.ID == "" || target.SpotifyID == "" {
		return nil
	}

	requestID := newScrapeRequestID()
	record, err := h.findArtistRecordForRefresh(ctx, target.ID)
	if err != nil {
		return err
	}

	rb := rollbackState{
		Record:              record,
		PreviousFetchStatus: record.GetString("fetch_status"),
		RequestID:           requestID,
		ArtistID:            target.ID,
	}

	if err := h.markArtistRefreshPending(ctx, record, requestID); err != nil {
		return err
	}

	// Audit first like the other queue paths: open the job stream so a crash
	// leaves a fact to reconcile instead of a phantom row.
	h.appendJobRequested(ctx, requestID, target.ID, commands.TypeRefresh)

	correlation.Associate(target.ID, requestID)
	if err := h.createScrapeJobRecord(ctx, requestID, target.ID); err != nil {
		return h.rollbackOnQueueFailure(rb, "create scrape job record", err)
	}

	return h.publishArtistRefreshRequest(ctx, target, requestID, rb)
}

func (h *Handler) findArtistRecordForRefresh(ctx context.Context, artistID string) (*core.Record, error) {
	record, err := h.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("queueArtistRefreshFromSong: find artist %s: %w", artistID, err)
	}
	return record, nil
}

func (h *Handler) publishArtistRefreshRequest(ctx context.Context, target songNewArtistTarget, requestID string, rb rollbackState) error {
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
		h.appendJobTransition(ctx, requestID, target.ID, func(j *scrapejob.Job) error {
			_, _, err := j.RecordFailed("publish_failed", eventsourcing.Correlation{RequestID: requestID})
			return err
		})
		return h.rollbackOnQueueFailure(rb, "publish scrape request", err)
	}
	if ack != nil && ack.Duplicate {
		return h.rollbackOnDuplicate(rb)
	}

	return nil
}

func (h *Handler) markArtistRefreshPending(ctx context.Context, record *core.Record, requestID string) error {
	// Event first with saga correlation so aggregate state converges with the row.
	h.emitArtistFetchStatus(ctx, record.Id, "pending", "song queue refresh", requestID)
	record.Set("fetch_status", "pending")
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return fmt.Errorf("queueArtistRefreshFromSong: mark artist pending: %w", err)
	}
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

func (h *Handler) rollbackOnQueueFailure(rb rollbackState, step string, origErr error) error {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if rollbackErr := h.rollbackSongArtistRefreshQueue(rollbackCtx, rb); rollbackErr != nil {
		return fmt.Errorf("queueArtistRefreshFromSong: %s: %w (cleanup failed: %v)", step, origErr, rollbackErr)
	}
	return fmt.Errorf("queueArtistRefreshFromSong: %s: %w", step, origErr)
}

func (h *Handler) rollbackOnDuplicate(rb rollbackState) error {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.rollbackSongArtistRefreshQueue(rollbackCtx, rb); err != nil {
		return fmt.Errorf("queueArtistRefreshFromSong: duplicate scrape request cleanup failed: %w", err)
	}
	return nil
}
