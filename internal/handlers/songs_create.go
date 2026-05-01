//go:build goexperiment.jsonv2

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

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
		TotalSongsOnAlbum: parseTotalSongs(r),
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

func (h *Handler) handleCreateSong(e *core.RequestEvent) error {
	input, status, errMsg := validateSongForm(e.Request)
	if errMsg != "" {
		return e.JSON(status, map[string]string{"error": errMsg})
	}

	artists, code, err := h.resolveArtistNames(e.Request.Context(), input.ArtistSpotifyIDs)
	if err != nil {
		return e.JSON(code, map[string]string{"error": err.Error()})
	}

	record, saveErr := h.persistSongWithMetadata(e.Request.Context(), input, artists)
	if saveErr != nil {
		return e.JSON(saveErr.status, map[string]string{"error": saveErr.Error()})
	}

	playlistSort := normalizePlaylistSort(e.Request.URL.Query().Get("playlist_sort"))
	ctx := e.Request.Context()
	pageData, buildErr := h.buildSongPageData(ctx, playlistSort)
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

func (h *Handler) persistSongWithMetadata(ctx context.Context, input songFormInput, artists []string) (*core.Record, *songSaveError) {
	if len(artists) == 0 {
		return nil, &songSaveError{http.StatusBadRequest, "at least one artist is required"}
	}
	if err := h.upsertAlbumForSong(albumUpsertParams{
		AlbumName:     input.AlbumName,
		PrimaryArtist: artists[0],
		ReleaseType:   input.ReleaseType,
		TotalSongs:    input.TotalSongsOnAlbum,
	}); err != nil {
		log.Printf("[handleCreateSong] upsertAlbumForSong failed: %v", err)
		return nil, &songSaveError{http.StatusInternalServerError, "failed to update album metadata"}
	}

	newArtists, err := h.upsertArtistsForSong(artists, input.ArtistSpotifyIDs, input.NewArtistGenre)
	if err != nil {
		log.Printf("[handleCreateSong] upsertArtistsForSong failed: %v", err)
		return nil, &songSaveError{http.StatusInternalServerError, "failed to update artist metadata"}
	}
	for _, target := range newArtists {
		if queueErr := h.queueArtistRefreshFromSong(ctx, target); queueErr != nil {
			log.Printf("[handleCreateSong] Warning: failed to queue refresh for new artist %s (%s): %v", target.Name, target.ID, queueErr)
		}
	}

	return h.createSongRecord(ctx, input, artists)
}

type songSaveError struct {
	status int
	msg    string
}

func (e *songSaveError) Error() string { return e.msg }

func (h *Handler) createSongRecord(ctx context.Context, input songFormInput, artists []string) (*core.Record, *songSaveError) {
	collection, err := h.app.FindCollectionByNameOrId("songs")
	if err != nil {
		return nil, &songSaveError{http.StatusInternalServerError, "songs collection not found"}
	}
	batchSeq, batchPos, err := h.nextRecentBatchAssignment(ctx, time.Now())
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
	if name, ok := h.lookupArtistLocally(spotifyID); ok {
		return name, 0, nil
	}
	return h.fetchArtistNameFromSpotify(ctx, spotifyID)
}

func (h *Handler) lookupArtistLocally(spotifyID string) (string, bool) {
	records, err := h.app.FindRecordsByFilter(
		"artists", "spotify_id = {:spotify_id}", "", 1, 0,
		dbx.Params{"spotify_id": spotifyID},
	)
	if err != nil || len(records) == 0 {
		return "", false
	}
	name := records[0].GetString("name")
	if name == "" {
		return "", false
	}
	log.Printf("[handleCreateSong] resolved artist %q from PocketBase (spotify_id=%s)", name, spotifyID)
	return name, true
}

func (h *Handler) fetchArtistNameFromSpotify(ctx context.Context, spotifyID string) (string, int, error) {
	endpoint := "https://open.spotify.com/oembed?url=" + url.QueryEscape("spotify:artist:"+spotifyID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", http.StatusBadGateway, fmt.Errorf("failed to infer artist name from spotify_id")
	}

	resp, err := h.httpClient.Do(req)
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

type albumUpsertParams struct {
	AlbumName     string
	PrimaryArtist string
	ReleaseType   string
	TotalSongs    int
}

func (h *Handler) upsertAlbumForSong(p albumUpsertParams) error {
	filter := "title = {:title} && artist_name = {:artist_name}"
	params := dbx.Params{"title": p.AlbumName, "artist_name": p.PrimaryArtist}
	if p.ReleaseType != "" {
		filter += " && release_type = {:release_type}"
		params["release_type"] = p.ReleaseType
	}
	records, err := h.app.FindRecordsByFilter(
		"albums",
		filter,
		"", 1, 0,
		params,
	)
	if err != nil {
		return err
	}
	if len(records) > 0 {
		return h.updateExistingAlbum(records[0], p)
	}
	return h.createAlbumRecord(p)
}

func (h *Handler) updateExistingAlbum(record *core.Record, p albumUpsertParams) error {
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
	return h.app.Save(record)
}

func (h *Handler) createAlbumRecord(p albumUpsertParams) error {
	collection, err := h.app.FindCollectionByNameOrId("albums")
	if err != nil {
		return err
	}
	newTotal := p.TotalSongs
	if newTotal < 1 {
		newTotal = 1
	}
	record := core.NewRecord(collection)
	record.Set("title", p.AlbumName)
	record.Set("artist_name", p.PrimaryArtist)
	record.Set("collection_songs", 1)
	record.Set("total_songs", newTotal)
	record.Set("release_type", p.ReleaseType)
	record.Set("status", "waiting")
	return h.app.Save(record)
}

type songNewArtistTarget struct {
	ID        string
	Name      string
	SpotifyID string
}

func (h *Handler) upsertArtistsForSong(artists []string, artistSpotifyIDs []string, newArtistGenre string) ([]songNewArtistTarget, error) {
	if len(artists) != len(artistSpotifyIDs) {
		return nil, fmt.Errorf("artists and artistSpotifyIDs length mismatch: %d vs %d", len(artists), len(artistSpotifyIDs))
	}
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
	h.createScrapeJobRecord(ctx, requestID, target.ID)

	record, err := h.app.FindRecordById("artists", target.ID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if err != nil {
		return err
	}
	record.Set("fetch_status", "pending")
	if err := h.app.SaveWithContext(ctx, record); err != nil {
		return err
	}

	return nil
}
