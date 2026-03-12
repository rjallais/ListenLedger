//go:build goexperiment.jsonv2

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/songbackfill"
)

type report struct {
	GeneratedAt       time.Time                 `json:"generated_at"`
	ApplyRequested    bool                      `json:"apply_requested"`
	MinimumConfidence float64                   `json:"minimum_confidence"`
	ReportPath        string                    `json:"report_path,omitempty"`
	ReviewQueueJSON   string                    `json:"review_queue_json,omitempty"`
	ReviewQueueCSV    string                    `json:"review_queue_csv,omitempty"`
	Summary           reportSummary             `json:"summary"`
	Resolutions       []songbackfill.Resolution `json:"resolutions"`
}

type reportSummary struct {
	SongsScanned      int `json:"songs_scanned"`
	UpdateCandidates  int `json:"update_candidates"`
	BelowThreshold    int `json:"below_threshold"`
	UpdatesApplied    int `json:"updates_applied"`
	ArtistNameChanges int `json:"artist_name_changes"`
	SkippedAmbiguous  int `json:"skipped_ambiguous"`
	SkippedUnresolved int `json:"skipped_unresolved"`
}

type priorReportSummary struct {
	GeneratedAt time.Time                 `json:"generated_at"`
	Resolutions []songbackfill.Resolution `json:"resolutions"`
}

type resolutionHints struct {
	Category      string
	Priority      int
	RetryEligible bool
	Notes         []string
}

var trailingDotEllipsisPattern = regexp.MustCompile(`(?i)(?:,\s*|\s+)(\.{2,})\s*$`)

func main() {
	defaultDataDir := appdir.ResolveDataDir()
	dataDir := flag.String("data-dir", defaultDataDir, "PocketBase data directory to read/write")
	apply := flag.Bool("apply", false, "Persist approved updates (default is dry-run)")
	limit := flag.Int("limit", 0, "Maximum number of songs to inspect (0 for all)")
	reportDir := flag.String("report-dir", "", "Directory for JSON audit reports (defaults to <data-dir>/backfill_reports)")
	minConfidence := flag.Float64("min-confidence", 0.90, "Minimum confidence required before writing a song update")
	httpTimeout := flag.Duration("http-timeout", 15*time.Second, "Timeout for external metadata lookups")
	tidalTokenURL := flag.String("tidal-token-url", "https://auth.tidal.com/v1/oauth2/token", "OAuth token endpoint for TIDAL client-credentials flow")
	tidalBase := flag.String("tidal-base", "https://openapi.tidal.com/v2", "Base URL for TIDAL searchResults lookups")
	tidalCountryCode := flag.String("tidal-country-code", "US", "Country code for TIDAL searchResults lookups")
	tidalToken := flag.String("tidal-token", "", "Bearer token for TIDAL OpenAPI requests (or set TIDAL_TOKEN)")
	tidalClientID := flag.String("tidal-client-id", "", "TIDAL client ID for client-credentials auth (or set TIDAL_CLIENT_ID)")
	tidalClientSecret := flag.String("tidal-client-secret", "", "TIDAL client secret for client-credentials auth (or set TIDAL_CLIENT_SECRET)")
	musicBrainzBase := flag.String("musicbrainz-base", "https://musicbrainz.org", "Base URL for MusicBrainz recording lookups")
	deezerBase := flag.String("deezer-base", "https://api.deezer.com", "Base URL for Deezer track lookups")
	retryFromLatestReport := flag.Bool("retry-from-latest-report", true, "Prefer songs that the most recent backfill report indicates are worth retrying")
	flag.Parse()

	if strings.TrimSpace(*reportDir) == "" {
		*reportDir = filepath.Join(*dataDir, "backfill_reports")
	}

	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDataDir: *dataDir,
	})

	if err := app.Bootstrap(); err != nil {
		log.Fatalf("[backfill_song_artists] bootstrap failed: %v (stop the live app first or point --data-dir at a backup copy)", err)
	}

	songs, err := loadSongs(app)
	if err != nil {
		log.Fatalf("[backfill_song_artists] failed to load songs: %v", err)
	}

	artists, err := loadArtists(app)
	if err != nil {
		log.Fatalf("[backfill_song_artists] failed to load artists: %v", err)
	}

	if *limit > 0 && len(songs) > *limit {
		songs = songs[:*limit]
	}

	if *retryFromLatestReport {
		if plannedSongs, latestReportPath, err := planSongsFromLatestReport(*reportDir, songs); err != nil {
			log.Printf("[backfill_song_artists] warning: failed to use latest report for retry planning: %v", err)
		} else if latestReportPath != "" {
			songs = plannedSongs
			log.Printf("[backfill_song_artists] prioritized %d songs using latest report %s", len(songs), latestReportPath)
		}
	}

	log.Printf("[backfill_song_artists] loaded %d songs with empty artist_spotify_ids and %d artists with spotify_id", len(songs), len(artists))
	if len(songs) == 0 {
		log.Printf("[backfill_song_artists] nothing to do")
		return
	}

	httpClient := &http.Client{Timeout: *httpTimeout}
	resolvedTidalToken := strings.TrimSpace(*tidalToken)
	if resolvedTidalToken == "" {
		resolvedTidalToken = strings.TrimSpace(os.Getenv("TIDAL_TOKEN"))
	}
	resolvedTidalClientID := strings.TrimSpace(*tidalClientID)
	if resolvedTidalClientID == "" {
		resolvedTidalClientID = strings.TrimSpace(os.Getenv("TIDAL_CLIENT_ID"))
	}
	resolvedTidalClientSecret := strings.TrimSpace(*tidalClientSecret)
	if resolvedTidalClientSecret == "" {
		resolvedTidalClientSecret = strings.TrimSpace(os.Getenv("TIDAL_CLIENT_SECRET"))
	}
	if resolvedTidalToken == "" && resolvedTidalClientID != "" && resolvedTidalClientSecret != "" {
		ctx, cancel := context.WithTimeout(context.Background(), *httpTimeout)
		var err error
		resolvedTidalToken, err = fetchTidalAccessToken(ctx, httpClient, *tidalTokenURL, resolvedTidalClientID, resolvedTidalClientSecret)
		cancel()
		if err != nil {
			log.Printf("[backfill_song_artists] warning: failed to obtain TIDAL access token: %v", err)
		} else {
			log.Printf("[backfill_song_artists] obtained TIDAL access token via client credentials")
		}
	}
	resolver := songbackfill.NewResolver(artists, songbackfill.Options{
		NamePrefillLookup: &songbackfill.TidalLookup{
			BaseURL:     *tidalBase,
			CountryCode: *tidalCountryCode,
			HTTPClient:  httpClient,
			UserAgent:   "ListenLedger/1.0 (song artist backfill)",
			AuthToken:   resolvedTidalToken,
		},
		TrackLookup: songbackfill.ChainTrackLookup{Lookups: []songbackfill.TrackMetadataLookup{
			&songbackfill.MusicBrainzLookup{
				BaseURL:    *musicBrainzBase,
				HTTPClient: httpClient,
				UserAgent:  "ListenLedger/1.0 (song artist backfill)",
			},
			&songbackfill.DeezerLookup{
				BaseURL:    *deezerBase,
				HTTPClient: httpClient,
				UserAgent:  "ListenLedger/1.0 (song artist backfill)",
			},
		}},
		MinimumConfidence: *minConfidence,
	})

	resolutions := make([]songbackfill.Resolution, 0, len(songs))
	applied := 0
	artistNameChanges := 0

	for _, song := range songs {
		song = prepareSongForResolution(song)
		ctx, cancel := context.WithTimeout(context.Background(), *httpTimeout)
		resolution := resolver.Resolve(ctx, song)
		cancel()

		resolutions = append(resolutions, resolution)
		if !resolution.Approved(*minConfidence) && !resolution.NamePrefillApproved(*minConfidence) {
			continue
		}

		if resolution.UpdatedArtistName != resolution.OriginalArtistName {
			artistNameChanges++
		}

		if !*apply {
			continue
		}

		record, err := app.FindRecordById("songs", song.ID)
		if err != nil {
			log.Printf("[backfill_song_artists] warning: failed to load song %s for update: %v", song.ID, err)
			continue
		}

		record.Set("artist_name", resolution.UpdatedArtistName)
		if resolution.UpdatedArtistSpotifyIDs != "" {
			record.Set("artist_spotify_ids", resolution.UpdatedArtistSpotifyIDs)
		}
		if err := app.Save(record); err != nil {
			log.Printf("[backfill_song_artists] warning: failed to save song %s: %v", song.ID, err)
			continue
		}

		applied++
	}

	summary := buildSummary(resolutions, *minConfidence)
	summary.UpdatesApplied = applied
	summary.ArtistNameChanges = artistNameChanges

	if err := os.MkdirAll(*reportDir, 0o755); err != nil {
		log.Fatalf("[backfill_song_artists] failed to create report directory: %v", err)
	}

	timestamp := time.Now().UTC().Format("20060102_150405")
	reportPath := filepath.Join(*reportDir, fmt.Sprintf("song_artist_backfill_%s.json", timestamp))
	reviewQueue := buildReviewQueue(reportPath, resolutions, artists)
	reviewQueueJSONPath := ""
	reviewQueueCSVPath := ""
	if len(reviewQueue.ReviewEntries) > 0 {
		reviewQueueJSONPath = filepath.Join(*reportDir, fmt.Sprintf("song_artist_review_queue_%s.json", timestamp))
		reviewQueueCSVPath = filepath.Join(*reportDir, fmt.Sprintf("song_artist_review_queue_%s.csv", timestamp))
		reviewQueue.JSONPath = reviewQueueJSONPath
		reviewQueue.CSVPath = reviewQueueCSVPath
		if err := writeReviewQueueJSON(reviewQueueJSONPath, reviewQueue); err != nil {
			log.Fatalf("[backfill_song_artists] failed to write review queue json: %v", err)
		}
		if err := writeReviewQueueCSV(reviewQueueCSVPath, reviewQueue); err != nil {
			log.Fatalf("[backfill_song_artists] failed to write review queue csv: %v", err)
		}
	}
	reportPayload := report{
		GeneratedAt:       time.Now().UTC(),
		ApplyRequested:    *apply,
		MinimumConfidence: *minConfidence,
		ReportPath:        reportPath,
		ReviewQueueJSON:   reviewQueueJSONPath,
		ReviewQueueCSV:    reviewQueueCSVPath,
		Summary:           summary,
		Resolutions:       resolutions,
	}

	if err := writeReport(reportPath, reportPayload); err != nil {
		log.Fatalf("[backfill_song_artists] failed to write report: %v", err)
	}

	log.Printf("[backfill_song_artists] wrote audit report to %s", reportPath)
	log.Printf("[backfill_song_artists] candidates=%d below_threshold=%d applied=%d ambiguous=%d unresolved=%d artist_name_changes=%d",
		summary.UpdateCandidates,
		summary.BelowThreshold,
		summary.UpdatesApplied,
		summary.SkippedAmbiguous,
		summary.SkippedUnresolved,
		summary.ArtistNameChanges,
	)
	if len(reviewQueue.ReviewEntries) > 0 {
		log.Printf("[backfill_song_artists] wrote review queue to %s and %s (%d items)", reviewQueueJSONPath, reviewQueueCSVPath, len(reviewQueue.ReviewEntries))
	}
	if !*apply {
		log.Printf("[backfill_song_artists] dry run only; review the report, run cmd/safebackup, then rerun with --apply")
	}
}

func loadSongs(app *pocketbase.PocketBase) ([]songbackfill.SongInput, error) {
	records, err := app.FindAllRecords("songs")
	if err != nil {
		return nil, err
	}

	songs := make([]songbackfill.SongInput, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.GetString("artist_spotify_ids")) != "" {
			continue
		}
		songs = append(songs, songbackfill.SongInput{
			ID:               record.Id,
			Title:            strings.TrimSpace(record.GetString("title")),
			ArtistName:       strings.TrimSpace(record.GetString("artist_name")),
			ReleaseDate:      strings.TrimSpace(record.GetString("release_date")),
			ArtistSpotifyIDs: strings.TrimSpace(record.GetString("artist_spotify_ids")),
		})
	}

	sort.SliceStable(songs, func(i, j int) bool {
		if songs[i].ArtistName != songs[j].ArtistName {
			return songs[i].ArtistName < songs[j].ArtistName
		}
		return songs[i].Title < songs[j].Title
	})

	return songs, nil
}

func planSongsFromLatestReport(reportDir string, songs []songbackfill.SongInput) ([]songbackfill.SongInput, string, error) {
	reportPath, err := latestBackfillReportPath(reportDir)
	if err != nil {
		return songs, "", err
	}
	if reportPath == "" {
		return songs, "", nil
	}

	hints, err := loadResolutionHints(reportPath)
	if err != nil {
		return songs, reportPath, err
	}

	planned := prioritizeSongsForRetry(songs, hints)
	return planned, reportPath, nil
}

func latestBackfillReportPath(reportDir string) (string, error) {
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	latest := ""
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "song_artist_backfill_") || !strings.HasSuffix(name, ".json") {
			continue
		}
		if latest == "" || name > filepath.Base(latest) {
			latest = filepath.Join(reportDir, name)
		}
	}

	return latest, nil
}

func loadResolutionHints(path string) (map[string]resolutionHints, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var payload priorReportSummary
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	hints := make(map[string]resolutionHints, len(payload.Resolutions))
	for _, resolution := range payload.Resolutions {
		if resolution.SongID == "" {
			continue
		}
		hints[resolution.SongID] = classifyResolutionHint(resolution)
	}
	return hints, nil
}

func classifyResolutionHint(resolution songbackfill.Resolution) resolutionHints {
	hint := resolutionHints{
		Category:      "new_song",
		Priority:      3,
		RetryEligible: true,
		Notes:         append([]string(nil), resolution.Notes...),
	}

	switch {
	case resolution.Action == songbackfill.ActionSkipAmbiguous:
		hint.Category = "ambiguous_external_credit"
		hint.Priority = 4
		hint.RetryEligible = false
	case containsResolutionNote(resolution.Notes, "external lookup did not find a confident multi-artist credit for an ellipsis-based song"):
		hint.Category = "needs_manual_credit_lookup"
		hint.Priority = 5
		hint.RetryEligible = false
	case len(extractMissingArtistNames(resolution.Notes)) > 0 && len(selectCandidateArtistNames(resolution)) > 0:
		hint.Category = "missing_artist_record"
		hint.Priority = 1
		hint.RetryEligible = true
	case len(extractMissingArtistNames(resolution.Notes)) > 0:
		hint.Category = "artist_name_mismatch"
		hint.Priority = 2
		hint.RetryEligible = true
	}

	return hint
}

func prioritizeSongsForRetry(songs []songbackfill.SongInput, hints map[string]resolutionHints) []songbackfill.SongInput {
	type prioritizedSong struct {
		song           songbackfill.SongInput
		priority       int
		previouslySeen bool
		retryEligible  bool
	}

	prioritized := make([]prioritizedSong, 0, len(songs))
	for _, song := range songs {
		hint, ok := hints[song.ID]
		if !ok {
			prioritized = append(prioritized, prioritizedSong{song: song, priority: 0, retryEligible: true})
			continue
		}
		prioritized = append(prioritized, prioritizedSong{
			song:           song,
			priority:       hint.Priority,
			previouslySeen: true,
			retryEligible:  hint.RetryEligible,
		})
	}

	sort.SliceStable(prioritized, func(i, j int) bool {
		if prioritized[i].retryEligible != prioritized[j].retryEligible {
			return prioritized[i].retryEligible
		}
		if prioritized[i].priority != prioritized[j].priority {
			return prioritized[i].priority < prioritized[j].priority
		}
		if prioritized[i].previouslySeen != prioritized[j].previouslySeen {
			return !prioritized[i].previouslySeen
		}
		if prioritized[i].song.ArtistName != prioritized[j].song.ArtistName {
			return prioritized[i].song.ArtistName < prioritized[j].song.ArtistName
		}
		return prioritized[i].song.Title < prioritized[j].song.Title
	})

	result := make([]songbackfill.SongInput, 0, len(prioritized))
	for _, item := range prioritized {
		result = append(result, item.song)
	}
	return result
}

func prepareSongForResolution(song songbackfill.SongInput) songbackfill.SongInput {
	song.ArtistName = sanitizeStoredArtistName(song.ArtistName)
	return song
}

func sanitizeStoredArtistName(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "…", "..."))
	if value == "" {
		return value
	}

	value = trailingDotEllipsisPattern.ReplaceAllString(value, ", ...")
	value = strings.Join(strings.Fields(value), " ")
	if strings.HasSuffix(value, ", ....") {
		value = strings.TrimSuffix(value, ", ....") + ", ..."
	}
	return value
}

func containsResolutionNote(notes []string, target string) bool {
	for _, note := range notes {
		if note == target {
			return true
		}
	}
	return false
}

func selectCandidateArtistNames(resolution songbackfill.Resolution) []string {
	selected := selectCandidateForReview(resolution)
	if selected == nil {
		return nil
	}
	return append([]string(nil), selected.ArtistNames...)
}

func loadArtists(app *pocketbase.PocketBase) ([]songbackfill.ArtistInput, error) {
	records, err := app.FindAllRecords("artists")
	if err != nil {
		return nil, err
	}

	artists := make([]songbackfill.ArtistInput, 0, len(records))
	for _, record := range records {
		spotifyID := strings.TrimSpace(record.GetString("spotify_id"))
		if spotifyID == "" {
			continue
		}
		artists = append(artists, songbackfill.ArtistInput{
			RecordID:  record.Id,
			Name:      strings.TrimSpace(record.GetString("name")),
			SpotifyID: spotifyID,
		})
	}

	return artists, nil
}

func buildSummary(resolutions []songbackfill.Resolution, minimumConfidence float64) reportSummary {
	summary := reportSummary{
		SongsScanned: len(resolutions),
	}

	for _, resolution := range resolutions {
		switch resolution.Action {
		case songbackfill.ActionUpdate:
			if resolution.Approved(minimumConfidence) {
				summary.UpdateCandidates++
			} else {
				summary.BelowThreshold++
			}
		case songbackfill.ActionUpdateNameOnly:
			if resolution.NamePrefillApproved(minimumConfidence) {
				summary.UpdateCandidates++
			} else {
				summary.BelowThreshold++
			}
		case songbackfill.ActionSkipAmbiguous:
			summary.SkippedAmbiguous++
		case songbackfill.ActionSkipUnresolved:
			summary.SkippedUnresolved++
		}
	}

	return summary
}

func writeReport(path string, payload report) error {
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func fetchTidalAccessToken(ctx context.Context, client *http.Client, tokenURL, clientID, clientSecret string) (string, error) {
	tokenURL = strings.TrimSpace(tokenURL)
	if tokenURL == "" {
		tokenURL = "https://auth.tidal.com/v1/oauth2/token"
	}
	clientID = strings.TrimSpace(clientID)
	clientSecret = strings.TrimSpace(clientSecret)
	if clientID == "" || clientSecret == "" {
		return "", fmt.Errorf("missing TIDAL client credentials")
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	basic := base64.StdEncoding.EncodeToString([]byte(clientID + ":" + clientSecret))
	req.Header.Set("Authorization", "Basic "+basic)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tidal token endpoint returned %s", resp.Status)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("failed to decode tidal token response: %w", err)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("tidal token response did not include access_token")
	}

	return strings.TrimSpace(payload.AccessToken), nil
}
