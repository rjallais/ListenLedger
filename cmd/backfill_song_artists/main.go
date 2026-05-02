//go:build goexperiment.jsonv2

// Command backfill_song_artists audits songs with empty artist_spotify_ids and
// resolves multi-artist credits using MusicBrainz, Deezer, and optionally TIDAL
// as external sources. By default it performs a dry-run that emits a JSON report
// and a CSV review queue; rerun with --apply after review to persist updates.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/songbackfill"
)

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

	ctx := context.Background()

	songs, err := loadSongs(ctx, app)
	if err != nil {
		log.Fatalf("[backfill_song_artists] failed to load songs: %v", err)
	}

	artists, err := loadArtists(ctx, app)
	if err != nil {
		log.Fatalf("[backfill_song_artists] failed to load artists: %v", err)
	}

	songs = applyRetryPlanning(*retryFromLatestReport, *reportDir, songs, *limit)

	log.Printf("[backfill_song_artists] loaded %d songs with empty artist_spotify_ids and %d artists with spotify_id", len(songs), len(artists))
	if len(songs) == 0 {
		log.Printf("[backfill_song_artists] nothing to do")
		return
	}

	httpClient := &http.Client{Timeout: *httpTimeout}
	resolver := buildResolver(ctx, httpClient, buildResolverParams{
		TidalTokenURL:     *tidalTokenURL,
		TidalClientID:     *tidalClientID,
		TidalClientSecret: *tidalClientSecret,
		TidalFlagToken:    *tidalToken,
		TidalBase:         *tidalBase,
		TidalCountryCode:  *tidalCountryCode,
		MusicBrainzBase:   *musicBrainzBase,
		DeezerBase:        *deezerBase,
		HTTPTimeout:       *httpTimeout,
		MinConfidence:     *minConfidence,
	}, artists)

	resolutions := resolveSongs(ctx, resolver, songs, *httpTimeout)
	applied, artistNameChanges := applyApprovedResolutions(ctx, app, resolutions, *minConfidence, *apply)

	summary := buildSummary(resolutions, *minConfidence)
	summary.UpdatesApplied = applied
	summary.ArtistNameChanges = artistNameChanges

	if err := os.MkdirAll(*reportDir, 0o755); err != nil {
		log.Fatalf("[backfill_song_artists] failed to create report directory: %v", err)
	}

	timestamp := time.Now().UTC().Format("20060102_150405")
	reportPath := filepath.Join(*reportDir, fmt.Sprintf("song_artist_backfill_%s.json", timestamp))
	reviewQueue, reviewQueueJSONPath, reviewQueueCSVPath, err := writeReviewOutputs(ctx, *reportDir, timestamp, reportPath, resolutions, artists)
	if err != nil {
		log.Fatalf("[backfill_song_artists] failed to write review outputs: %v", err)
	}
	reportPayload := report{
		GeneratedAt:      time.Now().UTC(),
		ApplyRequested:   *apply,
		MinimumConfidence: *minConfidence,
		ReportPath:       reportPath,
		ReviewQueueJSON:  reviewQueueJSONPath,
		ReviewQueueCSV:   reviewQueueCSVPath,
		Summary:          summary,
		Resolutions:      resolutions,
	}

	if err := writeReport(reportPath, reportPayload); err != nil {
		log.Fatalf("[backfill_song_artists] failed to write report: %v", err)
	}

	logSummary(summary, reviewQueue, reviewQueueJSONPath, reviewQueueCSVPath, *apply)
}

type buildResolverParams struct {
	TidalTokenURL     string
	TidalClientID     string
	TidalClientSecret string
	TidalFlagToken    string
	TidalBase         string
	TidalCountryCode  string
	MusicBrainzBase   string
	DeezerBase        string
	HTTPTimeout       time.Duration
	MinConfidence     float64
}

func buildResolver(ctx context.Context, httpClient *http.Client, params buildResolverParams, artists []songbackfill.ArtistInput) *songbackfill.Resolver {
	resolvedTidalToken := resolveTidalToken(ctx, httpClient, tidalCredentials{
		TokenURL:     params.TidalTokenURL,
		ClientID:     params.TidalClientID,
		ClientSecret: params.TidalClientSecret,
		HTTPTimeout:  params.HTTPTimeout,
	}, params.TidalFlagToken)

	return songbackfill.NewResolver(artists, songbackfill.Options{
		NamePrefillLookup: &songbackfill.TidalLookup{
			BaseURL:     params.TidalBase,
			CountryCode: params.TidalCountryCode,
			HTTPClient:  httpClient,
			UserAgent:   "ListenLedger/1.0 (song artist backfill)",
			AuthToken:   resolvedTidalToken,
		},
		TrackLookup: songbackfill.ChainTrackLookup{Lookups: []songbackfill.TrackMetadataLookup{
			&songbackfill.MusicBrainzLookup{
				BaseURL:    params.MusicBrainzBase,
				HTTPClient: httpClient,
				UserAgent:  "ListenLedger/1.0 (song artist backfill)",
			},
			&songbackfill.DeezerLookup{
				BaseURL:    params.DeezerBase,
				HTTPClient: httpClient,
				UserAgent:  "ListenLedger/1.0 (song artist backfill)",
			},
		}},
		MinimumConfidence: params.MinConfidence,
	})
}

func applyRetryPlanning(enabled bool, reportDir string, songs []songbackfill.SongInput, limit int) []songbackfill.SongInput {
	applyLimit := func(items []songbackfill.SongInput) []songbackfill.SongInput {
		if limit > 0 && len(items) > limit {
			return items[:limit]
		}
		return items
	}

	if !enabled {
		return applyLimit(songs)
	}
	plannedSongs, latestReportPath, err := planSongsFromLatestReport(reportDir, songs)
	if err != nil {
		log.Printf("[backfill_song_artists] warning: failed to use latest report for retry planning: %v", err)
		return applyLimit(songs)
	}
	if latestReportPath != "" {
		plannedSongs = applyLimit(plannedSongs)
		log.Printf("[backfill_song_artists] prioritized %d songs using latest report %s", len(plannedSongs), latestReportPath)
		return plannedSongs
	}
	return applyLimit(songs)
}

func resolveSongs(ctx context.Context, resolver *songbackfill.Resolver, songs []songbackfill.SongInput, httpTimeout time.Duration) []songbackfill.Resolution {
	resolutions := make([]songbackfill.Resolution, 0, len(songs))
	for _, song := range songs {
		song = prepareSongForResolution(song)
		reqCtx, cancel := context.WithTimeout(ctx, httpTimeout)
		resolution := resolver.Resolve(reqCtx, song)
		cancel()
		resolutions = append(resolutions, resolution)
	}
	return resolutions
}

func logSummary(summary reportSummary, queue reviewQueue, jsonPath, csvPath string, apply bool) {
	log.Printf("[backfill_song_artists] wrote audit report")
	log.Printf("[backfill_song_artists] candidates=%d below_threshold=%d applied=%d ambiguous=%d unresolved=%d artist_name_changes=%d",
		summary.UpdateCandidates,
		summary.BelowThreshold,
		summary.UpdatesApplied,
		summary.SkippedAmbiguous,
		summary.SkippedUnresolved,
		summary.ArtistNameChanges,
	)
	if len(queue.ReviewEntries) > 0 {
		log.Printf("[backfill_song_artists] wrote review queue to %s and %s (%d items)", jsonPath, csvPath, len(queue.ReviewEntries))
	}
	if !apply {
		log.Printf("[backfill_song_artists] dry run only; review the report, run cmd/safebackup, then rerun with --apply")
	}
}
