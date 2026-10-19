// Command backfill_song_metadata fills missing song metadata (album,
// release date, release year, release type) from TIDAL search and the
// PocketBase release_type labels the migration dropped, as events: it
// appends SongMetadataEnriched facts to the authoritative event store
// (SQLite or JetStream per LISTENLEDGER_EVENT_STORE) and projects them into
// the SQLite read model, so a replay rebuilds the same rows. By default it
// performs a dry-run that emits a JSON report; rerun with --apply after
// review to append the events. --apply against the live data dir requires
// --live-ok (stop the app first — in jetstream mode the tool boots an
// embedded NATS server on the live store, so a second server would corrupt
// it).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/db"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/songbackfill"
	"ListenLedger/internal/storeutil"
)

const (
	actionUpdate            = "update"
	actionSkipUnresolved    = "skip_unresolved"
	actionSkipLowConfidence = "skip_low_confidence"
	actionSkipNoData        = "skip_no_data"
	actionSkipLookupFailed  = "skip_lookup_failed"
	actionSkipNoMissing     = "skip_no_missing"
	actionSkipError         = "skip_error"

	sourceTidal       = "tidal"
	sourcePocketbase  = "pocketbase"
	notePBReleaseType = "release_type from the PocketBase label the migration dropped"
)

// songRow is the read-model slice needed to decide enrichment and seed
// legacy streams (rows predating the event log).
type songRow struct {
	id          string
	title       string
	artistName  string
	album       string
	releaseDate string
	releaseType string
	spotifyID   string
	releaseYear int64
}

type fillPlan struct {
	Album       string `json:"album,omitempty"`
	ReleaseDate string `json:"release_date,omitempty"`
	ReleaseYear int64  `json:"release_year,omitempty"`
	ReleaseType string `json:"release_type,omitempty"`
}

type candidateInfo struct {
	Source      string   `json:"source"`
	Title       string   `json:"title"`
	ArtistNames []string `json:"artist_names,omitempty"`
	AlbumName   string   `json:"album_name,omitempty"`
	ReleaseDate string   `json:"release_date,omitempty"`
	ReleaseType string   `json:"release_type,omitempty"`
	Confidence  float64  `json:"confidence"`
}

type resolution struct {
	SongID     string         `json:"song_id"`
	Title      string         `json:"title"`
	ArtistName string         `json:"artist_name"`
	Action     string         `json:"action"`
	Confidence float64        `json:"confidence,omitempty"`
	Fill       *fillPlan      `json:"fill,omitempty"`
	Candidate  *candidateInfo `json:"candidate,omitempty"`
	Note       string         `json:"note,omitempty"`
	Applied    bool           `json:"applied,omitempty"`

	row songRow `json:"-"`
}

type summary struct {
	SongsInspected       int `json:"songs_inspected"`
	UpdatesPlanned       int `json:"updates_planned"`
	UpdatesApplied       int `json:"updates_applied"`
	SkippedUnresolved    int `json:"skipped_unresolved"`
	SkippedLowConfidence int `json:"skipped_low_confidence"`
	SkippedNoData        int `json:"skipped_no_data"`
	SkippedNoMissing     int `json:"skipped_no_missing"`
	SkippedLookupFailed  int `json:"skipped_lookup_failed"`
	Errors               int `json:"errors"`
}

type report struct {
	GeneratedAt       time.Time    `json:"generated_at"`
	ApplyRequested    bool         `json:"apply_requested"`
	MinimumConfidence float64      `json:"minimum_confidence"`
	EventStoreMode    string       `json:"event_store_mode"`
	Summary           summary      `json:"summary"`
	Resolutions       []resolution `json:"resolutions"`
}

func main() {
	defaultDataDir := appdir.ResolveDataDir()
	dataDir := flag.String("data-dir", defaultDataDir, "Data directory holding the SQLite ledger (and NATS store in jetstream mode)")
	apply := flag.Bool("apply", false, "Append SongMetadataEnriched events and project them (default is dry-run)")
	liveOK := flag.Bool("live-ok", false, "Allow --apply against the live data dir (stop the app first; in jetstream mode a second embedded NATS server boots on the live store)")
	limit := flag.Int("limit", 0, "Maximum number of songs to inspect (0 for all)")
	reportDir := flag.String("report-dir", "", "Directory for JSON reports (defaults to <data-dir>/backfill_reports)")
	minConfidence := flag.Float64("min-confidence", 0.90, "Minimum confidence required before enriching a song")
	httpTimeout := flag.Duration("http-timeout", 15*time.Second, "Timeout for TIDAL lookups")
	tidalTokenURL := flag.String("tidal-token-url", "https://auth.tidal.com/v1/oauth2/token", "OAuth token endpoint for TIDAL client-credentials flow")
	tidalBase := flag.String("tidal-base", "https://openapi.tidal.com/v2", "Base URL for TIDAL searchResults lookups")
	tidalCountryCode := flag.String("tidal-country-code", "US", "Country code for TIDAL searchResults lookups")
	tidalToken := flag.String("tidal-token", "", "Bearer token for TIDAL OpenAPI requests (or set TIDAL_TOKEN)")
	tidalClientID := flag.String("tidal-client-id", "", "TIDAL client ID for client-credentials auth (or set TIDAL_CLIENT_ID)")
	tidalClientSecret := flag.String("tidal-client-secret", "", "TIDAL client secret for client-credentials auth (or set TIDAL_CLIENT_SECRET)")
	flag.Parse()

	ctx := context.Background()
	mode, err := eventsourcing.SelectedStoreName()
	if err != nil {
		log.Fatalf("[backfill_song_metadata] %v", err)
	}
	log.Printf("[backfill_song_metadata] event store mode: %s", mode)

	if *apply && !*liveOK && os.Getenv("LISTENLEDGER_BACKFILL_LIVE_OK") != "1" && storeutil.IsLiveDataDir(*dataDir) {
		log.Fatalf("[backfill_song_metadata] refusing --apply against the live data dir: stop the app first, or point --data-dir at a copy, or pass --live-ok (a second server on the live NATS store would corrupt it while the app runs)")
	}

	if strings.TrimSpace(*reportDir) == "" {
		*reportDir = filepath.Join(*dataDir, "backfill_reports")
	}

	database, err := db.SetupDB(ctx, slog.Default(), *dataDir, false)
	if err != nil {
		log.Fatalf("[backfill_song_metadata] failed to open ledger: %v", err)
	}
	defer func() { _ = database.Close() }()

	rows, err := loadSongsMissingMetadata(ctx, database, *limit)
	if err != nil {
		log.Fatalf("[backfill_song_metadata] failed to load songs: %v", err)
	}
	log.Printf("[backfill_song_metadata] loaded %d song(s) with missing metadata", len(rows))
	if len(rows) == 0 {
		log.Printf("[backfill_song_metadata] nothing to do")
		return
	}

	pbTypes, err := loadPocketBaseReleaseTypes(ctx, *dataDir)
	if err != nil {
		log.Fatalf("[backfill_song_metadata] failed to load PocketBase release types: %v", err)
	}
	if len(pbTypes) > 0 {
		log.Printf("[backfill_song_metadata] loaded %d PocketBase release-type label(s)", len(pbTypes))
	}

	token := songbackfill.ResolveTidalToken(ctx, &http.Client{Timeout: *httpTimeout}, songbackfill.TidalCredentials{
		TokenURL:     *tidalTokenURL,
		ClientID:     *tidalClientID,
		ClientSecret: *tidalClientSecret,
		HTTPTimeout:  *httpTimeout,
	}, *tidalToken)
	if token == "" {
		log.Fatalf("[backfill_song_metadata] no TIDAL token available (set --tidal-token or TIDAL_CLIENT_ID/TIDAL_CLIENT_SECRET); nothing can be enriched")
	}

	lookup := &songbackfill.TidalLookup{
		BaseURL:     *tidalBase,
		CountryCode: *tidalCountryCode,
		HTTPClient:  &http.Client{Timeout: *httpTimeout},
		AuthToken:   token,
	}

	resolutions := resolveAll(ctx, lookup, rows, pbTypes, *minConfidence)
	rep := report{
		GeneratedAt:       time.Now().UTC(),
		ApplyRequested:    *apply,
		MinimumConfidence: *minConfidence,
		EventStoreMode:    mode,
		Resolutions:       resolutions,
	}
	rep.Summary = summarize(resolutions)
	log.Printf("[backfill_song_metadata] plan: %d enrichable, %d below confidence, %d without data, %d unresolved, %d lookup failures",
		rep.Summary.UpdatesPlanned, rep.Summary.SkippedLowConfidence, rep.Summary.SkippedNoData,
		rep.Summary.SkippedUnresolved, rep.Summary.SkippedLookupFailed)

	applyFailures := 0
	if *apply {
		applied, failed, err := applyResolutions(ctx, *dataDir, database, resolutions)
		if err != nil {
			log.Fatalf("[backfill_song_metadata] apply failed: %v", err)
		}
		// Recompute: the pre-apply summary counted every planned update as
		// planned; afterwards the split between applied rows, converged rows
		// (skip_no_missing) and errored rows must show in the report.
		rep.Summary = summarize(resolutions)
		log.Printf("[backfill_song_metadata] appended %d SongMetadataEnriched event(s)", applied)
		applyFailures = failed
	}

	if err := writeReport(*reportDir, rep); err != nil {
		log.Fatalf("[backfill_song_metadata] failed to write report: %v", err)
	}
	if !*apply {
		log.Printf("[backfill_song_metadata] dry run complete; rerun with --apply to append the events")
	}
	// Exit non-zero when any append or projection failed: the events that did
	// land stay, but exit 0 must not read as full success.
	if applyFailures > 0 {
		log.Fatalf("[backfill_song_metadata] %d song(s) failed during apply (per-song errors in the report)", applyFailures)
	}
}

// resolveAll plans enrichment for every song: TIDAL search keyed by title,
// gated by the primary-artist prefix, then the highest-confidence candidate
// that actually carries data for a missing field. A PocketBase release_type
// label (dropped by the PB→ledger migration) pre-fills the type; the
// user-curated label wins over a TIDAL suffix.
func resolveAll(ctx context.Context, lookup songbackfill.TrackMetadataLookup, rows []songRow, pbTypes map[string]string, minConfidence float64) []resolution {
	resolutions := make([]resolution, 0, len(rows))
	for _, row := range rows {
		res := resolution{
			SongID:     row.id,
			Title:      row.title,
			ArtistName: row.artistName,
			row:        row,
		}
		pbType := ""
		if row.releaseType == "" {
			pbType = pbTypes[row.id]
		}

		candidates, err := lookup.Lookup(ctx, songbackfill.SongInput{
			ID: row.id, Title: row.title, ArtistName: row.artistName, ReleaseDate: row.releaseDate,
		}, songbackfill.PrimaryArtistPrefix(row.artistName))
		if err != nil {
			// A PocketBase release_type label is local, trusted data:
			// port it even when TIDAL is unreachable, mirroring the
			// candidate-less fallback below.
			if pbType != "" {
				res.Fill = planYear(&fillPlan{ReleaseType: pbType}, row)
				res.Confidence = 1
				res.Note = notePBReleaseType
				res.Action = actionUpdate
			} else {
				res.Action = actionSkipLookupFailed
				res.Note = err.Error()
			}
			resolutions = append(resolutions, res)
			continue
		}
		if len(candidates) == 0 {
			// A PocketBase label ports even when TIDAL resolves nothing.
			if pbType != "" {
				res.Fill = planYear(&fillPlan{ReleaseType: pbType}, row)
				res.Confidence = 1
				res.Note = notePBReleaseType
				res.Action = actionUpdate
			} else {
				res.Action = actionSkipUnresolved
			}
			resolutions = append(resolutions, res)
			continue
		}

		// Lookup returns candidates deduped and sorted by confidence desc,
		// so the first one carrying data for a missing field is the best.
		for i := range candidates {
			candidate := candidates[i]
			fill := fillPlan{ReleaseType: pbType}
			if row.album == "" && candidate.AlbumName != "" {
				fill.Album = candidate.AlbumName
			}
			if row.releaseDate == "" && candidate.ReleaseDate != "" {
				fill.ReleaseDate = candidate.ReleaseDate
			}
			if row.releaseType == "" && fill.ReleaseType == "" && candidate.ReleaseType != "" {
				fill.ReleaseType = candidate.ReleaseType
			}
			// TIDAL must contribute the album, the date, or a type suffix;
			// a year fix alone is derivation, not enrichment. The PB label
			// pre-fill is trusted local data, not a TIDAL contribution, so it
			// must not make a dataless candidate win the loop.
			tidalType := pbType == "" && fill.ReleaseType != ""
			if fill.Album == "" && fill.ReleaseDate == "" && !tidalType {
				continue
			}
			planYear(&fill, row)
			res.Fill = &fill
			res.Confidence = candidate.Confidence
			if pbType != "" {
				res.Note = notePBReleaseType
			}
			res.Candidate = &candidateInfo{
				Source:      candidate.Source,
				Title:       candidate.Title,
				ArtistNames: candidate.ArtistNames,
				AlbumName:   candidate.AlbumName,
				ReleaseDate: candidate.ReleaseDate,
				ReleaseType: candidate.ReleaseType,
				Confidence:  candidate.Confidence,
			}
			if candidate.Confidence < minConfidence {
				// The trusted PB label still lands even when TIDAL's best
				// candidate is below the confidence threshold.
				if pbType != "" {
					res.Fill = planYear(&fillPlan{ReleaseType: pbType}, row)
					res.Confidence = 1
					res.Candidate = nil
					res.Note = notePBReleaseType
					res.Action = actionUpdate
				} else {
					res.Action = actionSkipLowConfidence
				}
			} else {
				res.Action = actionUpdate
			}
			break
		}
		if res.Action == "" {
			if pbType != "" {
				res.Fill = planYear(&fillPlan{ReleaseType: pbType}, row)
				res.Confidence = 1
				res.Note = notePBReleaseType
				res.Action = actionUpdate
			} else {
				res.Action = actionSkipNoData
			}
		}
		resolutions = append(resolutions, res)
	}
	return resolutions
}

// planYear mirrors the aggregate's merge in the report: the year comes from
// whichever date survives the merge, so the planned fill matches the event.
func planYear(fill *fillPlan, row songRow) *fillPlan {
	if row.releaseYear == 0 {
		// The domain derives the year from the merged release date, so the
		// fill's date only supplies a year when the row has none; when the
		// row already carries a date, that date is what the aggregate will
		// record and a year planned from the fill's date would never land.
		yearSource := row.releaseDate
		if yearSource == "" {
			yearSource = fill.ReleaseDate
		}
		fill.ReleaseYear = int64(songbackfill.ParseReleaseYear(yearSource))
	}
	return fill
}

func summarize(resolutions []resolution) summary {
	var s summary
	s.SongsInspected = len(resolutions)
	for _, res := range resolutions {
		switch res.Action {
		case actionUpdate:
			if res.Applied {
				s.UpdatesApplied++
			} else {
				s.UpdatesPlanned++
			}
		case actionSkipUnresolved:
			s.SkippedUnresolved++
		case actionSkipLowConfidence:
			s.SkippedLowConfidence++
		case actionSkipNoData:
			s.SkippedNoData++
		case actionSkipNoMissing:
			s.SkippedNoMissing++
		case actionSkipLookupFailed:
			s.SkippedLookupFailed++
		default:
			s.Errors++
		}
	}
	return s
}

func writeReport(reportDir string, rep report) error {
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	timestamp := time.Now().UTC().Format("20060102_150405")
	path := filepath.Join(reportDir, "song_metadata_backfill_"+timestamp+".json")
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	log.Printf("[backfill_song_metadata] report written to %s", path)
	return nil
}
