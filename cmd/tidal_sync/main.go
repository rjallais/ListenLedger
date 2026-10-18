// Command tidal_sync synchronizes user favorites from TIDAL, records them as
// immutable domain events, and reconciles differences against the ListenLedger catalog.
package main

import (
	"cmp"
	"context"
	"encoding/csv"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/pocketbase/pocketbase"
	"golang.org/x/text/unicode/norm"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/correctionproposal"
	"ListenLedger/internal/domain/song"
	syncdomain "ListenLedger/internal/domain/tidalsync"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
	"ListenLedger/internal/songbackfill"
	"ListenLedger/internal/storeutil"
	"ListenLedger/internal/tidal"
	"ListenLedger/internal/tidalsync"
	_ "ListenLedger/migrations"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()

	switch os.Args[1] {
	case "login":
		if err := runLogin(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "plan":
		if err := runPlan(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "apply":
		if err := runApply(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "help", "-h", "--help":
		printUsage()
	default:
		// If first argument looks like a flag, assume "plan"
		if strings.HasPrefix(os.Args[1], "-") {
			if err := runPlan(ctx, os.Args[1:]); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return
		}
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`Usage: tidal_sync <subcommand> [flags]

Subcommands:
  login    Authenticate with TIDAL via OAuth PKCE and save refresh token to FNOX
  plan     Fetch favorites, append observation events, reconcile against catalog, emit report
  apply    Apply approved proposals to the authoritative event store and read models

Run 'tidal_sync <subcommand> -h' for subcommand help.
`)
}

func runLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	port := fs.Int("port", 8888, "Loopback redirect port")
	redirectURI := fs.String("redirect-uri", "", "Full redirect URI registered in TIDAL developer dashboard")
	scope := fs.String("scope", "collection.read", "OAuth scopes to request (space-separated)")
	_ = fs.Parse(args)

	clientID, err := tidal.ResolveSecret(ctx, "TIDAL_CLIENT_ID")
	if err != nil || clientID == "" {
		return errors.New("missing TIDAL_CLIENT_ID (set in env or run: fnox set TIDAL_CLIENT_ID <id>)")
	}

	clientSecret, err := tidal.ResolveSecret(ctx, "TIDAL_CLIENT_SECRET")
	if err != nil || clientSecret == "" {
		return errors.New("missing TIDAL_CLIENT_SECRET (set in env or run: fnox set TIDAL_CLIENT_SECRET <secret>)")
	}

	redir := strings.TrimSpace(*redirectURI)
	if redir == "" {
		redir = fmt.Sprintf("http://127.0.0.1:%d/callback", *port)
	}

	scopes := strings.Fields(*scope)
	if len(scopes) == 0 {
		scopes = []string{"collection.read"}
	}

	cfg := tidal.AuthConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURI:  redir,
		Scopes:       scopes,
		HTTPClient:   &http.Client{Timeout: 30 * time.Second},
	}

	fmt.Println("Starting TIDAL OAuth PKCE authorization flow...")
	token, err := tidal.AuthorizeLoopback(ctx, cfg)
	if err != nil {
		return fmt.Errorf("authorization failed: %w", err)
	}

	fmt.Printf("\n✓ Successfully authenticated with TIDAL!\nUser ID: %s\nToken Expires: %s\n", token.UserID, token.Expiry.Format(time.RFC3339))
	return nil
}

type syncReport struct {
	Timestamp   time.Time                `json:"timestamp"`
	RunID       string                   `json:"run_id"`
	UserID      string                   `json:"user_id"`
	CountryCode string                   `json:"country_code"`
	DryRun      bool                     `json:"dry_run"`
	TracksCount int                      `json:"tracks_count"`
	Summary     map[string]int           `json:"summary"`
	Proposals   []ProposalPlanItemReport `json:"proposals"`
}

type ProposalPlanItemReport struct {
	tidalsync.ProposalPlan
	Approved bool   `json:"approved"`
	Applied  bool   `json:"applied"`
	Note     string `json:"note,omitempty"`
}

func runPlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	dataDir := fs.String("data-dir", appdir.ResolveDataDir(), "Data directory")
	limit := fs.Int("limit", 0, "Max favorite tracks to fetch (0 = all)")
	country := fs.String("country", cmp.Or(os.Getenv("TIDAL_COUNTRY_CODE"), "BR"), "TIDAL country code")
	minConf := fs.Float64("min-confidence", 0.85, "Minimum confidence threshold")
	autoLinkThresh := fs.Float64("auto-link-threshold", 0.90, "Auto-link confidence threshold")
	dryRun := fs.Bool("dry-run", true, "Plan without writing events to store")
	liveOK := fs.Bool("live-ok", false, "Allow writing events to live data directory")
	fillImpreciseDates := fs.Bool("fill-imprecise-dates", false, "Propose filling imprecise release dates (day defaulted to 01 or year-only) into existing catalog songs")
	reportDir := fs.String("report-dir", "backfill_reports", "Directory to write reports")
	_ = fs.Parse(args)

	// Resolve credentials
	clientID, _ := tidal.ResolveSecret(ctx, "TIDAL_CLIENT_ID")
	clientSecret, _ := tidal.ResolveSecret(ctx, "TIDAL_CLIENT_SECRET")
	refreshToken, _ := tidal.ResolveSecret(ctx, "TIDAL_REFRESH_TOKEN")

	if refreshToken == "" {
		return errors.New("missing TIDAL_REFRESH_TOKEN: please run 'tidal_sync login' first")
	}

	// Refresh access token
	authCfg := tidal.AuthConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		PersistRefreshToken: func(c context.Context, token string) error {
			return tidal.SaveSecret(c, "TIDAL_REFRESH_TOKEN", token)
		},
	}
	authHTTP := &http.Client{Timeout: 30 * time.Second}
	token, err := tidal.RefreshAccessToken(ctx, authHTTP, authCfg, refreshToken)
	if token == nil {
		return fmt.Errorf("refreshing TIDAL access token: %w", err)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: TIDAL refresh token rotated but not persisted (%v); the stored secret is now stale. Run 'tidal_sync login' before the next sync.\n", err)
	}
	currentRefresh := token.RefreshToken

	client := tidal.NewClient(token.AccessToken)
	client.CountryCode = *country
	client.TokenRefresher = func(c context.Context) (string, error) {
		t, err := tidal.RefreshAccessToken(c, authHTTP, authCfg, currentRefresh)
		if t == nil {
			return "", err
		}
		currentRefresh = t.RefreshToken
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: TIDAL refresh token rotated but not persisted (%v); the stored secret is now stale. Run 'tidal_sync login' before the next sync.\n", err)
		}
		return t.AccessToken, nil
	}

	fmt.Printf("Fetching favorite tracks for user %s (country: %s)...\n", token.UserID, *country)
	tracks, err := client.FetchFavoriteTracks(ctx, token.UserID, *limit)
	if err != nil {
		return fmt.Errorf("fetching favorite tracks: %w", err)
	}
	fmt.Printf("Fetched %d tracks from TIDAL.\n", len(tracks))

	// Open read model database
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	database, err := db.SetupDB(ctx, logger, *dataDir, false)
	if err != nil {
		return fmt.Errorf("opening SQLite database at %s: %w", *dataDir, err)
	}

	catalog, err := loadCatalogSongs(ctx, database)
	if err != nil {
		return fmt.Errorf("loading catalog songs: %w", err)
	}
	fmt.Printf("Loaded %d catalog songs from read model.\n", len(catalog))

	runID := fmt.Sprintf("tidalsync-%d", time.Now().UnixNano())
	now := time.Now().UTC()

	// If not dry-run, append tidalsync run stream
	if !*dryRun {
		if storeutil.IsLiveDataDir(*dataDir) && !*liveOK {
			return errors.New("cannot write events against live data dir without --live-ok (stop the app first)")
		}

		store, cleanup, err := storeutil.OpenEventStore(ctx, *dataDir, database)
		if err != nil {
			return fmt.Errorf("opening authoritative event store: %w", err)
		}
		defer cleanup()

		syncAgg, err := syncdomain.NewSync(runID, token.UserID, *country, now)
		if err != nil {
			return fmt.Errorf("creating sync aggregate: %w", err)
		}
		if len(tracks) == 0 {
			if err := syncAgg.RecordFavoritesObserved(1, nil, nil, nil); err != nil {
				return fmt.Errorf("recording observed tracks: %w", err)
			}
		} else {
			for page, chunk := 1, tracks; len(chunk) > 0; page++ {
				n := min(len(chunk), 500)
				if err := syncAgg.RecordFavoritesObserved(page, chunk[:n], nil, nil); err != nil {
					return fmt.Errorf("recording observed tracks page %d: %w", page, err)
				}
				chunk = chunk[n:]
			}
		}
		if err := syncAgg.RecordCompleted(time.Now().UTC()); err != nil {
			return fmt.Errorf("completing sync: %w", err)
		}
		syncRepo := syncdomain.NewRepository(store)
		if _, err := syncRepo.Save(ctx, syncAgg); err != nil {
			return fmt.Errorf("saving sync run to event store: %w", err)
		}
		fmt.Printf("✓ Recorded %d observed favorites in stream %s\n", len(tracks), runID)
	}

	// Reconcile
	opts := tidalsync.ReconcileOptions{
		MinConfidence:      *minConf,
		AutoLinkThreshold:  *autoLinkThresh,
		FillImpreciseDates: *fillImpreciseDates,
	}
	plans := tidalsync.Reconcile(tracks, catalog, opts)

	// Build report
	summary := make(map[string]int)
	reportItems := make([]ProposalPlanItemReport, len(plans))
	for i, p := range plans {
		summary[p.Kind]++
		reportItems[i] = ProposalPlanItemReport{
			ProposalPlan: p,
			Approved:     p.AutoApprove,
		}
	}

	rep := syncReport{
		Timestamp:   now,
		RunID:       runID,
		UserID:      token.UserID,
		CountryCode: *country,
		DryRun:      *dryRun,
		TracksCount: len(tracks),
		Summary:     summary,
		Proposals:   reportItems,
	}

	reportPath, csvPath, err := writeSyncReports(*reportDir, rep)
	if err != nil {
		return fmt.Errorf("writing reports: %w", err)
	}

	// Print summary
	fmt.Printf("\n--- TIDAL Sync Plan Summary ---\n")
	fmt.Printf("Total observed tracks:  %d\n", len(tracks))
	fmt.Printf("Proposals planned:\n")
	for k, count := range summary {
		fmt.Printf("  • %-15s: %d\n", k, count)
	}
	fmt.Printf("\nReports generated:\n  JSON: %s\n  CSV:  %s\n", reportPath, csvPath)
	if *dryRun {
		fmt.Printf("\n(Dry-run mode: no events were appended. Review the report, then run 'apply' to execute.)\n")
	}

	return nil
}

func afterString(m map[string]any, k, fallback string) string {
	if m == nil {
		return fallback
	}
	if v, ok := m[k].(string); ok && v != "" {
		return v
	}
	return fallback
}

func runApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	dataDir := fs.String("data-dir", appdir.ResolveDataDir(), "Data directory")
	reportFile := fs.String("report", "", "Path to sync report JSON file")
	approveAllAbove := fs.Float64("approve-all-above", 0.0, "Approve all proposals with confidence >= threshold")
	liveOK := fs.Bool("live-ok", false, "Acknowledge running against the live data directory")
	limit := fs.Int("limit", 0, "Max proposals to apply (0 = all)")
	_ = fs.Parse(args)

	if storeutil.IsLiveDataDir(*dataDir) && !*liveOK {
		return errors.New("cannot apply against live data dir without --live-ok (stop the app first)")
	}

	reportPath := strings.TrimSpace(*reportFile)
	if reportPath == "" {
		latest, err := findLatestReport("backfill_reports")
		if err != nil {
			return fmt.Errorf("no report specified and failed to find latest: %w", err)
		}
		reportPath = latest
		fmt.Printf("Using latest report: %s\n", reportPath)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		return fmt.Errorf("reading report file %s: %w", reportPath, err)
	}

	var rep syncReport
	if err := json.Unmarshal(data, &rep); err != nil {
		return fmt.Errorf("decoding report %s: %w", reportPath, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	database, err := db.SetupDB(ctx, logger, *dataDir, false)
	if err != nil {
		return fmt.Errorf("opening SQLite database: %w", err)
	}

	store, cleanup, err := storeutil.OpenEventStore(ctx, *dataDir, database)
	if err != nil {
		return fmt.Errorf("opening authoritative event store: %w", err)
	}
	defer cleanup()

	songRepo := song.NewRepository(store)
	propRepo := correctionproposal.NewRepository(store)
	catalogProj := projections.NewCatalogProjection(logger, database)

	var pbApp *pocketbase.PocketBase
	if _, err := os.Stat(filepath.Join(*dataDir, "data.db")); err == nil {
		app := pocketbase.NewWithConfig(pocketbase.Config{DefaultDataDir: *dataDir})
		if err := app.Bootstrap(); err != nil {
			logger.Warn("bootstrapping PocketBase mirror failed", "error", err)
		} else {
			pbApp = app
			defer func() {
				if err := pbApp.ClearBootstrap(); err != nil {
					logger.Warn("clearing PocketBase bootstrap state", "error", err)
				}
			}()
		}
	}

	now := time.Now().UTC()
	appliedCount := 0

	for i := range rep.Proposals {
		p := &rep.Proposals[i]
		if p.Applied {
			continue
		}

		prevTidal, _ := p.Before["tidal_id"].(string)
		isRelink := p.Kind == "link" && prevTidal != ""
		thresholdEligible := p.Kind != "addition" && p.Kind != "duplicate" && !isRelink && !p.RequiresReview
		shouldApply := p.Approved || (thresholdEligible && *approveAllAbove > 0 && p.Confidence >= *approveAllAbove)
		if !shouldApply {
			continue
		}

		// Apply proposal to song aggregate
		var songAgg *song.Song
		if p.Kind == "addition" {
			title, _ := p.After["title"].(string)
			artist, _ := p.After["artist_name"].(string)
			album, _ := p.After["album"].(string)
			relDate, _ := p.After["release_date"].(string)
			relType, _ := p.After["release_type"].(string)
			relYear := int64(songbackfill.ParseReleaseYear(relDate))
			newSong, err := song.NewSong(p.TargetID, title, artist, album, relDate, relType, "", relYear, 0, 0, false)
			if err != nil {
				p.Note = err.Error()
				continue
			}
			songAgg = newSong
		} else {
			loaded, err := songRepo.Load(ctx, p.TargetID)
			if err != nil {
				if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
					p.Note = err.Error()
					continue
				}
				seeded, seedErr := storeutil.SeedSongFromRow(ctx, database, p.TargetID)
				if seedErr != nil {
					p.Note = seedErr.Error()
					continue
				}
				loaded = seeded
			}
			songAgg = loaded
		}

		// Apply specific change
		var (
			cmdErr   error
			recorded bool
		)
		switch p.Kind {
		case "addition":
			// SongCreated is already pending on the aggregate.
			recorded = true
			tidalID, _ := p.After["tidal_id"].(string)
			if tidalID != "" {
				recorded, cmdErr = songAgg.LinkTidalID(tidalID, "tidal_sync", p.Confidence)
			}
		case "link":
			tidalID, _ := p.After["tidal_id"].(string)
			if songAgg.TidalID != prevTidal && songAgg.TidalID != tidalID {
				p.Note = fmt.Sprintf("stale proposal: song now linked to %s", songAgg.TidalID)
				continue
			}
			recorded, cmdErr = songAgg.LinkTidalID(tidalID, "tidal_sync", p.Confidence)
		case "fill":
			album := afterString(p.After, "album", songAgg.Album)
			relDate := afterString(p.After, "release_date", songAgg.ReleaseDate)
			relType := afterString(p.After, "release_type", songAgg.ReleaseType)
			recorded, cmdErr = songAgg.EnrichMetadata(album, relDate, relType, "tidal")
		case "album_name", "release_type":
			album := afterString(p.After, "album", songAgg.Album)
			relDate := afterString(p.After, "release_date", songAgg.ReleaseDate)
			relType := afterString(p.After, "release_type", songAgg.ReleaseType)
			recorded, cmdErr = songAgg.CorrectMetadata(album, relDate, relType, "tidal")
		case "credit":
			artist, _ := p.After["artist_name"].(string)
			recorded, cmdErr = songAgg.CorrectArtistCredits(artist, "tidal")
		case "duplicate":
			canonicalID, _ := p.After["duplicate_of"].(string)
			recorded, cmdErr = songAgg.MarkDuplicate(canonicalID, "tidal")
		}
		if cmdErr != nil {
			p.Note = cmdErr.Error()
			continue
		}
		if !recorded {
			// The stream or seeded row already reflects the proposal; saving
			// would record a no-op fact and mark it applied. A seed-only save
			// (pending SongCreated on a legacy row) does not need a proposal
			// fact either — the row converges on its next real write.
			p.Note = "no change: stream already reflects the proposal"
			continue
		}

		events, err := songRepo.Save(ctx, songAgg)
		if err != nil {
			p.Note = err.Error()
			continue
		}

		if err := catalogProj.Project(ctx, song.StreamTypeSong, events); err != nil {
			p.Note = fmt.Sprintf("projecting: %v", err)
			continue
		}

		if pbApp != nil {
			if err := storeutil.MirrorSongToPB(ctx, pbApp, songAgg); err != nil {
				logger.Warn("mirroring song to PocketBase failed", "song_id", songAgg.AggregateID(), "error", err)
			}
		}

		// Log proposal aggregate
		propID := fmt.Sprintf("prop-%s-%d", p.TargetID, time.Now().UnixNano())
		propAgg, err := correctionproposal.NewProposal(
			propID, rep.RunID, p.TargetType, p.TargetID, p.Kind,
			p.Before, p.After, p.Evidence, p.Confidence, p.TidalTrackID, now,
		)
		if err != nil {
			p.Note = fmt.Sprintf("creating proposal: %v", err)
			continue
		}
		if err := propAgg.Approve(now, "applied via CLI"); err != nil {
			p.Note = fmt.Sprintf("approving proposal: %v", err)
			continue
		}
		if err := propAgg.MarkApplied(now); err != nil {
			p.Note = fmt.Sprintf("marking proposal applied: %v", err)
			continue
		}
		if _, err := propRepo.Save(ctx, propAgg); err != nil {
			p.Note = fmt.Sprintf("saving proposal: %v", err)
			continue
		}

		p.Applied = true
		appliedCount++

		if *limit > 0 && appliedCount >= *limit {
			break
		}
	}

	// Update report with applied flags
	updatedJSON, err := json.Marshal(rep)
	if err != nil {
		return fmt.Errorf("marshaling updated report JSON: %w", err)
	}
	if err := os.WriteFile(reportPath, updatedJSON, 0644); err != nil {
		return fmt.Errorf("writing updated report JSON: %w", err)
	}

	fmt.Printf("\n✓ Successfully applied %d proposals into event store and catalog read model!\n", appliedCount)
	return nil
}

func loadCatalogSongs(ctx context.Context, database *toolbeltdb.Database) ([]tidalsync.CatalogSong, error) {
	query := `SELECT id, title, artist_name, album, release_date, release_year, release_type, spotify_id, tidal_id, duplicate_of
		FROM songs
		ORDER BY id;`

	var songs []tidalsync.CatalogSong
	err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			songs = append(songs, tidalsync.CatalogSong{
				ID:          stmt.ColumnText(0),
				Title:       norm.NFC.String(stmt.ColumnText(1)),
				ArtistName:  norm.NFC.String(stmt.ColumnText(2)),
				Album:       norm.NFC.String(stmt.ColumnText(3)),
				ReleaseDate: stmt.ColumnText(4),
				ReleaseYear: stmt.ColumnInt64(5),
				ReleaseType: stmt.ColumnText(6),
				SpotifyID:   stmt.ColumnText(7),
				TidalID:     stmt.ColumnText(8),
				DuplicateOf: stmt.ColumnText(9),
			})
		}
	})
	if err != nil {
		return nil, err
	}
	return songs, nil
}

func writeSyncReports(reportDir string, rep syncReport) (jsonPath, csvPath string, err error) {
	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return "", "", fmt.Errorf("creating report directory %s: %w", reportDir, err)
	}

	baseName := fmt.Sprintf("tidal_sync_%s", rep.Timestamp.Format("20060102_150405"))
	jsonPath = filepath.Join(reportDir, baseName+".json")
	csvPath = filepath.Join(reportDir, baseName+".csv")

	jsonData, err := json.Marshal(rep)
	if err != nil {
		return "", "", fmt.Errorf("marshaling report JSON: %w", err)
	}
	if err := os.WriteFile(jsonPath, jsonData, 0644); err != nil {
		return "", "", fmt.Errorf("writing report JSON: %w", err)
	}

	csvFile, err := os.Create(csvPath)
	if err != nil {
		return "", "", fmt.Errorf("creating report CSV: %w", err)
	}
	defer func() { _ = csvFile.Close() }()

	// Write UTF-8 BOM so LibreOffice Calc and Excel automatically parse diacritics correctly
	if _, err := csvFile.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		return "", "", fmt.Errorf("writing UTF-8 BOM to report CSV: %w", err)
	}

	w := csv.NewWriter(csvFile)

	header := []string{"Kind", "TargetID", "Title", "Artist", "Confidence", "AutoApprove", "Evidence", "TidalTrackID", "Before", "After"}
	if err := w.Write(header); err != nil {
		return "", "", err
	}

	for _, p := range rep.Proposals {
		beforeJSON, _ := json.Marshal(p.Before)
		afterJSON, _ := json.Marshal(p.After)
		row := []string{
			p.Kind,
			p.TargetID,
			p.Title,
			p.Artist,
			fmt.Sprintf("%.2f", p.Confidence),
			strconv.FormatBool(p.AutoApprove),
			p.Evidence,
			p.TidalTrackID,
			string(beforeJSON),
			string(afterJSON),
		}
		if err := w.Write(row); err != nil {
			return "", "", err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return "", "", fmt.Errorf("flushing report CSV: %w", err)
	}
	if err := csvFile.Close(); err != nil {
		return "", "", fmt.Errorf("closing report CSV: %w", err)
	}

	return jsonPath, csvPath, nil
}

func findLatestReport(reportDir string) (string, error) {
	var files []string
	err := filepath.WalkDir(reportDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), "tidal_sync_") && strings.HasSuffix(d.Name(), ".json") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", errors.New("no tidal_sync reports found")
	}
	slices.Sort(files)
	return files[len(files)-1], nil
}
