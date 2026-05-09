//go:build goexperiment.jsonv2

// Package main implements the seed command, which reads CSV files of artists
// and albums and upserts them into the ListenLedger PocketBase database.
// Run with --dry-run to preview changes without writing.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/appdir"
)

func main() {
	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDataDir: appdir.ResolveDataDir(),
	})

	opts := seedOptions{
		dryRun:           flag.Bool("dry-run", false, "Show what would be seeded without making changes"),
		sheet1:           flag.String("sheet1", "Music - Sheet1.csv", "Path to Sheet1 CSV file"),
		sheet2:           flag.String("sheet2", "Music - Sheet2.csv", "Path to Sheet2 CSV file"),
		sheet2GenreGroup: flag.String("sheet2-genre-group", "rock_metal", "Genre group for artists seeded from Sheet2"),
	}
	flag.Parse()

	if err := app.Bootstrap(); err != nil {
		log.Fatal(err)
	}

	if *opts.dryRun {
		log.Println("[seed] Dry run mode - no changes will be made")
	}

	ctx := context.Background()
	if err := runSeed(ctx, app, opts); err != nil {
		log.Fatalf("[seed] Failed: %v", err)
	}

	log.Println("[seed] Completed successfully")
}

type seedOptions struct {
	dryRun           *bool
	sheet1           *string
	sheet2           *string
	sheet2GenreGroup *string
}

func runSeed(ctx context.Context, app *pocketbase.PocketBase, opts seedOptions) error {
	if err := seedAlbums(ctx, app, *opts.dryRun, *opts.sheet1); err != nil {
		return fmt.Errorf("failed to seed albums: %w", err)
	}

	if err := seedArtistsFromSheet1(ctx, app, *opts.dryRun, *opts.sheet1); err != nil {
		return fmt.Errorf("failed to seed artists from Sheet1: %w", err)
	}

	if err := seedFromSheet2(ctx, app, *opts.dryRun, *opts.sheet2, *opts.sheet2GenreGroup); err != nil {
		return fmt.Errorf("failed to seed from Sheet2: %w", err)
	}

	return nil
}

func readCSVRecords(path, sheetName string) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", sheetName, err)
	}
	defer func() { _ = file.Close() }()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", sheetName, err)
	}
	return records, nil
}

func seedAlbums(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet1Path string) error {
	records, err := readCSVRecords(sheet1Path, "Sheet1")
	if err != nil {
		return err
	}

	collection, err := app.FindCollectionByNameOrId("albums")
	if err != nil {
		return fmt.Errorf("albums collection not found: %w", err)
	}

	cfg := albumSeedConfig{app: app, collection: collection, dryRun: dryRun}
	count := 0
	for i, row := range records {
		if i == 0 || len(row) < 5 {
			continue
		}
		count += cfg.seedAlbumRow(ctx, row)
	}

	log.Printf("[seed] %s %d album records", dryRunAction(dryRun), count)
	return nil
}

type albumSeedConfig struct {
	app        *pocketbase.PocketBase
	collection *core.Collection
	dryRun     bool
}

func (cfg albumSeedConfig) seedAlbumRow(ctx context.Context, row []string) int {
	title := strings.TrimSpace(row[0])
	artistName := strings.TrimSpace(row[1])
	if title == "" || artistName == "" {
		return 0
	}

	collectionSongs := parseNumber(row[2])
	totalSongs := parseNumber(row[3])
	status := determineAlbumStatus(collectionSongs, totalSongs)

	if cfg.dryRun {
		log.Printf("[seed] Would create album: %q by %q (%d/%d, %s)", title, artistName, collectionSongs, totalSongs, status)
		return 1
	}

	record := core.NewRecord(cfg.collection)
	record.Set("title", title)
	record.Set("artist_name", artistName)
	record.Set("collection_songs", collectionSongs)
	record.Set("total_songs", totalSongs)
	record.Set("status", status)

	if err := cfg.app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[seed] Warning: failed to save album %q: %v", title, err)
		return 0
	}
	return 1
}

type artistColumnMapping struct {
	Name            int
	SpotifyID       int
	Listeners       int
	CollectionSongs int
	TotalSongs      int
}

func seedArtistsFromSheet1(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet1Path string) error {
	records, err := readCSVRecords(sheet1Path, "Sheet1")
	if err != nil {
		return err
	}

	collection, err := app.FindCollectionByNameOrId("artists")
	if err != nil {
		return fmt.Errorf("artists collection not found: %w", err)
	}

	sc := seedContext{app: app, collection: collection, dryRun: dryRun, seen: make(map[string]bool)}

	rockMetalMapping := artistColumnMapping{Name: 7, SpotifyID: 8, Listeners: 9, CollectionSongs: 10, TotalSongs: 11}
	everythingElseMapping := artistColumnMapping{Name: 13, SpotifyID: 14, Listeners: 15, CollectionSongs: 16, TotalSongs: 17}

	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) > 11 {
			sc.seedArtistGenreGroup(ctx, row, rockMetalMapping, "rock_metal")
		}
		if len(row) > 17 {
			sc.seedArtistGenreGroup(ctx, row, everythingElseMapping, "everything_else")
		}
	}

	log.Printf("[seed] %s %d rock_metal artists, %d everything_else artists", dryRunAction(dryRun), sc.rockMetalCount, sc.everythingElseCount)
	return nil
}

type seedContext struct {
	app                 *pocketbase.PocketBase
	collection          *core.Collection
	dryRun              bool
	seen                map[string]bool
	rockMetalCount      int
	everythingElseCount int
}

func (c *seedContext) incrementGenreCount(genreGroup string) {
	switch genreGroup {
	case "rock_metal":
		c.rockMetalCount++
	case "everything_else":
		c.everythingElseCount++
	default:
		log.Printf("[seed] Warning: unknown genre_group %q, not incrementing any count", genreGroup)
	}
}

func isArtistDuplicate(name, spotifyID string, seen map[string]bool) bool {
	return name == "" || spotifyID == "" || seen[spotifyID]
}

func (c *seedContext) seedArtistGenreGroup(ctx context.Context, row []string, cols artistColumnMapping, genreGroup string) {
	name := strings.TrimSpace(row[cols.Name])
	spotifyID := strings.TrimSpace(row[cols.SpotifyID])
	if isArtistDuplicate(name, spotifyID, c.seen) {
		return
	}

	listeners, err := parseListenersStrict(strings.TrimSpace(row[cols.Listeners]))
	if err != nil {
		log.Printf("[seed] Warning: failed to parse listeners for %s artist %q: %v", genreGroup, name, err)
		return
	}
	collectionSongs := parseNumber(row[cols.CollectionSongs])
	totalSongs := parseNumber(row[cols.TotalSongs])

	if c.dryRun {
		log.Printf("[seed] Would create %s artist: %q (%s, %d listeners)", genreGroup, name, spotifyID, listeners)
		c.seen[spotifyID] = true
		c.incrementGenreCount(genreGroup)
		return
	}

	record := core.NewRecord(c.collection)
	record.Set("name", name)
	record.Set("spotify_id", spotifyID)
	record.Set("monthly_listeners", listeners)
	record.Set("genre_group", genreGroup)
	record.Set("list_status", "included")
	record.Set("fetch_status", "idle")
	record.Set("collection_songs", collectionSongs)
	record.Set("total_songs", totalSongs)

	if err := c.app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[seed] Warning: failed to save %s artist %q: %v", genreGroup, name, err)
		return
	}
	c.seen[spotifyID] = true
	c.incrementGenreCount(genreGroup)
}

type sheet2Config struct {
	app               *pocketbase.PocketBase
	dryRun            bool
	songsCollection   *core.Collection
	artistsCollection *core.Collection
	genreGroup        string
}

func seedFromSheet2(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet2Path, genreGroup string) error {
	if genreGroup == "" {
		return fmt.Errorf("sheet2-genre-group must not be empty")
	}
	records, err := readCSVRecords(sheet2Path, "Sheet2")
	if err != nil {
		return err
	}

	songsCollection, err := app.FindCollectionByNameOrId("songs")
	if err != nil {
		return fmt.Errorf("songs collection not found: %w", err)
	}

	artistsCollection, err := app.FindCollectionByNameOrId("artists")
	if err != nil {
		return fmt.Errorf("artists collection not found: %w", err)
	}

	cfg := sheet2Config{
		app:               app,
		dryRun:            dryRun,
		songsCollection:   songsCollection,
		artistsCollection: artistsCollection,
		genreGroup:        genreGroup,
	}

	songCount, artistUpsertCount := cfg.processSheet2Rows(ctx, records)

	if dryRun {
		log.Printf("[seed] Would create %d song records, would upsert %d artists from Sheet2", songCount, artistUpsertCount)
	} else {
		log.Printf("[seed] Created %d song records, upserted %d artists from Sheet2", songCount, artistUpsertCount)
	}
	return nil
}

func isSongDuplicate(title, artistName string, added map[string]bool) bool {
	return title == "" || artistName == "" || added[title+"|"+artistName]
}

func (cfg sheet2Config) processSheet2Rows(ctx context.Context, records [][]string) (int, int) {
	songCount := 0
	artistUpsertCount := 0
	addedSongs := make(map[string]bool)
	processedArtists := make(map[string]bool)

	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) > 5 {
			songCount += cfg.seedSongRow(ctx, row, addedSongs)
		}
		artistUpsertCount += cfg.processArtistRow(ctx, row, processedArtists)
	}
	return songCount, artistUpsertCount
}

func (cfg sheet2Config) processArtistRow(ctx context.Context, row []string, processed map[string]bool) int {
	if len(row) <= 9 {
		return 0
	}
	spotifyID := strings.TrimSpace(row[8])
	if spotifyID == "" || processed[spotifyID] {
		return 0
	}
	upsertCount := cfg.upsertArtistRow(ctx, row)
	if upsertCount > 0 {
		processed[spotifyID] = true
	}
	return upsertCount
}

func (cfg sheet2Config) seedSongRow(ctx context.Context, row []string, added map[string]bool) int {
	songTitle := strings.TrimSpace(row[3])
	artistName := strings.TrimSpace(row[4])
	releaseDate := strings.TrimSpace(row[5])

	if isSongDuplicate(songTitle, artistName, added) {
		return 0
	}

	if cfg.dryRun {
		log.Printf("[seed] Would create song: %q by %q (%s)", songTitle, artistName, releaseDate)
		added[songTitle+"|"+artistName] = true
		return 1
	}

	record := core.NewRecord(cfg.songsCollection)
	record.Set("title", songTitle)
	record.Set("artist_name", artistName)
	record.Set("release_date", releaseDate)
	record.Set("is_recent", true)

	if err := cfg.app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[seed] Warning: failed to save song %q: %v", songTitle, err)
		return 0
	}
	added[songTitle+"|"+artistName] = true
	return 1
}

func (cfg sheet2Config) upsertArtistRow(ctx context.Context, row []string) int {
	bandName := strings.TrimSpace(row[7])
	spotifyID := strings.TrimSpace(row[8])
	listenersStr := strings.TrimSpace(row[9])

	if bandName == "" || spotifyID == "" {
		return 0
	}
	listeners, err := parseListenersStrict(listenersStr)
	if err != nil {
		log.Printf("[seed] Warning: failed to parse listeners %q for artist %q: %v", listenersStr, bandName, err)
		return 0
	}
	if listeners == 0 {
		log.Printf("[seed] Skipping artist %q with zero listeners", bandName)
		return 0
	}

	if cfg.dryRun {
		return cfg.logDryRunArtistUpsert(ctx, bandName, spotifyID, listeners)
	}
	return cfg.saveArtistUpsert(ctx, bandName, spotifyID, listeners)
}

func findArtistBySpotifyID(ctx context.Context, app *pocketbase.PocketBase, collection *core.Collection, spotifyID string) (*core.Record, error) {
	records := make([]*core.Record, 0)
	err := app.RecordQuery(collection.Id).
		WithContext(ctx).
		AndWhere(dbx.NewExp("spotify_id = {:spotifyId}", dbx.Params{"spotifyId": spotifyID})).
		OrderBy("created DESC").
		Limit(1).
		All(&records)
	if err != nil {
		return nil, fmt.Errorf("failed to find artist by spotify id %s in collection %s: %w", spotifyID, collection.Id, err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

func (cfg sheet2Config) logDryRunArtistUpsert(ctx context.Context, bandName, spotifyID string, listeners int) int {
	existing, err := findArtistBySpotifyID(ctx, cfg.app, cfg.artistsCollection, spotifyID)
	if err != nil {
		log.Printf("[seed] Warning: lookup failed for artist %q (%s): %v", bandName, spotifyID, err)
		return 0
	}
	if existing != nil {
		log.Printf("[seed] Would update artist: %q (%d listeners)", bandName, listeners)
		return 1
	}
	log.Printf("[seed] Would create artist: %q (%s, %d listeners)", bandName, spotifyID, listeners)
	return 1
}

func (cfg sheet2Config) saveArtistUpsert(ctx context.Context, bandName, spotifyID string, listeners int) int {
	existing, err := findArtistBySpotifyID(ctx, cfg.app, cfg.artistsCollection, spotifyID)
	if err != nil {
		log.Printf("[seed] Warning: lookup failed for artist %q (%s): %v", bandName, spotifyID, err)
		return 0
	}
	if existing != nil {
		existing.Set("monthly_listeners", listeners)
		if err := cfg.app.SaveWithContext(ctx, existing); err != nil {
			log.Printf("[seed] Warning: failed to update artist %q: %v", bandName, err)
			return 0
		}
		return 1
	}

	record := core.NewRecord(cfg.artistsCollection)
	record.Set("name", bandName)
	record.Set("spotify_id", spotifyID)
	record.Set("monthly_listeners", listeners)
	record.Set("genre_group", cfg.genreGroup)
	record.Set("list_status", "waiting")
	record.Set("fetch_status", "idle")

	if err := cfg.app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[seed] Warning: failed to create artist %q: %v", bandName, err)
		return 0
	}
	return 1
}

func determineAlbumStatus(collectionSongs, totalSongs int) string {
	if totalSongs == 0 {
		return "waiting"
	}

	if collectionSongs == totalSongs {
		return "full"
	}

	percentage := float64(collectionSongs) / float64(totalSongs) * 100
	if percentage > 15.5 {
		return "processed_once"
	}

	return "waiting"
}

func parseNumber(s string) int {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSuffix(s, "%")

	n, _ := strconv.Atoi(s)
	return n
}

func parseListenersStrict(s string) (int, error) {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "\"", "")

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("parseListenersStrict: invalid listeners %q: %w", s, err)
	}
	return n, nil
}

func dryRunAction(dryRun bool) string {
	if dryRun {
		return "Would create"
	}
	return "Created"
}
