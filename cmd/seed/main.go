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

	dryRun := flag.Bool("dry-run", false, "Show what would be seeded without making changes")
	sheet1 := flag.String("sheet1", "Music - Sheet1.csv", "Path to Sheet1 CSV file")
	sheet2 := flag.String("sheet2", "Music - Sheet2.csv", "Path to Sheet2 CSV file")
	sheet2GenreGroup := flag.String("sheet2-genre-group", "rock_metal", "Genre group for artists seeded from Sheet2")
	flag.Parse()

	if err := app.Bootstrap(); err != nil {
		log.Fatal(err)
	}

	if *dryRun {
		log.Println("[seed] Dry run mode - no changes will be made")
	}

	ctx := context.Background()
	if err := runSeed(ctx, app, *dryRun, *sheet1, *sheet2, *sheet2GenreGroup); err != nil {
		log.Fatalf("[seed] Failed: %v", err)
	}

	log.Println("[seed] Completed successfully")
}

func runSeed(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet1Path, sheet2Path, sheet2GenreGroup string) error {
	if err := seedAlbums(ctx, app, dryRun, sheet1Path); err != nil {
		return fmt.Errorf("failed to seed albums: %w", err)
	}

	if err := seedArtistsFromSheet1(ctx, app, dryRun, sheet1Path); err != nil {
		return fmt.Errorf("failed to seed artists from Sheet1: %w", err)
	}

	if err := seedFromSheet2(ctx, app, dryRun, sheet2Path, sheet2GenreGroup); err != nil {
		return fmt.Errorf("failed to seed from Sheet2: %w", err)
	}

	return nil
}

// seedAlbums reads Sheet1 CSV at sheet1Path and creates album records in the "albums" collection.
// It skips the header row and any rows missing title or artist, parses collection and total song counts,
// determines album status, and either logs the intended creations when dryRun is true or saves records.
// Returns an error if the CSV file cannot be opened or read, or if the albums collection cannot be located.
// Individual record save failures are logged and do not abort processing.
func seedAlbums(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet1Path string) error {
	file, err := os.Open(sheet1Path)
	if err != nil {
		return fmt.Errorf("failed to open Sheet1: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read Sheet1: %w", err)
	}

	collection, err := app.FindCollectionByNameOrId("albums")
	if err != nil {
		return fmt.Errorf("albums collection not found: %w", err)
	}

	count := 0
	for i, row := range records {
		if i == 0 || len(row) < 5 {
			continue
		}
		count += seedAlbumRow(ctx, app, collection, dryRun, row)
	}

	log.Printf("[seed] %s %d album records", dryRunAction(dryRun), count)
	return nil
}

func seedAlbumRow(ctx context.Context, app *pocketbase.PocketBase, collection *core.Collection, dryRun bool, row []string) int {
	title := strings.TrimSpace(row[0])
	artistName := strings.TrimSpace(row[1])
	if title == "" || artistName == "" {
		return 0
	}

	collectionSongs := parseNumber(row[2])
	totalSongs := parseNumber(row[3])
	status := determineAlbumStatus(collectionSongs, totalSongs)

	if dryRun {
		log.Printf("[seed] Would create album: %q by %q (%d/%d, %s)", title, artistName, collectionSongs, totalSongs, status)
		return 1
	}

	record := core.NewRecord(collection)
	record.Set("title", title)
	record.Set("artist_name", artistName)
	record.Set("collection_songs", collectionSongs)
	record.Set("total_songs", totalSongs)
	record.Set("status", status)

	if err := app.SaveWithContext(ctx, record); err != nil {
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

// seedArtistsFromSheet1 reads artists from the given Sheet1 CSV and creates artist records for two genre groups ("rock_metal" and "everything_else").
//
// It extracts fields (name, spotify_id, monthly_listeners, collection_songs, total_songs) from two separate column ranges per row, deduplicates by spotify_id, and either logs the would-be actions when dryRun is true or saves new records to the "artists" collection.
//
// Returns an error if the CSV file cannot be opened or read, or if the "artists" collection cannot be located.
func seedArtistsFromSheet1(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet1Path string) error {
	file, err := os.Open(sheet1Path)
	if err != nil {
		return fmt.Errorf("failed to open Sheet1: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read Sheet1: %w", err)
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

func (c *seedContext) seedArtistGenreGroup(ctx context.Context, row []string, cols artistColumnMapping, genreGroup string) {
	name := strings.TrimSpace(row[cols.Name])
	spotifyID := strings.TrimSpace(row[cols.SpotifyID])
	if name == "" || spotifyID == "" || c.seen[spotifyID] {
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

func seedFromSheet2(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, sheet2Path, genreGroup string) error {
	if genreGroup == "" {
		return fmt.Errorf("sheet2-genre-group must not be empty")
	}
	file, err := os.Open(sheet2Path)
	if err != nil {
		return fmt.Errorf("failed to open Sheet2: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read Sheet2: %w", err)
	}

	songsCollection, err := app.FindCollectionByNameOrId("songs")
	if err != nil {
		return fmt.Errorf("songs collection not found: %w", err)
	}

	artistsCollection, err := app.FindCollectionByNameOrId("artists")
	if err != nil {
		return fmt.Errorf("artists collection not found: %w", err)
	}

	songCount := 0
	artistUpsertCount := 0
	addedSongs := make(map[string]bool)
	processedArtists := make(map[string]bool)

	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) > 5 {
			songCount += seedSongFromSheet2Row(ctx, app, dryRun, songsCollection, row, addedSongs)
		}
		if len(row) > 9 {
			spotifyID := strings.TrimSpace(row[8])
			if spotifyID == "" || processedArtists[spotifyID] {
				continue
			}
			upsertCount := upsertArtistFromSheet2(ctx, app, dryRun, artistsCollection, row, genreGroup)
			if upsertCount > 0 {
				processedArtists[spotifyID] = true
				artistUpsertCount += upsertCount
			}
		}
	}

	if dryRun {
		log.Printf("[seed] Would create %d song records, would upsert %d artists from Sheet2", songCount, artistUpsertCount)
	} else {
		log.Printf("[seed] Created %d song records, upserted %d artists from Sheet2", songCount, artistUpsertCount)
	}
	return nil
}

func seedSongFromSheet2Row(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, collection *core.Collection, row []string, added map[string]bool) int {
	songTitle := strings.TrimSpace(row[3])
	artistName := strings.TrimSpace(row[4])
	releaseDate := strings.TrimSpace(row[5])

	songKey := songTitle + "|" + artistName
	if songTitle == "" || artistName == "" || added[songKey] {
		return 0
	}

	if dryRun {
		log.Printf("[seed] Would create song: %q by %q (%s)", songTitle, artistName, releaseDate)
		added[songKey] = true
		return 1
	}

	record := core.NewRecord(collection)
	record.Set("title", songTitle)
	record.Set("artist_name", artistName)
	record.Set("release_date", releaseDate)
	record.Set("is_recent", true)

	if err := app.SaveWithContext(ctx, record); err != nil {
		log.Printf("[seed] Warning: failed to save song %q: %v", songTitle, err)
		return 0
	}
	added[songKey] = true
	return 1
}

func upsertArtistFromSheet2(ctx context.Context, app *pocketbase.PocketBase, dryRun bool, collection *core.Collection, row []string, genreGroup string) int {
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

	if dryRun {
		return logDryRunArtistUpsert(ctx, app, collection, bandName, spotifyID, listeners, genreGroup)
	}
	return saveArtistUpsert(ctx, app, collection, bandName, spotifyID, listeners, genreGroup)
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

func logDryRunArtistUpsert(ctx context.Context, app *pocketbase.PocketBase, collection *core.Collection, bandName, spotifyID string, listeners int, genreGroup string) int {
	existing, err := findArtistBySpotifyID(ctx, app, collection, spotifyID)
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

func saveArtistUpsert(ctx context.Context, app *pocketbase.PocketBase, collection *core.Collection, bandName, spotifyID string, listeners int, genreGroup string) int {
	existing, err := findArtistBySpotifyID(ctx, app, collection, spotifyID)
	if err != nil {
		log.Printf("[seed] Warning: lookup failed for artist %q (%s): %v", bandName, spotifyID, err)
		return 0
	}
	if existing != nil {
		existing.Set("monthly_listeners", listeners)
		if err := app.SaveWithContext(ctx, existing); err != nil {
			log.Printf("[seed] Warning: failed to update artist %q: %v", bandName, err)
			return 0
		}
		return 1
	}

	record := core.NewRecord(collection)
	record.Set("name", bandName)
	record.Set("spotify_id", spotifyID)
	record.Set("monthly_listeners", listeners)
	record.Set("genre_group", genreGroup)
	record.Set("list_status", "waiting")
	record.Set("fetch_status", "idle")

	if err := app.SaveWithContext(ctx, record); err != nil {
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

func parseListeners(s string) int {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "\"", "")

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
