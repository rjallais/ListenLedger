//go:build goexperiment.jsonv2

// Package main implements the seed command, which reads CSV files of artists
// and albums and upserts them into the ListenLedger PocketBase database.
// Run with --dry-run to preview changes without writing.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

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
	flag.Parse()

	if err := app.Bootstrap(); err != nil {
		log.Fatal(err)
	}

	if *dryRun {
		log.Println("[seed] Dry run mode - no changes will be made")
	}

	if err := runSeed(app, *dryRun, *sheet1, *sheet2); err != nil {
		log.Fatalf("[seed] Failed: %v", err)
	}

	log.Println("[seed] Completed successfully")
}

func runSeed(app *pocketbase.PocketBase, dryRun bool, sheet1Path, sheet2Path string) error {
	if err := seedAlbums(app, dryRun, sheet1Path); err != nil {
		return fmt.Errorf("failed to seed albums: %w", err)
	}

	if err := seedArtistsFromSheet1(app, dryRun, sheet1Path); err != nil {
		return fmt.Errorf("failed to seed artists from Sheet1: %w", err)
	}

	if err := seedFromSheet2(app, dryRun, sheet2Path); err != nil {
		return fmt.Errorf("failed to seed from Sheet2: %w", err)
	}

	return nil
}

// seedAlbums reads Sheet1 CSV at sheet1Path and creates album records in the "albums" collection.
// It skips the header row and any rows missing title or artist, parses collection and total song counts,
// determines album status, and either logs the intended creations when dryRun is true or saves records.
// Returns an error if the CSV file cannot be opened or read, or if the albums collection cannot be located.
// Individual record save failures are logged and do not abort processing.
func seedAlbums(app *pocketbase.PocketBase, dryRun bool, sheet1Path string) error {
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
		count += seedAlbumRow(app, collection, dryRun, row)
	}

	log.Printf("[seed] %s %d album records", dryRunAction(dryRun), count)
	return nil
}

func seedAlbumRow(app *pocketbase.PocketBase, collection *core.Collection, dryRun bool, row []string) int {
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

	if err := app.Save(record); err != nil {
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
func seedArtistsFromSheet1(app *pocketbase.PocketBase, dryRun bool, sheet1Path string) error {
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

	ctx := seedContext{app: app, collection: collection, dryRun: dryRun, seen: make(map[string]bool)}

	rockMetalMapping := artistColumnMapping{Name: 7, SpotifyID: 8, Listeners: 9, CollectionSongs: 10, TotalSongs: 11}
	everythingElseMapping := artistColumnMapping{Name: 13, SpotifyID: 14, Listeners: 15, CollectionSongs: 16, TotalSongs: 17}

	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) > 11 {
			ctx.rockMetalCount += ctx.seedArtistGenreGroup(row, rockMetalMapping, "rock_metal")
		}
		if len(row) > 17 {
			ctx.everythingElseCount += ctx.seedArtistGenreGroup(row, everythingElseMapping, "everything_else")
		}
	}

	log.Printf("[seed] %s %d rock_metal artists, %d everything_else artists", dryRunAction(dryRun), ctx.rockMetalCount, ctx.everythingElseCount)
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

func (c *seedContext) seedArtistGenreGroup(row []string, cols artistColumnMapping, genreGroup string) int {
	name := strings.TrimSpace(row[cols.Name])
	spotifyID := strings.TrimSpace(row[cols.SpotifyID])
	if name == "" || spotifyID == "" || c.seen[spotifyID] {
		return 0
	}

	listeners := parseListeners(strings.TrimSpace(row[cols.Listeners]))
	collectionSongs := parseNumber(row[cols.CollectionSongs])
	totalSongs := parseNumber(row[cols.TotalSongs])

	if c.dryRun {
		log.Printf("[seed] Would create %s artist: %q (%s, %d listeners)", genreGroup, name, spotifyID, listeners)
		c.seen[spotifyID] = true
		return 1
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

	if err := c.app.Save(record); err != nil {
		log.Printf("[seed] Warning: failed to save %s artist %q: %v", genreGroup, name, err)
		return 0
	}
	c.seen[spotifyID] = true
	return 1
}

func seedFromSheet2(app *pocketbase.PocketBase, dryRun bool, sheet2Path string) error {
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

	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) > 5 {
			songCount += seedSongFromSheet2Row(app, dryRun, songsCollection, row, addedSongs)
		}
		if len(row) > 9 {
			artistUpsertCount += upsertArtistFromSheet2(app, dryRun, artistsCollection, row)
		}
	}

	log.Printf("[seed] %s %d song records, %d artist upserts from Sheet2", dryRunAction(dryRun), songCount, artistUpsertCount)
	return nil
}

func seedSongFromSheet2Row(app *pocketbase.PocketBase, dryRun bool, collection *core.Collection, row []string, added map[string]bool) int {
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

	if err := app.Save(record); err != nil {
		log.Printf("[seed] Warning: failed to save song %q: %v", songTitle, err)
		return 0
	}
	added[songKey] = true
	return 1
}

func upsertArtistFromSheet2(app *pocketbase.PocketBase, dryRun bool, collection *core.Collection, row []string) int {
	bandName := strings.TrimSpace(row[7])
	spotifyID := strings.TrimSpace(row[8])
	listenersStr := strings.TrimSpace(row[9])

	if bandName == "" || spotifyID == "" {
		return 0
	}
	listeners := parseListeners(listenersStr)
	if listeners == 0 {
		return 0
	}

	if dryRun {
		return logDryRunArtistUpsert(app, collection, bandName, spotifyID, listeners)
	}
	return saveArtistUpsert(app, collection, bandName, spotifyID, listeners)
}

func findArtistBySpotifyID(app *pocketbase.PocketBase, collection *core.Collection, spotifyID string) (*core.Record, error) {
	records, err := app.FindRecordsByFilter(
		collection.Id, "spotify_id = {:spotifyId}", "-created", 1, 0,
		map[string]any{"spotifyId": spotifyID},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to find artist by spotify id %s in collection %s: %w", spotifyID, collection.Id, err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

func logDryRunArtistUpsert(app *pocketbase.PocketBase, collection *core.Collection, bandName, spotifyID string, listeners int) int {
	existing, err := findArtistBySpotifyID(app, collection, spotifyID)
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

func saveArtistUpsert(app *pocketbase.PocketBase, collection *core.Collection, bandName, spotifyID string, listeners int) int {
	existing, err := findArtistBySpotifyID(app, collection, spotifyID)
	if err != nil {
		log.Printf("[seed] Warning: lookup failed for artist %q (%s): %v", bandName, spotifyID, err)
		return 0
	}
	if existing != nil {
		existing.Set("monthly_listeners", listeners)
		if err := app.Save(existing); err != nil {
			log.Printf("[seed] Warning: failed to update artist %q: %v", bandName, err)
			return 0
		}
		return 1
	}

	record := core.NewRecord(collection)
	record.Set("name", bandName)
	record.Set("spotify_id", spotifyID)
	record.Set("monthly_listeners", listeners)
	// Sheet2 contains rock/metal artist data exclusively; all imports are classified as rock_metal.
	record.Set("genre_group", "rock_metal")
	record.Set("list_status", "waiting")
	record.Set("fetch_status", "idle")

	if err := app.Save(record); err != nil {
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

func dryRunAction(dryRun bool) string {
	if dryRun {
		return "Would create"
	}
	return "Created"
}
