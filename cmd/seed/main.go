//go:build goexperiment.jsonv2

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
		if i == 0 {
			continue
		}

		if len(row) < 5 {
			continue
		}

		title := strings.TrimSpace(row[0])
		artistName := strings.TrimSpace(row[1])

		if title == "" || artistName == "" {
			continue
		}

		collectionSongs := parseNumber(row[2])
		totalSongs := parseNumber(row[3])
		status := determineAlbumStatus(collectionSongs, totalSongs)

		if dryRun {
			log.Printf("[seed] Would create album: %q by %q (%d/%d, %s)", title, artistName, collectionSongs, totalSongs, status)
			count++
			continue
		}

		record := core.NewRecord(collection)
		record.Set("title", title)
		record.Set("artist_name", artistName)
		record.Set("collection_songs", collectionSongs)
		record.Set("total_songs", totalSongs)
		record.Set("status", status)

		if err := app.Save(record); err != nil {
			log.Printf("[seed] Warning: failed to save album %q: %v", title, err)
			continue
		}
		count++
	}

	log.Printf("[seed] %s %d album records", dryRunAction(dryRun), count)
	return nil
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

	existingArtists := make(map[string]bool)
	rockMetalCount := 0
	everythingElseCount := 0

	for i, row := range records {
		if i == 0 {
			continue
		}

		if len(row) > 11 {
			name := strings.TrimSpace(row[7])
			spotifyID := strings.TrimSpace(row[8])
			listenersStr := strings.TrimSpace(row[9])
			collectionSongs := parseNumber(row[10])
			totalSongs := parseNumber(row[11])

			if name != "" && spotifyID != "" && !existingArtists[spotifyID] {
				listeners := parseListeners(listenersStr)

				if dryRun {
					log.Printf("[seed] Would create rock_metal artist: %q (%s, %d listeners)", name, spotifyID, listeners)
					existingArtists[spotifyID] = true
					rockMetalCount++
					continue
				}

				record := core.NewRecord(collection)
				record.Set("name", name)
				record.Set("spotify_id", spotifyID)
				record.Set("monthly_listeners", listeners)
				record.Set("genre_group", "rock_metal")
				record.Set("list_status", "included")
				record.Set("fetch_status", "idle")
				record.Set("collection_songs", collectionSongs)
				record.Set("total_songs", totalSongs)

				if err := app.Save(record); err != nil {
					log.Printf("[seed] Warning: failed to save rock_metal artist %q: %v", name, err)
				} else {
					existingArtists[spotifyID] = true
					rockMetalCount++
				}
			}
		}

		if len(row) > 17 {
			name := strings.TrimSpace(row[13])
			spotifyID := strings.TrimSpace(row[14])
			listenersStr := strings.TrimSpace(row[15])
			collectionSongs := parseNumber(row[16])
			totalSongs := parseNumber(row[17])

			if name != "" && spotifyID != "" && !existingArtists[spotifyID] {
				listeners := parseListeners(listenersStr)

				if dryRun {
					log.Printf("[seed] Would create everything_else artist: %q (%s, %d listeners)", name, spotifyID, listeners)
					existingArtists[spotifyID] = true
					everythingElseCount++
					continue
				}

				record := core.NewRecord(collection)
				record.Set("name", name)
				record.Set("spotify_id", spotifyID)
				record.Set("monthly_listeners", listeners)
				record.Set("genre_group", "everything_else")
				record.Set("list_status", "included")
				record.Set("fetch_status", "idle")
				record.Set("collection_songs", collectionSongs)
				record.Set("total_songs", totalSongs)

				if err := app.Save(record); err != nil {
					log.Printf("[seed] Warning: failed to save everything_else artist %q: %v", name, err)
				} else {
					existingArtists[spotifyID] = true
					everythingElseCount++
				}
			}
		}
	}

	log.Printf("[seed] %s %d rock_metal artists, %d everything_else artists", dryRunAction(dryRun), rockMetalCount, everythingElseCount)
	return nil
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
	artistUpdateCount := 0
	addedSongs := make(map[string]bool)

	for i, row := range records {
		if i == 0 {
			continue
		}

		if len(row) > 5 {
			songTitle := strings.TrimSpace(row[3])
			artistName := strings.TrimSpace(row[4])
			releaseDate := strings.TrimSpace(row[5])

			songKey := songTitle + "|" + artistName
			if songTitle != "" && artistName != "" && !addedSongs[songKey] {
				if dryRun {
					log.Printf("[seed] Would create song: %q by %q (%s)", songTitle, artistName, releaseDate)
					addedSongs[songKey] = true
					songCount++
					continue
				}

				record := core.NewRecord(songsCollection)
				record.Set("title", songTitle)
				record.Set("artist_name", artistName)
				record.Set("release_date", releaseDate)
				record.Set("is_recent", true)

				if err := app.Save(record); err != nil {
					log.Printf("[seed] Warning: failed to save song %q: %v", songTitle, err)
				} else {
					addedSongs[songKey] = true
					songCount++
				}
			}
		}

		if len(row) > 9 {
			bandName := strings.TrimSpace(row[7])
			spotifyID := strings.TrimSpace(row[8])
			listenersStr := strings.TrimSpace(row[9])

			if bandName != "" && spotifyID != "" {
				listeners := parseListeners(listenersStr)
				if listeners == 0 {
					continue
				}

				if dryRun {
					existingRecords, err := app.FindRecordsByFilter(
						artistsCollection.Id,
						"spotify_id = {:spotifyId}",
						"-created",
						1, 0,
						map[string]any{"spotifyId": spotifyID},
					)

					if err == nil && len(existingRecords) > 0 {
						log.Printf("[seed] Would update artist: %q (%d listeners)", bandName, listeners)
						artistUpdateCount++
					} else {
						log.Printf("[seed] Would create artist: %q (%s, %d listeners)", bandName, spotifyID, listeners)
					}
					continue
				}

				existingRecords, err := app.FindRecordsByFilter(
					artistsCollection.Id,
					"spotify_id = {:spotifyId}",
					"-created",
					1, 0,
					map[string]any{"spotifyId": spotifyID},
				)

				if err == nil && len(existingRecords) > 0 {
					record := existingRecords[0]
					record.Set("monthly_listeners", listeners)
					if err := app.Save(record); err != nil {
						log.Printf("[seed] Warning: failed to update artist %q: %v", bandName, err)
					} else {
						artistUpdateCount++
					}
				} else {
					record := core.NewRecord(artistsCollection)
					record.Set("name", bandName)
					record.Set("spotify_id", spotifyID)
					record.Set("monthly_listeners", listeners)
					record.Set("genre_group", "rock_metal")
					record.Set("list_status", "waiting")
					record.Set("fetch_status", "idle")

					if err := app.Save(record); err != nil {
						log.Printf("[seed] Warning: failed to create artist %q: %v", bandName, err)
					}
				}
			}
		}
	}

	log.Printf("[seed] %s %d song records, %d artist updates from Sheet2", dryRunAction(dryRun), songCount, artistUpdateCount)
	return nil
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
