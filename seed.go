//go:build goexperiment.jsonv2

package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
)

// runSeed parses the CSV files and populates the PocketBase collections.
func runSeed(app *pocketbase.PocketBase) error {
	// Seed albums from Sheet1 (columns A-E)
	if err := seedAlbums(app); err != nil {
		return fmt.Errorf("failed to seed albums: %w", err)
	}

	// Seed artists from Sheet1 (columns H-L for rock_metal, N-R for everything_else)
	if err := seedArtistsFromSheet1(app); err != nil {
		return fmt.Errorf("failed to seed artists from Sheet1: %w", err)
	}

	// Seed songs and update artists from Sheet2
	if err := seedFromSheet2(app); err != nil {
		return fmt.Errorf("failed to seed from Sheet2: %w", err)
	}

	return nil
}

// seedAlbums parses Sheet1 columns A-E and creates album records.
func seedAlbums(app *pocketbase.PocketBase) error {
	file, err := os.Open("Music - Sheet1.csv")
	if err != nil {
		return fmt.Errorf("failed to open Sheet1: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

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
		// Skip header row
		if i == 0 {
			continue
		}

		// Need at least 5 columns for album data
		if len(row) < 5 {
			continue
		}

		title := strings.TrimSpace(row[0])
		artistName := strings.TrimSpace(row[1])

		// Skip empty rows
		if title == "" || artistName == "" {
			continue
		}

		collectionSongs := parseNumber(row[2])
		totalSongs := parseNumber(row[3])

		// Determine status based on percentage
		status := determineAlbumStatus(collectionSongs, totalSongs)

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

	log.Printf("[seed] Created %d album records", count)
	return nil
}

// seedArtistsFromSheet1 parses columns H-L (rock_metal) and N-R (everything_else).
func seedArtistsFromSheet1(app *pocketbase.PocketBase) error {
	file, err := os.Open("Music - Sheet1.csv")
	if err != nil {
		return fmt.Errorf("failed to open Sheet1: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read Sheet1: %w", err)
	}

	collection, err := app.FindCollectionByNameOrId("artists")
	if err != nil {
		return fmt.Errorf("artists collection not found: %w", err)
	}

	// Track existing artists by spotify_id to avoid duplicates
	existingArtists := make(map[string]bool)

	rockMetalCount := 0
	everythingElseCount := 0

	for i, row := range records {
		// Skip header row
		if i == 0 {
			continue
		}

		// Parse rock_metal artists (columns H-L, indices 7-11)
		// H=7: Name, I=8: Spotify ID, J=9: Monthly Listeners, K=10: Collection Songs, L=11: Total Songs
		if len(row) > 11 {
			name := strings.TrimSpace(row[7])
			spotifyID := strings.TrimSpace(row[8])
			listenersStr := strings.TrimSpace(row[9])
			collectionSongs := parseNumber(row[10])
			totalSongs := parseNumber(row[11])

			if name != "" && spotifyID != "" && !existingArtists[spotifyID] {
				listeners := parseListeners(listenersStr)

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

		// Parse everything_else artists (columns N-R, indices 13-17)
		// N=13: Name, O=14: Spotify ID, P=15: Monthly Listeners, Q=16: Collection Songs, R=17: Total Songs
		if len(row) > 17 {
			name := strings.TrimSpace(row[13])
			spotifyID := strings.TrimSpace(row[14])
			listenersStr := strings.TrimSpace(row[15])
			collectionSongs := parseNumber(row[16])
			totalSongs := parseNumber(row[17])

			if name != "" && spotifyID != "" && !existingArtists[spotifyID] {
				listeners := parseListeners(listenersStr)

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

	log.Printf("[seed] Created %d rock_metal artists, %d everything_else artists", rockMetalCount, everythingElseCount)
	return nil
}

// seedFromSheet2 parses Sheet2 for recent songs and additional artist data.
func seedFromSheet2(app *pocketbase.PocketBase) error {
	file, err := os.Open("Music - Sheet2.csv")
	if err != nil {
		return fmt.Errorf("failed to open Sheet2: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

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

	// Track added songs to avoid duplicates
	addedSongs := make(map[string]bool)

	for i, row := range records {
		// Skip header row
		if i == 0 {
			continue
		}

		// Parse recent songs from columns D-F (indices 3-5)
		// D=3: Song Title, E=4: Artist, F=5: Release Date
		if len(row) > 5 {
			songTitle := strings.TrimSpace(row[3])
			artistName := strings.TrimSpace(row[4])
			releaseDate := strings.TrimSpace(row[5])

			songKey := songTitle + "|" + artistName
			if songTitle != "" && artistName != "" && !addedSongs[songKey] {
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

		// Parse additional artist data from columns H-J (indices 7-9)
		// H=7: Band, I=8: ID, J=9: Listeners
		if len(row) > 9 {
			bandName := strings.TrimSpace(row[7])
			spotifyID := strings.TrimSpace(row[8])
			listenersStr := strings.TrimSpace(row[9])

			if bandName != "" && spotifyID != "" {
				// Try to find existing artist by spotify_id
				existingRecords, err := app.FindRecordsByFilter(
					artistsCollection.Id,
					"spotify_id = {:spotifyId}",
					"-created",
					1,
					0,
					map[string]any{"spotifyId": spotifyID},
				)

				if err == nil && len(existingRecords) > 0 {
					// Update existing artist
					record := existingRecords[0]
					listeners := parseListeners(listenersStr)
					if listeners > 0 {
						record.Set("monthly_listeners", listeners)
						if err := app.Save(record); err != nil {
							log.Printf("[seed] Warning: failed to update artist %q: %v", bandName, err)
						} else {
							artistUpdateCount++
						}
					}
				} else {
					// Create new artist (these may be smaller/indie artists from Sheet2)
					listeners := parseListeners(listenersStr)
					if listeners > 0 {
						record := core.NewRecord(artistsCollection)
						record.Set("name", bandName)
						record.Set("spotify_id", spotifyID)
						record.Set("monthly_listeners", listeners)
						record.Set("genre_group", "rock_metal") // Default for Sheet2 artists
						record.Set("list_status", "waiting")
						record.Set("fetch_status", "idle")

						if err := app.Save(record); err != nil {
							log.Printf("[seed] Warning: failed to create artist %q: %v", bandName, err)
						}
					}
				}
			}
		}
	}

	log.Printf("[seed] Created %d song records, updated %d artist records from Sheet2", songCount, artistUpdateCount)
	return nil
}

// determineAlbumStatus calculates the album status based on collection/total songs.
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

// parseNumber extracts an integer from a string, handling commas and percentages.
func parseNumber(s string) int {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSuffix(s, "%")

	n, _ := strconv.Atoi(s)
	return n
}

// parseListeners parses a listener count string like "53,129,158" to an integer.
func parseListeners(s string) int {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "\"", "")

	n, _ := strconv.Atoi(s)
	return n
}
