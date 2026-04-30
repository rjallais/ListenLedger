//go:build goexperiment.jsonv2

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/pocketbase/pocketbase"

	"ListenLedger/internal/songbackfill"
)

func applyApprovedResolutions(ctx context.Context, app *pocketbase.PocketBase, resolutions []songbackfill.Resolution, minConf float64, doApply bool) (applied, nameChanges int) {
	for _, resolution := range resolutions {
		if !resolution.Approved(minConf) && !resolution.NamePrefillApproved(minConf) {
			continue
		}
		if !doApply {
			if resolution.UpdatedArtistName != resolution.OriginalArtistName {
				nameChanges++
			}
			continue
		}
		record, err := app.FindRecordById("songs", resolution.SongID)
		if err != nil {
			log.Printf("[backfill_song_artists] warning: failed to load song %s for update: %v", resolution.SongID, err)
			continue
		}
		record.Set("artist_name", resolution.UpdatedArtistName)
		if resolution.UpdatedArtistSpotifyIDs != "" {
			record.Set("artist_spotify_ids", resolution.UpdatedArtistSpotifyIDs)
		}
		if err := app.Save(record); err != nil {
			log.Printf("[backfill_song_artists] warning: failed to save song %s: %v", resolution.SongID, err)
			continue
		}
		applied++
		if resolution.UpdatedArtistName != resolution.OriginalArtistName {
			nameChanges++
		}
	}
	return
}

func writeReviewOutputs(ctx context.Context, reportDir, timestamp, reportPath string, resolutions []songbackfill.Resolution, artists []songbackfill.ArtistInput) (reviewQueue, string, string, error) {
	queue := buildReviewQueue(reportPath, resolutions, artists)
	if len(queue.ReviewEntries) == 0 {
		return queue, "", "", nil
	}
	jsonPath := filepath.Join(reportDir, fmt.Sprintf("song_artist_review_queue_%s.json", timestamp))
	csvPath := filepath.Join(reportDir, fmt.Sprintf("song_artist_review_queue_%s.csv", timestamp))
	queue.JSONPath = jsonPath
	queue.CSVPath = csvPath
	if err := writeReviewQueueJSON(jsonPath, queue); err != nil {
		return queue, jsonPath, csvPath, fmt.Errorf("write review queue json: %w", err)
	}
	if err := writeReviewQueueCSV(csvPath, queue); err != nil {
		_ = os.Remove(jsonPath)
		return queue, jsonPath, csvPath, fmt.Errorf("write review queue csv: %w", err)
	}
	return queue, jsonPath, csvPath, nil
}
