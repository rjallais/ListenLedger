//go:build goexperiment.jsonv2

package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pocketbase/pocketbase"

	"ListenLedger/internal/songbackfill"
)

func loadSongs(ctx context.Context, app *pocketbase.PocketBase) ([]songbackfill.SongInput, error) {
	records, err := app.FindRecordsByFilter("songs", "", "", 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read songs collection: %w", err)
	}

	songs := make([]songbackfill.SongInput, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.GetString("artist_spotify_ids")) != "" {
			continue
		}
		songs = append(songs, songbackfill.SongInput{
			ID:             record.Id,
			Title:          strings.TrimSpace(record.GetString("title")),
			ArtistName:     strings.TrimSpace(record.GetString("artist_name")),
			ReleaseDate:    strings.TrimSpace(record.GetString("release_date")),
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

func loadArtists(ctx context.Context, app *pocketbase.PocketBase) ([]songbackfill.ArtistInput, error) {
	records, err := app.FindRecordsByFilter("artists", "", "", 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read artists collection: %w", err)
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
