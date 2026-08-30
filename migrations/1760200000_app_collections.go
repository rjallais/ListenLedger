package migrations

import (
	"fmt"
	"strings"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		if err := upsertAlbumsCollection(app); err != nil {
			return err
		}
		if err := upsertArtistsCollection(app); err != nil {
			return err
		}
		if err := upsertSongsCollection(app); err != nil {
			return err
		}

		return nil
	}, func(app core.App) error {
		// Keep down migration no-op to avoid destructive collection drops.
		return nil
	})
}

func upsertAlbumsCollection(app core.App) error {
	return upsertCollection(app, "albums", func(c *core.Collection) {
		c.Fields.Add(
			&core.TextField{Name: "title", Required: true},
			&core.TextField{Name: "artist_name", Required: true},
			&core.NumberField{Name: "collection_songs", OnlyInt: true, Min: new(float64(0))},
			&core.NumberField{Name: "total_songs", OnlyInt: true, Min: new(float64(0))},
			&core.SelectField{
				Name:      "status",
				Values:    []string{"full", "processed_once", "waiting"},
				MaxSelect: 1,
			},
		)

		c.AddIndex("idx_albums_status", false, "`status`", "")
		c.AddIndex("idx_albums_total_songs", false, "`total_songs`", "")
		c.AddIndex("idx_albums_artist_name", false, "`artist_name`", "")
	})
}

func upsertArtistsCollection(app core.App) error {
	collection, err := app.FindCollectionByNameOrId("artists")
	existed := err == nil
	if !existed {
		collection = core.NewBaseCollection("artists")
	}

	collection.Fields.Add(
		&core.TextField{Name: "name", Required: true},
		&core.TextField{
			Name:    "spotify_id",
			Min:     22,
			Max:     22,
			Pattern: "^[A-Za-z0-9]{22}$",
		},
		&core.NumberField{Name: "monthly_listeners", OnlyInt: true, Min: new(float64(0))},
		&core.SelectField{
			Name:      "genre_group",
			Values:    []string{"rock_metal", "everything_else"},
			MaxSelect: 1,
		},
		&core.SelectField{
			Name:      "list_status",
			Values:    []string{"included", "recently_added", "not_added", "waiting"},
			MaxSelect: 1,
		},
		&core.DateField{Name: "last_updated"},
		&core.SelectField{
			Name:      "fetch_status",
			Values:    []string{"idle", "pending", "failed"},
			MaxSelect: 1,
		},
		&core.NumberField{Name: "collection_songs", OnlyInt: true, Min: new(float64(0))},
		&core.NumberField{Name: "total_songs", OnlyInt: true, Min: new(float64(0))},
	)

	collection.AddIndex("idx_artists_genre_status_listeners", false, "`genre_group`, `list_status`, `monthly_listeners` DESC", "")
	collection.AddIndex("idx_artists_list_status", false, "`list_status`", "")
	collection.AddIndex("idx_artists_fetch_status", false, "`fetch_status`", "")

	// If legacy data has duplicate non-empty spotify_id values,
	// keep startup non-blocking by applying a non-unique lookup index.
	hasDuplicates := false
	duplicateSamples := []string{}
	if existed {
		hasDuplicates, duplicateSamples, err = artistsSpotifyIDHasDuplicates(app)
		if err != nil {
			return fmt.Errorf("failed to inspect artists spotify_id duplicates: %w", err)
		}
	}

	collection.RemoveIndex("idx_artists_spotify_id_unique")
	collection.RemoveIndex("idx_artists_spotify_id")
	if hasDuplicates {
		collection.AddIndex("idx_artists_spotify_id", false, "`spotify_id`", "`spotify_id` != ''")
		app.Logger().Warn(
			"[migrations] skipped unique artists.spotify_id index due to duplicate values",
			"duplicate_groups_sample_size", len(duplicateSamples),
			"duplicate_sample", strings.Join(duplicateSamples, ", "),
		)
	} else {
		collection.AddIndex("idx_artists_spotify_id_unique", true, "`spotify_id`", "`spotify_id` != ''")
	}

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to upsert artists collection: %w", err)
	}

	return nil
}

func upsertSongsCollection(app core.App) error {
	return upsertCollection(app, "songs", func(c *core.Collection) {
		c.Fields.Add(
			&core.TextField{Name: "title", Required: true},
			&core.TextField{Name: "artist_name", Required: true},
			&core.TextField{Name: "album"},
			&core.TextField{Name: "release_date"},
			&core.NumberField{Name: "release_year", OnlyInt: true, Min: new(float64(0))},
			&core.TextField{
				Name:    "spotify_id",
				Pattern: `^$|^[A-Za-z0-9]{22}$`,
			},
			&core.BoolField{Name: "is_recent"},
			&core.NumberField{Name: "recent_batch_seq", OnlyInt: true, Min: new(float64(0))},
			&core.NumberField{Name: "recent_batch_pos", OnlyInt: true, Min: new(float64(0))},
		)

		c.AddIndex("idx_songs_is_recent_release_date", false, "`is_recent`, `release_date`", "")
		c.AddIndex("idx_songs_recent_batch_seq", false, "`is_recent`, `recent_batch_seq`", "")
		c.AddIndex("idx_songs_artist_name", false, "`artist_name`", "")
		c.AddIndex("idx_songs_spotify_id_unique", true, "`spotify_id`", "`spotify_id` != ''")
	})
}

func upsertCollection(app core.App, name string, configure func(collection *core.Collection)) error {
	collection, err := app.FindCollectionByNameOrId(name)
	if err != nil {
		collection = core.NewBaseCollection(name)
	}

	configure(collection)

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to upsert %s collection: %w", name, err)
	}

	return nil
}

func artistsSpotifyIDHasDuplicates(app core.App) (bool, []string, error) {
	type duplicateRow struct {
		SpotifyID string `db:"spotify_id"`
		Total     int    `db:"total"`
	}

	rows := []duplicateRow{}
	if err := app.DB().NewQuery(`
		SELECT spotify_id, COUNT(*) AS total
		FROM artists
		WHERE spotify_id != ''
		GROUP BY spotify_id
		HAVING COUNT(*) > 1
		ORDER BY total DESC, spotify_id ASC
		LIMIT 10
	`).All(&rows); err != nil {
		return false, nil, err
	}

	if len(rows) == 0 {
		return false, nil, nil
	}

	samples := make([]string, 0, len(rows))
	for _, row := range rows {
		samples = append(samples, row.SpotifyID)
	}

	return true, samples, nil
}

//go:fix inline
func float64Ptr(value float64) *float64 {
	return &value
}
