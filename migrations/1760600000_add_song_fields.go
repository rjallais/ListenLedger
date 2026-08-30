package migrations

import (
	"fmt"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		if err := addSongFields(app); err != nil {
			return err
		}
		if err := addAlbumReleaseType(app); err != nil {
			return err
		}
		return nil
	}, func(app core.App) error {
		// Down migration is a no-op to avoid destructive field drops.
		return nil
	})
}

// addSongFields adds release_type and artist_spotify_ids to the songs collection.
func addSongFields(app core.App) error {
	collection, err := app.FindCollectionByNameOrId("songs")
	if err != nil {
		return fmt.Errorf("songs collection not found: %w", err)
	}

	collection.Fields.Add(
		&core.SelectField{
			Name:      "release_type",
			Values:    []string{"album", "ep", "single"},
			MaxSelect: 1,
		},
		&core.TextField{
			Name: "artist_spotify_ids",
		},
	)

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to add fields to songs collection: %w", err)
	}

	return nil
}

// addAlbumReleaseType adds a release_type select field to the albums collection.
func addAlbumReleaseType(app core.App) error {
	collection, err := app.FindCollectionByNameOrId("albums")
	if err != nil {
		return fmt.Errorf("albums collection not found: %w", err)
	}

	collection.Fields.Add(
		&core.SelectField{
			Name:      "release_type",
			Values:    []string{"album", "ep", "single"},
			MaxSelect: 1,
		},
	)

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to add release_type to albums collection: %w", err)
	}

	return nil
}
