package migrations

import (
	"fmt"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		collection, err := app.FindCollectionByNameOrId("songs")
		if err != nil {
			return fmt.Errorf("songs collection not found: %w", err)
		}

		collection.Fields.Add(
			&core.TextField{
				Name: "tidal_id",
			},
			&core.TextField{
				Name: "duplicate_of",
			},
		)

		collection.AddIndex("idx_songs_tidal_id", false, "`tidal_id`", "`tidal_id` != ''")
		collection.AddIndex("idx_songs_duplicate_of", false, "`duplicate_of`", "`duplicate_of` != ''")

		if err := app.Save(collection); err != nil {
			return fmt.Errorf("failed to add tidal_id and duplicate_of to songs collection: %w", err)
		}

		return nil
	}, func(app core.App) error {
		return nil
	})
}
