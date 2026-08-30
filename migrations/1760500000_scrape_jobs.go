package migrations

import (
	"fmt"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		return upsertScrapeJobsCollection(app)
	}, func(app core.App) error {
		// Keep down migration no-op to avoid destructive collection drops.
		return nil
	})
}

func upsertScrapeJobsCollection(app core.App) error {
	artists, err := app.FindCollectionByNameOrId("artists")
	if err != nil {
		return fmt.Errorf("artists collection not found: %w", err)
	}

	collection, err := app.FindCollectionByNameOrId("scrape_jobs")
	if err != nil {
		collection = core.NewBaseCollection("scrape_jobs")
	}

	collection.Fields.Add(
		&core.TextField{Name: "request_id", Required: true},
		&core.RelationField{Name: "artist", CollectionId: artists.Id, MaxSelect: 1, Required: true},
		&core.SelectField{Name: "status", Values: []string{"queued", "processing", "succeeded", "failed"}, MaxSelect: 1},
		&core.NumberField{Name: "attempts", OnlyInt: true, Min: new(float64(0))},
		&core.TextField{Name: "error"},
		&core.DateField{Name: "queued_at"},
		&core.DateField{Name: "started_at"},
		&core.DateField{Name: "finished_at"},
	)

	collection.AddIndex("idx_scrape_jobs_request_id_unique", true, "`request_id`", "")
	collection.AddIndex("idx_scrape_jobs_artist", false, "`artist`", "")
	collection.AddIndex("idx_scrape_jobs_status", false, "`status`", "")

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to upsert scrape_jobs collection: %w", err)
	}

	return nil
}
