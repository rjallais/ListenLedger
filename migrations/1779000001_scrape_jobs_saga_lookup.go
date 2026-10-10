package migrations

import (
	"fmt"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		collection, err := app.FindCollectionByNameOrId("scrape_jobs")
		if err != nil {
			return fmt.Errorf("scrape_jobs collection not found: %w", err)
		}
		// Covering index for the live-saga lookup in
		// internal/app/hooks.go (latestLiveSagaRequestID): artist equality +
		// status filter, newest first. Keeps the fanout fallback off the
		// record-save write path latency budget.
		const idxName = "idx_scrape_jobs_artist_status_queued"
		collection.RemoveIndex(idxName)
		collection.AddIndex(idxName, false, "`artist`, `status`, `queued_at`", "")
		if err := app.Save(collection); err != nil {
			return fmt.Errorf("failed to add index %s: %w", idxName, err)
		}
		return nil
	}, func(app core.App) error {
		collection, err := app.FindCollectionByNameOrId("scrape_jobs")
		if err != nil {
			return fmt.Errorf("scrape_jobs collection not found during rollback: %w", err)
		}
		collection.RemoveIndex("idx_scrape_jobs_artist_status_queued")
		if err := app.Save(collection); err != nil {
			return fmt.Errorf("failed to remove saga lookup index: %w", err)
		}
		return nil
	})
}
