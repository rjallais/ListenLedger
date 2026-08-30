package migrations

import (
	"fmt"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		for _, name := range []string{"albums", "songs"} {
			if err := dedupeTitleArtist(app, name); err != nil {
				return err
			}
			if err := addUniqueTitleArtistIndex(app, name); err != nil {
				return err
			}
		}
		return nil
	}, func(app core.App) error {
		return nil
	})
}

func dedupeTitleArtist(app core.App, collectionName string) error {
	records, err := app.FindRecordsByFilter(collectionName, "", "", 0, 0, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch %s for dedupe: %w", collectionName, err)
	}
	seen := map[string]*core.Record{}
	for _, r := range records {
		key := r.GetString("title") + "\x00" + r.GetString("artist_name")
		if key == "\x00" {
			continue
		}
		if prev, ok := seen[key]; ok {
			// Keep newest (created DESC, id as tie-breaker)
			keep, del := prev, r
			if r.GetString("created") > prev.GetString("created") || (r.GetString("created") == prev.GetString("created") && r.Id > prev.Id) {
				keep, del = r, prev
			}
			if err := app.Delete(del); err != nil {
				return fmt.Errorf("failed to delete duplicate %s %s: %w", collectionName, del.Id, err)
			}
			seen[key] = keep
		} else {
			seen[key] = r
		}
	}
	return nil
}

func addUniqueTitleArtistIndex(app core.App, collectionName string) error {
	collection, err := app.FindCollectionByNameOrId(collectionName)
	if err != nil {
		return fmt.Errorf("%s collection not found: %w", collectionName, err)
	}
	idxName := "idx_" + collectionName + "_title_artist_unique"
	// Drop if exists (idempotent re-run), then add unique.
	// Partial index: empty title+artist_name pairs are preserved by the dedupe
	// pass (key == "\x00" skip) and must be excluded here or Save fails on
	// multiple blank records colliding with the unique constraint.
	collection.RemoveIndex(idxName)
	collection.AddIndex(idxName, true, "`title`, `artist_name`", "`title` != '' OR `artist_name` != ''")
	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to add unique index %s: %w", idxName, err)
	}
	return nil
}
