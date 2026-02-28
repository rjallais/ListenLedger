//go:build goexperiment.jsonv2

package migrations

import (
	"fmt"
	"sort"
	"time"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

const songsBatchSize = 13

func init() {
	m.Register(func(app core.App) error {
		if err := addSongRecentBatchField(app); err != nil {
			return err
		}
		if err := backfillSongRecentBatchSeq(app); err != nil {
			return err
		}
		return nil
	}, func(app core.App) error {
		// Keep down migration as no-op to avoid destructive field/index removal.
		return nil
	})
}

func addSongRecentBatchField(app core.App) error {
	collection, err := app.FindCollectionByNameOrId("songs")
	if err != nil {
		return fmt.Errorf("songs collection not found: %w", err)
	}

	collection.Fields.Add(
		&core.NumberField{
			Name:    "recent_batch_seq",
			OnlyInt: true,
			Min:     float64PtrLocal(0),
		},
	)
	collection.AddIndex("idx_songs_recent_batch_seq", false, "`is_recent`, `recent_batch_seq`", "")

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to add recent_batch_seq to songs collection: %w", err)
	}

	return nil
}

func backfillSongRecentBatchSeq(app core.App) error {
	records, err := app.FindRecordsByFilter("songs", "", "", 0, 0, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch songs for backfill: %w", err)
	}

	recent := make([]*core.Record, 0, len(records))
	for _, record := range records {
		if !record.GetBool("is_recent") {
			if record.GetInt("recent_batch_seq") != 0 {
				record.Set("recent_batch_seq", 0)
				if err := app.Save(record); err != nil {
					return fmt.Errorf("failed setting non-recent batch seq for song %s: %w", record.Id, err)
				}
			}
			continue
		}
		recent = append(recent, record)
	}

	sort.SliceStable(recent, func(i, j int) bool {
		left := songOrderingTime(recent[i])
		right := songOrderingTime(recent[j])
		if !left.Equal(right) {
			return left.Before(right)
		}
		return recent[i].Id < recent[j].Id
	})

	for idx, record := range recent {
		targetSeq := idx/songsBatchSize + 1
		if record.GetInt("recent_batch_seq") == targetSeq {
			continue
		}

		record.Set("recent_batch_seq", targetSeq)
		if err := app.Save(record); err != nil {
			return fmt.Errorf("failed setting recent_batch_seq for song %s: %w", record.Id, err)
		}
	}

	return nil
}

func songOrderingTime(record *core.Record) time.Time {
	created := record.GetDateTime("created").Time()
	if !created.IsZero() {
		return created
	}

	releaseDate := record.GetString("release_date")
	t, err := time.Parse("2006-01-02", releaseDate)
	if err == nil {
		return t
	}

	return time.Time{}
}

func float64PtrLocal(value float64) *float64 {
	return new(value)
}
