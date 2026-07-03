//go:build goexperiment.jsonv2

package migrations

import (
	"fmt"
	"sort"
	"time"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

func init() {
	m.Register(func(app core.App) error {
		if err := addSongRecentBatchPosField(app); err != nil {
			return err
		}
		if err := backfillSongRecentBatchPos(app); err != nil {
			return err
		}
		return nil
	}, func(app core.App) error {
		// Keep down migration as no-op to avoid destructive field/index removal.
		return nil
	})
}

func addSongRecentBatchPosField(app core.App) error {
	collection, err := app.FindCollectionByNameOrId("songs")
	if err != nil {
		return fmt.Errorf("songs collection not found: %w", err)
	}

	collection.Fields.Add(
		&core.NumberField{
			Name:    "recent_batch_pos",
			OnlyInt: true,
			Min:     float64PtrLocalBatchPos(0),
		},
	)
	collection.AddIndex("idx_songs_recent_batch_pos", false, "`is_recent`, `recent_batch_seq`, `recent_batch_pos`", "")

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("failed to add recent_batch_pos to songs collection: %w", err)
	}

	return nil
}

func backfillSongRecentBatchPos(app core.App) error {
	records, err := app.FindRecordsByFilter("songs", "", "", 0, 0, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch songs for batch position backfill: %w", err)
	}

	type batchEntry struct {
		record *core.Record
		seq    int
		at     time.Time
	}

	perSeq := map[int][]batchEntry{}
	for _, record := range records {
		if !record.GetBool("is_recent") {
			if record.GetInt("recent_batch_pos") != 0 {
				record.Set("recent_batch_pos", 0)
				if err := app.Save(record); err != nil {
					return fmt.Errorf("failed resetting recent_batch_pos for non-recent song %s: %w", record.Id, err)
				}
			}
			continue
		}

		seq := record.GetInt("recent_batch_seq")
		if seq <= 0 {
			seq = 1
			record.Set("recent_batch_seq", seq)
			if err := app.Save(record); err != nil {
				return fmt.Errorf("failed normalizing recent_batch_seq for song %s: %w", record.Id, err)
			}
		}

		perSeq[seq] = append(perSeq[seq], batchEntry{
			record: record,
			seq:    seq,
			at:     songOrderingTime(record),
		})
	}

	for seq, entries := range perSeq {
		sort.SliceStable(entries, func(i, j int) bool {
			if !entries[i].at.Equal(entries[j].at) {
				return entries[i].at.Before(entries[j].at)
			}
			return entries[i].record.Id < entries[j].record.Id
		})

		for idx, entry := range entries {
			targetPos := min(idx+1, songsBatchSize)

			if entry.record.GetInt("recent_batch_pos") == targetPos && entry.record.GetInt("recent_batch_seq") == seq {
				continue
			}

			entry.record.Set("recent_batch_seq", seq)
			entry.record.Set("recent_batch_pos", targetPos)
			if err := app.Save(entry.record); err != nil {
				return fmt.Errorf("failed setting recent batch position for song %s: %w", entry.record.Id, err)
			}
		}
	}

	return nil
}

func float64PtrLocalBatchPos(value float64) *float64 {
	return new(value)
}
