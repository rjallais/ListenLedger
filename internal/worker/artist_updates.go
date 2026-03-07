//go:build goexperiment.jsonv2

package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
)

const totalSongsRecalcDebounce = 3 * time.Second

func (w *Worker) queueTotalSongsRecalc(artistID string) {
	if strings.TrimSpace(artistID) == "" {
		return
	}

	w.recalcMu.Lock()
	defer w.recalcMu.Unlock()

	w.recalcPending[artistID] = struct{}{}
	if w.recalcTimer == nil {
		w.recalcTimer = time.AfterFunc(totalSongsRecalcDebounce, w.flushTotalSongsRecalc)
		return
	}
	w.recalcTimer.Reset(totalSongsRecalcDebounce)
}

func (w *Worker) flushTotalSongsRecalc() {
	w.recalcMu.Lock()
	pending := make(map[string]struct{}, len(w.recalcPending))
	for artistID := range w.recalcPending {
		pending[artistID] = struct{}{}
	}
	w.recalcPending = make(map[string]struct{})
	w.recalcTimer = nil
	w.recalcMu.Unlock()

	if len(pending) == 0 {
		return
	}

	if err := w.recalculateTotalSongsForArtists(w.ctx, pending); err != nil {
		if w.ctx.Err() != nil {
			return
		}
		log.Printf("[worker] Warning: failed to recalculate total_songs ranks: %v", err)

		w.recalcMu.Lock()
		for artistID := range pending {
			w.recalcPending[artistID] = struct{}{}
		}
		if w.recalcTimer == nil {
			w.recalcTimer = time.AfterFunc(totalSongsRecalcDebounce, w.flushTotalSongsRecalc)
		} else {
			w.recalcTimer.Reset(totalSongsRecalcDebounce)
		}
		w.recalcMu.Unlock()
	}
}

func (w *Worker) recalculateTotalSongsForArtists(ctx context.Context, artistIDs map[string]struct{}) error {
	byGenre := map[string]map[string]struct{}{
		"rock_metal":      {},
		"everything_else": {},
	}

	for artistID := range artistIDs {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("recalculate total_songs cancelled: %w", err)
		}

		record, err := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return fmt.Errorf("find artist %s: %w", artistID, err)
		}
		if record == nil {
			continue
		}
		if record.GetString("list_status") == "waiting" {
			continue
		}
		genre := record.GetString("genre_group")
		if _, ok := byGenre[genre]; !ok {
			continue
		}
		byGenre[genre][artistID] = struct{}{}
	}

	for genre, targets := range byGenre {
		if len(targets) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("recalculate total_songs cancelled for genre %s: %w", genre, err)
		}

		records, err := w.app.FindRecordsByFilter(
			"artists",
			"genre_group = {:genre} && list_status != {:waiting}",
			"-monthly_listeners,name",
			0,
			0,
			dbx.Params{"genre": genre, "waiting": "waiting"},
		)
		if err != nil {
			return fmt.Errorf("list artists for %s: %w", genre, err)
		}

		totalCount := len(records)
		for index, record := range records {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("recalculate total_songs cancelled while saving artist %s: %w", record.Id, err)
			}

			targetTotalSongs := totalCount - index
			if record.GetInt("total_songs") == targetTotalSongs {
				continue
			}

			record.Set("total_songs", targetTotalSongs)
			if err := w.app.Save(record); err != nil {
				return fmt.Errorf("save total_songs for artist %s: %w", record.Id, err)
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Artist record helpers
// ---------------------------------------------------------------------------

// updateArtistStatus updates the fetch_status field of an artist.
func (w *Worker) updateArtistStatus(ctx context.Context, artistID, status string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("check cancellation before loading artist %s for status update: %w", artistID, err)
	}

	record, err := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if err != nil {
		return fmt.Errorf("load artist %s for status update: %w", artistID, err)
	}

	record.Set("fetch_status", status)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("check cancellation before saving status for artist %s: %w", artistID, err)
	}
	if err := w.app.Save(record); err != nil {
		return fmt.Errorf("save fetch_status for artist %s: %w", artistID, err)
	}
	return nil
}

// updateArtistListeners updates the monthly_listeners, last_updated, and fetch_status of an artist.
func (w *Worker) updateArtistListeners(ctx context.Context, artistID string, listeners int) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("check cancellation before loading artist %s for listener update: %w", artistID, err)
	}

	record, err := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
		q.WithContext(ctx)
		return nil
	})
	if err != nil {
		return fmt.Errorf("load artist %s for listener update: %w", artistID, err)
	}

	record.Set("monthly_listeners", listeners)
	record.Set("last_updated", time.Now())
	record.Set("fetch_status", "idle")
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("check cancellation before saving listeners for artist %s: %w", artistID, err)
	}
	if err := w.app.Save(record); err != nil {
		return fmt.Errorf("save listeners for artist %s: %w", artistID, err)
	}
	return nil
}
