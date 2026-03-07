//go:build goexperiment.jsonv2

package worker

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
)

func (w *Worker) queueTotalSongsRecalc(artistID string) {
	if strings.TrimSpace(artistID) == "" {
		return
	}

	const debounce = 3 * time.Second

	w.recalcMu.Lock()
	defer w.recalcMu.Unlock()

	w.recalcPending[artistID] = struct{}{}
	if w.recalcTimer == nil {
		w.recalcTimer = time.AfterFunc(debounce, w.flushTotalSongsRecalc)
		return
	}
	w.recalcTimer.Reset(debounce)
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

	if err := w.recalculateTotalSongsForArtists(pending); err != nil {
		log.Printf("[worker] Warning: failed to recalculate total_songs ranks: %v", err)
	}
}

func (w *Worker) recalculateTotalSongsForArtists(artistIDs map[string]struct{}) error {
	byGenre := map[string]map[string]struct{}{
		"rock_metal":      {},
		"everything_else": {},
	}

	for artistID := range artistIDs {
		record, err := w.app.FindRecordById("artists", artistID)
		if err != nil {
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
			if _, tracked := targets[record.Id]; !tracked {
				continue
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
func (w *Worker) updateArtistStatus(artistID, status string) error {
	record, err := w.app.FindRecordById("artists", artistID)
	if err != nil {
		return err
	}

	record.Set("fetch_status", status)
	return w.app.Save(record)
}

// updateArtistListeners updates the monthly_listeners, last_updated, and fetch_status of an artist.
func (w *Worker) updateArtistListeners(artistID string, listeners int) error {
	record, err := w.app.FindRecordById("artists", artistID)
	if err != nil {
		return err
	}

	record.Set("monthly_listeners", listeners)
	record.Set("last_updated", time.Now())
	record.Set("fetch_status", "idle")
	return w.app.Save(record)
}
