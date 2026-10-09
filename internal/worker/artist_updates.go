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

	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

const rankRefreshDebounce = 3 * time.Second

func (w *Worker) queueRankRefresh(artistID string) {
	if strings.TrimSpace(artistID) == "" {
		return
	}

	w.rankMu.Lock()
	defer w.rankMu.Unlock()

	w.rankPending[artistID] = struct{}{}
	if w.rankTimer == nil {
		w.rankTimer = time.AfterFunc(rankRefreshDebounce, w.flushRankRefresh)
		return
	}
	w.rankTimer.Reset(rankRefreshDebounce)
}

func (w *Worker) flushRankRefresh() {
	w.rankMu.Lock()
	pending := make(map[string]struct{}, len(w.rankPending))
	for artistID := range w.rankPending {
		pending[artistID] = struct{}{}
	}
	w.rankPending = make(map[string]struct{})
	w.rankTimer = nil
	w.rankMu.Unlock()

	if len(pending) == 0 {
		return
	}

	// Rank notification only: artist total_songs rank is derived at read
	// time (dynamicTotalSongs), never stored. Past recalc writes are frozen
	// legacy; clients refresh their visible slices from ranks.updated.
	genres, err := w.rankGenresForArtists(w.ctx, pending)
	if err != nil {
		if w.ctx.Err() != nil {
			return
		}
		log.Printf("[worker] Warning: failed resolving rank genres: %v", err)
		return
	}

	w.publishRanksUpdated(genres)
}

// publishRanksUpdated notifies UI subscribers that total_songs ranks were
// recalculated for the given genres so each client refreshes its visible
// page slice exactly once. Ephemeral core NATS (like queue.updated): a UI
// hint fully derivable from the DB, not a durable domain event.
func (w *Worker) publishRanksUpdated(genres []string) {
	if w.nc == nil || len(genres) == 0 {
		return
	}
	for _, genre := range genres {
		if strings.TrimSpace(genre) == "" {
			continue
		}
		subject := messaging.SubjectRanksUpdatedForGenre(genre)
		if err := w.nc.Publish(subject, []byte("{}")); err != nil {
			log.Printf("[worker] Warning: failed to publish %s: %v", subject, err)
			continue
		}
		log.Printf("[worker] Published %s", subject)
	}
}

// rankGenresForArtists resolves the genre groups containing finished scrapes
// so clients refresh the affected rank slices. Read-only: rank is derived at
// query time, so no rows are written here.
func (w *Worker) rankGenresForArtists(ctx context.Context, artistIDs map[string]struct{}) ([]string, error) {
	if w.app == nil {
		return nil, nil
	}
	genres := make(map[string]struct{})

	for artistID := range artistIDs {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("rank genre lookup cancelled: %w", err)
		}

		record, err := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("find artist %s: %w", artistID, err)
		}
		if record == nil {
			continue
		}
		if record.GetString("list_status") == "waiting" {
			continue
		}
		if genre := record.GetString("genre_group"); genre == "rock_metal" || genre == "everything_else" {
			genres[genre] = struct{}{}
		}
	}

	out := make([]string, 0, len(genres))
	for genre := range genres {
		out = append(out, genre)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Artist record helpers
// ---------------------------------------------------------------------------

// updateArtistStatus updates the fetch_status field of an artist.
// Optional requestIDs[0] links the fact to its saga instance in metadata.
func (w *Worker) updateArtistStatus(ctx context.Context, artistID, status string, requestIDs ...string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("check cancellation before loading artist %s for status update: %w", artistID, err)
	}
	var corr eventsourcing.Correlation
	if len(requestIDs) > 0 {
		corr.RequestID = requestIDs[0]
	}

	// 1. Event store & projection update
	if w.artistRepo != nil {
		agg, err := w.artistRepo.Load(ctx, artistID)
		if err != nil {
			if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
				log.Printf("[worker] Warning: load artist aggregate %s for status update: %v", artistID, err)
			}
		} else if agg != nil {
			if err := agg.SetFetchStatus(status, "", corr); err != nil {
				log.Printf("[worker] Warning: set fetch status %s for artist %s: %v", status, artistID, err)
			} else if events, saveErr := w.artistRepo.Save(ctx, agg); saveErr != nil {
				log.Printf("[worker] Warning: save status event %s for artist %s: %v", status, artistID, saveErr)
			} else if len(events) > 0 && w.artistProjection != nil {
				if projErr := w.artistProjection.Project(ctx, agg, events); projErr != nil {
					log.Printf("[worker] Warning: project status event %s for artist %s: %v", status, artistID, projErr)
				}
			}
		}
	}

	// 2. PocketBase update for backward compatibility
	if w.app != nil {
		record, err := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("load artist %s for status update: %w", artistID, err)
		}

		record.Set("fetch_status", status)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("check cancellation before saving status for artist %s: %w", artistID, err)
		}
		if err := w.app.Save(record); err != nil {
			return fmt.Errorf("save fetch_status for artist %s: %w", artistID, err)
		}
	}
	return nil
}

// updateArtistListeners updates the monthly_listeners, last_updated, and fetch_status of an artist
// via event sourcing and projections, keeping PocketBase synchronized for backward compatibility.
// Optional requestIDs[0] links the fact to its saga instance in metadata.
func (w *Worker) updateArtistListeners(ctx context.Context, artistID string, listeners int, provider string, durationMs int64, requestIDs ...string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("check cancellation before loading artist %s for listener update: %w", artistID, err)
	}

	// 1. PocketBase update for backward compatibility, first: the sets below
	// are idempotent, so a failure here leaves no fact behind and a retry is
	// clean. (Appending the event first would record a fact that a retry
	// would duplicate when this save fails.)
	if w.app != nil {
		record, err := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
			q.WithContext(ctx)
			return nil
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
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
	}

	// 2. Event store & projection
	if w.artistRepo != nil {
		agg, err := w.artistRepo.Load(ctx, artistID)
		if err != nil {
			if errors.Is(err, eventsourcing.ErrStreamNotFound) {
				// Bootstrap from PocketBase when available: legacy artists
				// predate the event log. Without app there is no row to
				// seed from (event-only operation, e.g. tests), so fall
				// back to the artist ID; with app a missing row means the
				// artist does not exist and fabricating one would ghost a
				// placeholder aggregate, so fail instead.
				name := artistID
				spotifyID := ""
				genreGroup := "everything_else"
				listStatus := "included"
				if w.app != nil {
					rec, recErr := w.app.FindRecordById("artists", artistID, func(q *dbx.SelectQuery) error {
						q.WithContext(ctx)
						return nil
					})
					if recErr != nil {
						return fmt.Errorf("load artist %s for aggregate seed: %w", artistID, recErr)
					}
					if rec == nil {
						return fmt.Errorf("seed artist aggregate %s: PocketBase row missing", artistID)
					}
					name = rec.GetString("name")
					spotifyID = rec.GetString("spotify_id")
					genreGroup = rec.GetString("genre_group")
					listStatus = rec.GetString("list_status")
				}
				agg, err = artist.NewArtist(artistID, name, spotifyID, genreGroup, listStatus)
				if err != nil {
					return fmt.Errorf("create artist aggregate for %s: %w", artistID, err)
				}
			} else {
				return fmt.Errorf("load artist aggregate %s: %w", artistID, err)
			}
		}

		var corr eventsourcing.Correlation
		if len(requestIDs) > 0 {
			corr.RequestID = requestIDs[0]
		}
		if err := agg.RecordMonthlyListeners(int64(listeners), provider, durationMs, corr); err != nil {
			return fmt.Errorf("record monthly listeners for %s: %w", artistID, err)
		}

		events, err := w.artistRepo.Save(ctx, agg)
		if err != nil {
			return fmt.Errorf("save artist aggregate %s: %w", artistID, err)
		}

		if w.artistProjection != nil && len(events) > 0 {
			if err := w.artistProjection.Project(ctx, agg, events); err != nil {
				log.Printf("[worker] Warning: projection error for artist %s: %v", artistID, err)
			}
		}
	}
	return nil
}
