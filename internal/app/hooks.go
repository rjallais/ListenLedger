package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
)

// registerArtistUpdateFanout publishes artist.updated for artist saves so
// SSE subscribers and batch tracking converge. Rank (total_songs) is derived
// at read time and no longer written by any flow, so every save fans out.
func registerArtistUpdateFanout(ctx context.Context, app *pocketbase.PocketBase, js jetstream.JetStream) {
	// publishSnapshot carries the cheap in-memory fields the async publish
	// needs, captured synchronously on the hook so the record write never
	// waits on the saga lookup below.
	type publishSnapshot struct {
		artistID         string
		name             string
		monthlyListeners int
		fetchStatus      string
		updatedAt        string
	}
	publish := func(record *core.Record, requestID string) {
		snap := publishSnapshot{
			artistID:         record.Id,
			name:             record.GetString("name"),
			monthlyListeners: record.GetInt("monthly_listeners"),
			fetchStatus:      record.GetString("fetch_status"),
			updatedAt:        record.GetDateTime("last_updated").Time().Format(time.RFC3339),
		}
		// Off the write path: the saga lookup below waits up to 2s on a
		// scrape_jobs query, which must never block the record save that
		// triggered this hook.
		go func(snap publishSnapshot, requestID string) {
			if requestID == "" {
				requestID = correlation.Pop(snap.artistID)
			} else {
				correlation.Clear(snap.artistID)
			}
			// Restart-safe fallback: the in-memory registry (5m TTL) misses when
			// a saga outlives it (Apify fetches run 350s+) or the process
			// restarted mid-saga. Resolve from the live saga row instead — but
			// only queued/processing jobs count: a manual edit with no live saga
			// must ship with empty request_id, never a stale historical one.
			if requestID == "" {
				requestID = latestLiveSagaRequestID(ctx, app, snap.artistID)
			}

			update := messaging.ArtistUpdated{
				Version:          messaging.SchemaVersionV1,
				RequestID:        requestID,
				ArtistID:         snap.artistID,
				Name:             snap.name,
				MonthlyListeners: snap.monthlyListeners,
				FetchStatus:      snap.fetchStatus,
				UpdatedAt:        snap.updatedAt,
			}

			logger := app.Logger()
			data, err := messaging.MarshalArtistUpdated(update)
			if err != nil {
				logger.Warn("[hooks] failed to marshal artist.updated", "err", err)
				return
			}

			msgID := "artist.updated:" + snap.artistID + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)
			publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			if _, err := js.Publish(publishCtx, messaging.SubjectArtistUpdated, data, jetstream.WithMsgID(msgID)); err != nil {
				logger.Warn("[hooks] failed to publish artist.updated to JetStream", "err", err)
			} else if requestID != "" {
				logger.Debug("[hooks] published artist.updated", "artist_id", snap.artistID, "request_id", requestID)
			}
		}(snap, requestID)
	}

	app.OnRecordAfterUpdateSuccess("artists").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return fmt.Errorf("artists after update hook: %w", err)
		}
		publish(e.Record, "")
		return nil
	})

	app.OnRecordAfterCreateSuccess("artists").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return fmt.Errorf("artists after create hook: %w", err)
		}
		publish(e.Record, "")
		return nil
	})
}

// latestLiveSagaRequestID resolves the saga instance for an artist from its
// live scrape_jobs row (queued/processing, newest first). Best-effort and
// fail-open: any error, timeout, or absence returns "" so the fanout still
// publishes — just without saga correlation. Only live jobs count, so manual
// edits with no saga in flight never inherit a stale historical request_id.
func latestLiveSagaRequestID(ctx context.Context, app *pocketbase.PocketBase, artistID string) string {
	artistID = strings.TrimSpace(artistID)
	if app == nil || artistID == "" {
		return ""
	}
	if err := ctx.Err(); err != nil {
		return ""
	}
	qCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	records := make([]*core.Record, 0, 1)
	err := app.RecordQuery("scrape_jobs").
		WithContext(qCtx).
		AndWhere(dbx.NewExp("artist = {:artist} AND status IN ({:queued}, {:processing})",
			dbx.Params{"artist": artistID, "queued": "queued", "processing": "processing"})).
		OrderBy("queued_at DESC").
		Limit(1).
		All(&records)
	if err != nil || len(records) == 0 {
		return ""
	}
	return strings.TrimSpace(records[0].GetString("request_id"))
}

func registerQueueUpdateFanout(ctx context.Context, app *pocketbase.PocketBase, nc *nats.Conn) {
	var mu sync.Mutex
	var timer *time.Timer

	trigger := func() {
		mu.Lock()
		defer mu.Unlock()

		select {
		case <-ctx.Done():
			return
		default:
		}

		if timer != nil {
			return
		}

		timer = time.AfterFunc(500*time.Millisecond, func() {
			mu.Lock()
			timer = nil
			mu.Unlock()

			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := nc.Publish(messaging.SubjectQueueUpdated, []byte("{}")); err != nil {
				app.Logger().Warn("[hooks] failed to publish queue.updated", "err", err)
			}
		})
	}

	app.OnRecordAfterCreateSuccess("scrape_jobs").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return fmt.Errorf("scrape_jobs after create hook: %w", err)
		}
		trigger()
		return nil
	})

	app.OnRecordAfterUpdateSuccess("scrape_jobs").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return fmt.Errorf("scrape_jobs after update hook: %w", err)
		}
		trigger()
		return nil
	})
}
