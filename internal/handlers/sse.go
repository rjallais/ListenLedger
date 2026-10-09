package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/a-h/templ"
	"github.com/nats-io/nats.go"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

type ssePatcher func(c templ.Component, opts ...datastar.PatchElementOption) error

// newSSEPatcher creates a thread-safe element patcher bound to the SSE stream.
func (h *Handler) newSSEPatcher(ctx context.Context, sse *datastar.ServerSentEventGenerator, mu *sync.Mutex) ssePatcher {
	return func(c templ.Component, opts ...datastar.PatchElementOption) error {
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		allOpts := patchOpts(h.cfg, "", opts...)
		if err := sse.PatchElementTempl(c, allOpts...); err != nil {
			_ = sse.ConsoleError(err)
			return err
		}
		return nil
	}
}

// newSSESignalsPatcher creates a thread-safe signals patcher bound to the SSE stream.
func (h *Handler) newSSESignalsPatcher(ctx context.Context, sse *datastar.ServerSentEventGenerator, mu *sync.Mutex) func([]byte) error {
	return func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sse.PatchSignals(payload); err != nil {
			_ = sse.ConsoleError(err)
			return err
		}
		return nil
	}
}

// patchLatestBatchResult patches the latest batch progress into the DOM if an active snapshot exists.
func (h *Handler) patchLatestBatchResult(ctx context.Context, patch ssePatcher, logger *slog.Logger) {
	if snapshot, ok := h.getLatestBatchSnapshot(ctx); ok {
		if err := patch(templates.BatchRefreshResult(
			snapshot.ID,
			snapshot.Total,
			snapshot.Completed,
			snapshot.Stats,
			snapshot.Done,
		)); err != nil {
			logger.Debug("[sse] Failed to patch batch refresh result", "error", err)
		}
	}
}

// patchLatestQueueStats fetches queue statistics and streams the widget update.
func (h *Handler) patchLatestQueueStats(ctx context.Context, patch ssePatcher, logger *slog.Logger) {
	qCtx, qCancel := context.WithTimeout(ctx, 2*time.Second)
	defer qCancel()
	queueStats := h.getQueueStats(qCtx)
	if err := patch(templates.QueueJobsWidgetContent(queueStats)); err != nil {
		logger.Debug("[sse] Failed to patch queue stats", "error", err)
	}
}

// patchBatchAndQueueProgress sends both the batch refresh status and the queue counters.
func (h *Handler) patchBatchAndQueueProgress(ctx context.Context, patch ssePatcher, logger *slog.Logger) {
	h.patchLatestBatchResult(ctx, patch, logger)
	h.patchLatestQueueStats(ctx, patch, logger)
}

// patchArtistElement fetches the latest artist state from DB and patches the card or table row.
func (h *Handler) patchArtistElement(ctx context.Context, patch ssePatcher, logger *slog.Logger, artistID string) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 2*time.Second)
	defer dbCancel()

	record, err := h.findArtistRecord(dbCtx, artistID)
	if err != nil {
		logger.Debug("[sse] Failed to find artist record for patch", "artist_id", artistID, "error", err)
		return
	}

	genre := record.GetString("genre_group")
	rankCache, _ := h.buildArtistRankMap(dbCtx, genre)
	totalSongs := h.dynamicTotalSongs(dbCtx, record, rankCache)
	artist := artistFromRecord(record, totalSongs)

	var comp templ.Component
	if artist.ListStatus == waitingArtistStatus {
		comp = templates.WaitingArtistCard(artist)
	} else {
		comp = templates.ArtistRow(artist)
	}

	if err := patch(comp); err != nil {
		logger.Debug("[sse] Failed to patch artist element", "artist_id", artistID, "error", err)
	}
}

type managedSubscription struct {
	sub  *nats.Subscription
	once sync.Once
	wg   *sync.WaitGroup
}

func newManagedSubscription(wg *sync.WaitGroup) *managedSubscription {
	wg.Add(1)
	return &managedSubscription{wg: wg}
}

func (m *managedSubscription) done() {
	m.once.Do(func() {
		if m.wg != nil {
			m.wg.Done()
		}
	})
}

func (m *managedSubscription) drain(name string, logger *slog.Logger) {
	if m == nil || m.sub == nil {
		return
	}
	if err := m.sub.Drain(); err != nil {
		logger.Debug("[sse] failed to drain "+name, "error", err)
		m.done()
	}
}

// subscribeArtistUpdates subscribes to NATS artist update notifications and streams DOM changes.
func (h *Handler) subscribeArtistUpdates(ctx context.Context, patch ssePatcher, logger *slog.Logger, wg *sync.WaitGroup) (*managedSubscription, error) {
	managed := newManagedSubscription(wg)
	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			logger.Warn("[sse] Failed to unmarshal update", "error", err)
			return
		}

		// Batch completion is marked once by the global subscriber in
		// ensureBatchProgressSubscriber; marking here too would repeat it
		// for every open SSE connection.
		h.patchArtistElement(ctx, patch, logger, update.ArtistID)
		h.patchBatchAndQueueProgress(ctx, patch, logger)
	})
	if err != nil {
		managed.done()
		return nil, err
	}
	managed.sub = sub
	sub.SetClosedHandler(func(_ string) {
		managed.done()
	})
	return managed, nil
}

// ranksTickSignalForGenre maps a genre group to the Datastar signal bumped
// when its total_songs ranks are recalculated. Unknown genres are rejected so
// a malformed subject can never inject an arbitrary signal name.
func ranksTickSignalForGenre(genre string) (string, bool) {
	if !allowedGenreGroups[genre] {
		return "", false
	}
	return templates.RanksTickSignal(genre), true
}

// subscribeRanksUpdates subscribes to total_songs recalculation completions
// and bumps the genre's rank tick signal. Watching table wrappers refetch
// their visible page slice via data-effect (one page-sized morph per recalc
// instead of one SSE morph per rewritten row).
func (h *Handler) subscribeRanksUpdates(ctx context.Context, patchSignals func([]byte) error, logger *slog.Logger, wg *sync.WaitGroup) (*managedSubscription, error) {
	managed := newManagedSubscription(wg)
	sub, err := h.nc.Subscribe(messaging.SubjectRanksUpdatedWildcard, func(msg *nats.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tick, ok := ranksTickSignalForGenre(messaging.RanksGenreFromSubject(msg.Subject))
		if !ok {
			logger.Debug("[sse] Ignoring ranks event for unknown genre", "subject", msg.Subject)
			return
		}

		payload, err := json.Marshal(map[string]int64{tick: time.Now().UnixNano()})
		if err != nil {
			logger.Warn("[sse] Failed to marshal ranks tick", "error", err)
			return
		}
		if err := patchSignals(payload); err != nil {
			logger.Debug("[sse] Failed to patch ranks tick", "signal", tick, "error", err)
		}
	})
	if err != nil {
		managed.done()
		return nil, err
	}
	managed.sub = sub
	sub.SetClosedHandler(func(_ string) {
		managed.done()
	})
	return managed, nil
}

// subscribeQueueUpdates subscribes to queue counter updates and streams widget changes.
func (h *Handler) subscribeQueueUpdates(ctx context.Context, patch ssePatcher, logger *slog.Logger, wg *sync.WaitGroup) (*managedSubscription, error) {
	managed := newManagedSubscription(wg)
	sub, err := h.nc.Subscribe(messaging.SubjectQueueUpdated, func(msg *nats.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		h.patchLatestQueueStats(ctx, patch, logger)
	})
	if err != nil {
		managed.done()
		return nil, err
	}
	managed.sub = sub
	sub.SetClosedHandler(func(_ string) {
		managed.done()
	})
	return managed, nil
}

// sseKeepaliveLoop sends periodic SSE comment lines to prevent intermediaries from disconnecting idle clients.
func sseKeepaliveLoop(ctx context.Context, w http.ResponseWriter, mu *sync.Mutex, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mu.Lock()
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				mu.Unlock()
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			mu.Unlock()
		}
	}
}

// HandleSSE provides Server-Sent Events for real-time updates.
func (h *Handler) HandleSSE(w http.ResponseWriter, r *http.Request) {
	h.ensureBatchProgressSubscriber()
	logger := LoggerFromContext(r.Context())

	// Use sseStreamOpts (no compression) for the persistent SSE connection.
	sse := datastar.NewSSE(w, r, sseStreamOpts...)
	ctx := r.Context()

	var patchMu sync.Mutex
	patch := h.newSSEPatcher(ctx, sse, &patchMu)
	patchSignals := h.newSSESignalsPatcher(ctx, sse, &patchMu)

	h.patchBatchAndQueueProgress(ctx, patch, logger)

	var wg sync.WaitGroup
	sub, err := h.subscribeArtistUpdates(ctx, patch, logger, &wg)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to subscribe: %v", err), http.StatusInternalServerError)
		return
	}

	queueSub, err := h.subscribeQueueUpdates(ctx, patch, logger, &wg)
	if err != nil {
		logger.Warn("[sse] failed to subscribe to queue updates", "error", err)
	}

	ranksSub, err := h.subscribeRanksUpdates(ctx, patchSignals, logger, &wg)
	if err != nil {
		logger.Warn("[sse] failed to subscribe to ranks updates", "error", err)
	}

	defer func() {
		go func() {
			if sub != nil {
				sub.drain("artist updates", logger)
			}
			if queueSub != nil {
				queueSub.drain("queue updates", logger)
			}
			if ranksSub != nil {
				ranksSub.drain("ranks updates", logger)
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(1 * time.Second):
				logger.Warn("[sse] timeout waiting for subscriptions to drain")
			}
		}()
	}()

	sseKeepaliveLoop(ctx, w, &patchMu, 5*time.Second)
}

func (h *Handler) handleSSE(e *core.RequestEvent) error {
	h.HandleSSE(e.Response, e.Request)
	return nil
}
