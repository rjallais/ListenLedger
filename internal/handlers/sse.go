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
	"zombiezen.com/go/sqlite"

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

// batchProgressTracker keeps one SSE client's view monotonic per batch:
// out-of-order publishes must not lower the completion count or reopen a
// finished batch.
type batchProgressTracker struct {
	latest map[string]batchProgressSnapshot
}

func newBatchProgressTracker() *batchProgressTracker {
	return &batchProgressTracker{latest: make(map[string]batchProgressSnapshot)}
}

func (t *batchProgressTracker) accept(snapshot batchProgressSnapshot) bool {
	if prev, ok := t.latest[snapshot.ID]; ok {
		if prev.Done || snapshot.Completed < prev.Completed {
			return false
		}
	}
	t.latest[snapshot.ID] = snapshot
	return true
}

// patchBatchProgressSnapshot streams both the batch result fragment and Datastar signals.
func (h *Handler) patchBatchProgressSnapshot(patch ssePatcher, patchSignals func([]byte) error, snapshot batchProgressSnapshot, logger *slog.Logger) {
	if patchSignals != nil {
		payload := fmt.Appendf(
			nil,
			`{"batchID":%q,"batchTotal":%d,"batchCompleted":%d,"batchDone":%t}`,
			snapshot.ID,
			snapshot.Total,
			snapshot.Completed,
			snapshot.Done,
		)
		if err := patchSignals(payload); err != nil {
			logger.Debug("[sse] Failed to patch batch signals", "error", err)
		}
	}
	if patch != nil {
		if err := patch(templates.BatchRefreshResult(
			snapshot.ID,
			snapshot.Total,
			snapshot.Completed,
			snapshot.Stats,
			snapshot.Done,
		), noViewTransitionsOpts...); err != nil {
			logger.Debug("[sse] Failed to patch batch refresh result", "error", err)
		}
	}
}

// patchLatestBatchResult patches the latest batch progress into the DOM if an active snapshot exists.
func (h *Handler) patchLatestBatchResult(ctx context.Context, patch ssePatcher, patchSignals func([]byte) error, logger *slog.Logger) {
	if snapshot, ok := h.getLatestBatchSnapshot(ctx); ok {
		h.patchBatchProgressSnapshot(patch, patchSignals, snapshot, logger)
	}
}

// patchInitialBatchProgress streams batch progress on connection start only if a batch is active.
func (h *Handler) patchInitialBatchProgress(ctx context.Context, patch ssePatcher, patchSignals func([]byte) error, logger *slog.Logger, tracker *batchProgressTracker) {
	if snapshot, ok := h.getActiveBatchSnapshot(ctx); ok {
		if tracker != nil {
			tracker.accept(snapshot)
		}
		h.patchBatchProgressSnapshot(patch, patchSignals, snapshot, logger)
	}
}

// patchLatestQueueStats fetches queue statistics and streams the widget update.
func (h *Handler) patchLatestQueueStats(ctx context.Context, patch ssePatcher, logger *slog.Logger) {
	qCtx, qCancel := context.WithTimeout(ctx, 2*time.Second)
	defer qCancel()
	queueStats := h.getQueueStats(qCtx)
	if err := patch(templates.QueueJobsWidgetContent(queueStats), noViewTransitionsOpts...); err != nil {
		logger.Debug("[sse] Failed to patch queue stats", "error", err)
	}
}

// patchBatchAndQueueProgress sends both the batch refresh status and the queue counters.
func (h *Handler) patchBatchAndQueueProgress(ctx context.Context, patch ssePatcher, patchSignals func([]byte) error, logger *slog.Logger) {
	h.patchLatestBatchResult(ctx, patch, patchSignals, logger)
	h.patchLatestQueueStats(ctx, patch, logger)
}

const sseQueueStatsDebounce = 150 * time.Millisecond

// newDebouncedQueueStatsTrigger returns a coalescing trigger that runs
// patchLatestQueueStats at most once per sseQueueStatsDebounce window per SSE
// client, preventing 200 artist.updated events during batch enqueue from
// triggering 200 back-to-back queue stats queries and DOM morphs.
func (h *Handler) newDebouncedQueueStatsTrigger(ctx context.Context, patch ssePatcher, logger *slog.Logger) func() {
	var (
		mu      sync.Mutex
		timer   *time.Timer
		running bool
		pending bool
	)
	var fire func()
	fire = func() {
		mu.Lock()
		if ctx.Err() != nil {
			timer = nil
			running = false
			pending = false
			mu.Unlock()
			return
		}
		if running {
			pending = true
			timer = nil
			mu.Unlock()
			return
		}
		running = true
		pending = false
		timer = nil
		mu.Unlock()

		h.patchLatestQueueStats(ctx, patch, logger)

		mu.Lock()
		running = false
		if pending && ctx.Err() == nil {
			pending = false
			timer = time.AfterFunc(sseQueueStatsDebounce, fire)
		}
		mu.Unlock()
	}

	return func() {
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if running {
			pending = true
			return
		}
		if timer != nil {
			return
		}
		timer = time.AfterFunc(sseQueueStatsDebounce, fire)
	}
}

// rankArtistPosition returns the (total, rank) of one artist among non-waiting
// artists of its genre via two indexed counts in a single ReadTX: total peers,
// plus peers ranked ahead (more listeners, or equal listeners with a smaller id
// — the same tie-break as buildArtistRankMap). Used by per-event SSE patches to
// avoid a full genre scan per event per connection.
func (h *Handler) rankArtistPosition(ctx context.Context, genre string, listeners int, artistID string) (total, rank int, err error) {
	if h.db == nil {
		return 0, 0, fmt.Errorf("rankArtistPosition requires SQLite")
	}
	err = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		totalStmt := tx.Prep("SELECT COUNT(*) FROM artists WHERE genre_group = ? AND list_status != 'waiting';")
		defer func() { _ = totalStmt.Reset() }()
		totalStmt.BindText(1, genre)
		hasRow, stepErr := totalStmt.Step()
		if stepErr != nil || !hasRow {
			return stepErr
		}
		total = int(totalStmt.ColumnInt64(0))

		rankStmt := tx.Prep(`SELECT COUNT(*) FROM artists WHERE genre_group = ? AND list_status != 'waiting'
			AND (monthly_listeners > ? OR (monthly_listeners = ? AND id < ?));`)
		defer func() { _ = rankStmt.Reset() }()
		rankStmt.BindText(1, genre)
		rankStmt.BindInt64(2, int64(listeners))
		rankStmt.BindInt64(3, int64(listeners))
		rankStmt.BindText(4, artistID)
		hasRow, stepErr = rankStmt.Step()
		if stepErr != nil || !hasRow {
			return stepErr
		}
		rank = int(rankStmt.ColumnInt64(0)) + 1
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("rank artist %s for genre %s: %w", artistID, genre, err)
	}
	return total, rank, nil
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

	var totalSongs int
	if record.GetString("list_status") == waitingArtistStatus {
		totalSongs = record.GetInt("collection_songs")
	} else {
		genre := record.GetString("genre_group")
		if h.db != nil {
			total, rank, rankErr := h.rankArtistPosition(dbCtx, genre, record.GetInt("monthly_listeners"), artistID)
			if rankErr != nil {
				// Rank is display-only (ArtistRow/Card do not render TotalSongs):
				// never drop the patch, fall back to the full-scan path.
				logger.Debug("[sse] Failed to rank artist for patch, falling back", "artist_id", artistID, "error", rankErr)
				rankCache, _ := h.buildArtistRankMap(dbCtx, genre)
				totalSongs = h.dynamicTotalSongs(dbCtx, record, rankCache)
			} else {
				totalSongs = rankedArtistTotalSongs(total, 0, rank-1)
			}
		} else {
			rankCache, _ := h.buildArtistRankMap(dbCtx, genre)
			totalSongs = h.dynamicTotalSongs(dbCtx, record, rankCache)
		}
	}
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
func (h *Handler) subscribeArtistUpdates(ctx context.Context, patch ssePatcher, scheduleQueueStats func(), logger *slog.Logger, wg *sync.WaitGroup) (*managedSubscription, error) {
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

		h.patchArtistElement(ctx, patch, logger, update.ArtistID)
		if scheduleQueueStats != nil {
			scheduleQueueStats()
		} else {
			h.patchLatestQueueStats(ctx, patch, logger)
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

// subscribeBatchProgress subscribes to authoritative batch progress updates and streams DOM and signal changes.
func (h *Handler) subscribeBatchProgress(ctx context.Context, patch ssePatcher, patchSignals func([]byte) error, logger *slog.Logger, wg *sync.WaitGroup, tracker *batchProgressTracker) (*managedSubscription, error) {
	managed := newManagedSubscription(wg)
	sub, err := h.nc.Subscribe(messaging.SubjectBatchProgress, func(msg *nats.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		update, err := messaging.UnmarshalBatchProgressUpdated(msg.Data)
		if err != nil {
			logger.Warn("[sse] Failed to unmarshal batch progress", "error", err)
			return
		}

		snapshot := batchProgressSnapshot{
			ID:        update.BatchID,
			Total:     update.Total,
			Completed: update.Completed,
			Done:      update.Done,
			Stats:     update.Stats,
		}
		if tracker != nil && !tracker.accept(snapshot) {
			return
		}
		h.patchBatchProgressSnapshot(patch, patchSignals, snapshot, logger)
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
func (h *Handler) subscribeQueueUpdates(ctx context.Context, patch ssePatcher, scheduleQueueStats func(), logger *slog.Logger, wg *sync.WaitGroup) (*managedSubscription, error) {
	managed := newManagedSubscription(wg)
	sub, err := h.nc.Subscribe(messaging.SubjectQueueUpdated, func(msg *nats.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if scheduleQueueStats != nil {
			scheduleQueueStats()
		} else {
			h.patchLatestQueueStats(ctx, patch, logger)
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
	scheduleQueueStats := h.newDebouncedQueueStatsTrigger(ctx, patch, logger)

	tracker := newBatchProgressTracker()
	h.patchInitialBatchProgress(ctx, patch, patchSignals, logger, tracker)
	h.patchLatestQueueStats(ctx, patch, logger)

	var wg sync.WaitGroup
	sub, err := h.subscribeArtistUpdates(ctx, patch, scheduleQueueStats, logger, &wg)
	if err != nil {
		logger.Error("[sse] failed to subscribe to artist updates", "error", err)
		patchMu.Lock()
		_ = sse.ConsoleError(fmt.Errorf("failed to subscribe: %w", err))
		patchMu.Unlock()
		return
	}

	queueSub, err := h.subscribeQueueUpdates(ctx, patch, scheduleQueueStats, logger, &wg)
	if err != nil {
		logger.Warn("[sse] failed to subscribe to queue updates", "error", err)
	}

	ranksSub, err := h.subscribeRanksUpdates(ctx, patchSignals, logger, &wg)
	if err != nil {
		logger.Warn("[sse] failed to subscribe to ranks updates", "error", err)
	}

	batchSub, err := h.subscribeBatchProgress(ctx, patch, patchSignals, logger, &wg, tracker)
	if err != nil {
		logger.Warn("[sse] failed to subscribe to batch progress updates", "error", err)
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
			if batchSub != nil {
				batchSub.drain("batch progress updates", logger)
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
