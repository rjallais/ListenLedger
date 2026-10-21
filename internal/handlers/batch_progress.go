package handlers

import (
	"context"
	"fmt"
	"log"
	"maps"
	"net/http"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/internal/batchprogress"
	"ListenLedger/internal/messaging"
	"ListenLedger/templates"
)

type batchProgressSnapshot struct {
	ID        string
	Stats     map[string]int
	Total     int
	Completed int
	Done      bool
}

func (h *Handler) ensureBatchProgressSubscriber() {
	h.batchMu.RLock()
	if h.batchUpdates != nil {
		h.batchMu.RUnlock()
		return
	}
	h.batchMu.RUnlock()

	h.batchSubMu.Lock()
	defer h.batchSubMu.Unlock()

	h.batchMu.RLock()
	if h.batchUpdates != nil {
		h.batchMu.RUnlock()
		return
	}
	h.batchMu.RUnlock()

	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			return
		}
		h.markBatchArtistDone(update.ArtistID, update.FetchStatus)
	})
	if err != nil {
		log.Printf("[batch] Failed to subscribe to %s: %v", messaging.SubjectArtistUpdated, err)
		return
	}

	h.batchMu.Lock()
	h.batchUpdates = sub
	h.batchMu.Unlock()
	log.Printf("[batch] Tracking progress from %s", messaging.SubjectArtistUpdated)
}

func (h *Handler) markBatchArtistDone(artistID, fetchStatus string) {
	if h.batchStore == nil {
		return
	}
	h.markBatchArtistDoneStored(artistID, fetchStatus)
}

const batchReconcileInterval = 2 * time.Second

// markBatchArtistDoneStored records completion in the SQLite projection.
// The NATS callback carries no context, so it gets a bounded background one.
// Best-effort low-latency path: failures only log — RunBatchReconciler
// converges the same fact from the SQLite read model within seconds.
func (h *Handler) markBatchArtistDoneStored(artistID, fetchStatus string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	batchID, err := h.batchStore.CompleteArtist(ctx, artistID, fetchStatus)
	if err != nil {
		log.Printf("[batch] CompleteArtist(%s) failed: %v", artistID, err)
		return
	}
	if batchID == "" {
		return
	}
	snap, found, err := h.batchStore.Snapshot(ctx, batchID)
	if err != nil {
		log.Printf("[batch] Snapshot(%s) failed: %v", batchID, err)
		return
	}
	if !found {
		return
	}
	h.publishBatchProgress(ctx, batchSnapshotFromStore(snap))
}

// publishBatchProgress publishes the authoritative batch progress to NATS.
func (h *Handler) publishBatchProgress(ctx context.Context, snapshot batchProgressSnapshot) {
	if h.nc == nil {
		return
	}
	data, err := messaging.MarshalBatchProgressUpdated(messaging.NewBatchProgressUpdated(
		snapshot.ID,
		snapshot.Total,
		snapshot.Completed,
		snapshot.Done,
		snapshot.Stats,
	))
	if err != nil {
		log.Printf("[batch] MarshalBatchProgressUpdated failed: %v", err)
		return
	}
	if err := h.nc.Publish(messaging.SubjectBatchProgress, data); err != nil {
		log.Printf("[batch] Publish to %s failed: %v", messaging.SubjectBatchProgress, err)
	}
}

// batchSnapshotFromStore converts a projection snapshot to the handler shape.
func batchSnapshotFromStore(snap batchprogress.Snapshot) batchProgressSnapshot {
	stats := make(map[string]int, len(snap.Stats))
	maps.Copy(stats, snap.Stats)
	return batchProgressSnapshot{
		ID:        snap.ID,
		Total:     snap.Total,
		Completed: snap.Completed,
		Done:      snap.Done,
		Stats:     stats,
	}
}

// reconcileBatchStoreThrottled runs the projection reconcile at most every
// batchReconcileInterval so snapshot reads stay cheap under polling.
func (h *Handler) reconcileBatchStoreThrottled(ctx context.Context) {
	if h.batchStore == nil {
		return
	}
	h.batchMu.Lock()
	if time.Since(h.lastBatchReconcile) < batchReconcileInterval {
		h.batchMu.Unlock()
		return
	}
	h.lastBatchReconcile = time.Now()
	h.batchMu.Unlock()

	if n, err := h.batchStore.Reconcile(ctx); err != nil {
		log.Printf("[batch] Reconcile failed: %v", err)
	} else if n > 0 {
		log.Printf("[batch] Reconcile completed %d member(s) missed by events", n)
		if snap, found, err := h.batchStore.LatestSnapshot(ctx); err == nil && found {
			h.publishBatchProgress(ctx, batchSnapshotFromStore(snap))
		}
	}
}

// RunBatchReconciler converges durable batch progress without UI polling.
// It reconciles once at startup (crash recovery for missed artist.updated
// fanout) then ticks every interval: members whose artist already left
// fetch_status='pending' are completed from the SQLite read model and each
// catch-up is logged as a BatchArtistCompleted fact. Crash-safe: Reconcile
// derives everything from batches/batch_members/artists, so restarts just
// re-converge on the next pass. No-op when batchStore is nil (in-mem tests).
func (h *Handler) RunBatchReconciler(ctx context.Context, interval time.Duration) {
	if h.batchStore == nil {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	reconcile := func() {
		rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if n, err := h.batchStore.Reconcile(rctx); err != nil {
			if ctx.Err() == nil {
				log.Printf("[batch] background Reconcile failed: %v", err)
			}
		} else if n > 0 {
			log.Printf("[batch] background Reconcile completed %d member(s)", n)
			if snap, found, err := h.batchStore.LatestSnapshot(rctx); err == nil && found {
				h.publishBatchProgress(rctx, batchSnapshotFromStore(snap))
			}
		}
	}
	reconcile()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

// createBatchProgress starts a batch in the durable SQLite projection,
// pruning done batches older than 2h. The store is mandatory: without it the
// refresh flow fails closed (fail-fast beats in-memory progress that dies on
// restart). Start is retry-safe, so callers surface the error for retry.
func (h *Handler) createBatchProgress(ctx context.Context, artistIDs []string, stats map[string]int) (batchProgressSnapshot, error) {
	if h.batchStore == nil {
		return batchProgressSnapshot{}, fmt.Errorf("batch progress store is not configured")
	}
	now := time.Now()
	batchID := strconv.FormatInt(now.UnixNano(), 36)
	timeout := 30 * time.Second
	if needed := time.Duration(len(artistIDs)) * 200 * time.Millisecond; needed > timeout {
		timeout = needed
	}
	storeCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	snap, err := h.batchStore.Start(storeCtx, batchID, artistIDs, stats, now.Add(-batchprogress.PruneRetention))
	if err != nil {
		return batchProgressSnapshot{}, fmt.Errorf("starting batch %s: %w", batchID, err)
	}
	res := batchSnapshotFromStore(snap)
	h.publishBatchProgress(storeCtx, res)
	return res, nil
}

func (h *Handler) getBatchSnapshot(ctx context.Context, batchID string) (batchProgressSnapshot, bool) {
	if batchID == "" || h.batchStore == nil {
		return batchProgressSnapshot{}, false
	}

	h.reconcileBatchStoreThrottled(ctx)
	snap, found, err := h.batchStore.Snapshot(ctx, batchID)
	if err != nil {
		log.Printf("[batch] Snapshot(%s) failed: %v", batchID, err)
		return batchProgressSnapshot{}, false
	}
	if !found {
		return batchProgressSnapshot{}, false
	}
	return batchSnapshotFromStore(snap), true
}

func (h *Handler) getActiveBatchSnapshot(ctx context.Context) (batchProgressSnapshot, bool) {
	if h.batchStore == nil {
		return batchProgressSnapshot{}, false
	}

	h.reconcileBatchStoreThrottled(ctx)
	snap, found, err := h.batchStore.ActiveSnapshot(ctx)
	if err != nil {
		log.Printf("[batch] ActiveSnapshot failed: %v", err)
		return batchProgressSnapshot{}, false
	}
	if !found {
		return batchProgressSnapshot{}, false
	}
	return batchSnapshotFromStore(snap), true
}

func (h *Handler) getLatestBatchSnapshot(ctx context.Context) (batchProgressSnapshot, bool) {
	if h.batchStore == nil {
		return batchProgressSnapshot{}, false
	}

	h.reconcileBatchStoreThrottled(ctx)
	snap, found, err := h.batchStore.LatestSnapshot(ctx)
	if err != nil {
		log.Printf("[batch] LatestSnapshot failed: %v", err)
		return batchProgressSnapshot{}, false
	}
	if !found {
		return batchProgressSnapshot{}, false
	}
	return batchSnapshotFromStore(snap), true
}

func (h *Handler) patchBatchRefreshStateHTTP(w http.ResponseWriter, r *http.Request, snapshot batchProgressSnapshot) error {
	sse := datastar.NewSSE(w, r, sseOpts...)
	payload := fmt.Appendf(
		nil,
		`{"batchID":%q,"batchTotal":%d,"batchCompleted":%d,"batchDone":%t}`,
		snapshot.ID,
		snapshot.Total,
		snapshot.Completed,
		snapshot.Done,
	)
	if err := sse.PatchSignals(payload); err != nil {
		return err
	}
	return sse.PatchElementTempl(
		templates.BatchRefreshResult(snapshot.ID, snapshot.Total, snapshot.Completed, snapshot.Stats, snapshot.Done),
	)
}

func (h *Handler) patchBatchRefreshState(e *core.RequestEvent, snapshot batchProgressSnapshot) error {
	return h.patchBatchRefreshStateHTTP(e.Response, e.Request, snapshot)
}
