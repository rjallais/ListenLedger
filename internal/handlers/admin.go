package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/eventsourcing"
)

// adminCheckpointNames lists the projection/relay frontiers reported by the
// ops status endpoint (same names cmd/replay records and cmd/audit reads).
var adminCheckpointNames = []string{
	eventsourcing.CheckpointArtistProjection,
	eventsourcing.CheckpointAlbumProjection,
	eventsourcing.CheckpointSongProjection,
	eventsourcing.CheckpointBatchProjection,
	eventsourcing.CheckpointDomainEventsRelay,
}

// HandleAdminStatus returns a read-only operations snapshot: outbox relay lag,
// projection/relay checkpoints, saga instance states, and the active batch.
// All reads are cheap indexed queries; nothing here writes. Like /api/queue
// and /api/quota it is unauthenticated operational data.
func (h *Handler) HandleAdminStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	status := map[string]any{"status": "ok"}
	degraded := false

	if h.store != nil {
		if lag, err := h.store.UnpublishedCount(ctx); err != nil {
			status["outbox_lag_error"] = err.Error()
			degraded = true
		} else {
			status["outbox_lag"] = lag
		}
		checkpoints := make(map[string]int64)
		for _, name := range adminCheckpointNames {
			if pos, err := h.store.GetCheckpoint(ctx, name); err != nil {
				degraded = true
			} else if pos > 0 {
				checkpoints[name] = pos
			}
		}
		status["checkpoints"] = checkpoints
	}

	if h.db != nil {
		if states, err := sagaInstanceStates(ctx, h); err != nil {
			status["sagas_error"] = err.Error()
			degraded = true
		} else {
			status["sagas"] = states
		}
		if snap, found, err := h.readActiveBatchSnapshotChecked(ctx); err != nil {
			status["batch_error"] = err.Error()
			degraded = true
		} else if found {
			status["batch"] = map[string]any{
				"id":        snap.ID,
				"total":     snap.Total,
				"completed": snap.Completed,
				"done":      snap.Done,
			}
		}
	}

	if degraded {
		status["status"] = "degraded"
	}
	_ = writeJSON(w, http.StatusOK, status)
}

// sagaInstanceStates counts durable saga instances by state.
func sagaInstanceStates(ctx context.Context, h *Handler) (map[string]int64, error) {
	states := make(map[string]int64)
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT state, COUNT(*) FROM saga_instances GROUP BY state;")
		defer func() { _ = stmt.Reset() }()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			states[stmt.ColumnText(0)] = stmt.ColumnInt64(1)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("count saga instance states: %w", err)
	}
	return states, nil
}

// readActiveBatchSnapshotChecked returns the active batch for status polling,
// distinguishing a failed read (err) from no active batch (!found). The stored
// path reads the projection directly; without a store there is none.
func (h *Handler) readActiveBatchSnapshotChecked(ctx context.Context) (batchProgressSnapshot, bool, error) {
	if h.batchStore != nil {
		snap, found, err := h.batchStore.ActiveSnapshot(ctx)
		if err != nil {
			return batchProgressSnapshot{}, false, err
		}
		if !found {
			return batchProgressSnapshot{}, false, nil
		}
		return batchSnapshotFromStore(snap), true, nil
	}
	snap, found := h.getActiveBatchSnapshot(ctx)
	return snap, found, nil
}

func (h *Handler) handleAdminStatus(e *core.RequestEvent) error {
	h.HandleAdminStatus(e.Response, e.Request)
	return nil
}
