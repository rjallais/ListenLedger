//go:build goexperiment.jsonv2

package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/internal/messaging"
)

// handleSSE provides Server-Sent Events for real-time updates.
func (h *Handler) handleSSE(e *core.RequestEvent) error {
	h.ensureBatchProgressSubscriber()

	w := e.Response
	r := e.Request

	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Use sseStreamOpts (no compression) for the persistent SSE connection.
	sse := datastar.NewSSE(w, r, sseStreamOpts...)

	if snapshot, ok := h.getLatestBatchSnapshot(); ok {
		payload := formatBatchSignal(snapshot.ID, snapshot.Total, snapshot.Completed, snapshot.Done)
		if err := sse.PatchSignals(payload); err != nil {
			return err
		}
	}

	ctx := r.Context()

	sub, err := h.nc.Subscribe(messaging.SubjectArtistUpdated, func(msg *nats.Msg) {
		// Don't attempt to write to a closed connection.
		select {
		case <-ctx.Done():
			return
		default:
		}

		update, err := messaging.UnmarshalArtistUpdated(msg.Data)
		if err != nil {
			log.Printf("[sse] Failed to unmarshal update: %v", err)
			return
		}

		// Send artist table updates (listeners, timestamps, status).
		signals := map[string]any{
			"artistListeners": map[string]int{
				update.ArtistID: update.MonthlyListeners,
			},
			"artistUpdated": map[string]string{
				update.ArtistID: formatUpdatedAt(update.UpdatedAt),
			},
			"artistFetchStatus": map[string]string{
				update.ArtistID: update.FetchStatus,
			},
		}

		if snapshot, ok := h.getLatestBatchSnapshot(); ok {
			signals["batchID"] = snapshot.ID
			signals["batchTotal"] = snapshot.Total
			signals["batchCompleted"] = snapshot.Completed
			signals["batchDone"] = snapshot.Done
		}

		payload, err := json.Marshal(signals)
		if err != nil {
			log.Printf("[sse] Failed to marshal signals: %v", err)
			return
		}

		if err := sse.PatchSignals(payload); err != nil {
			log.Printf("[sse] Failed to send signals: %v", err)
		}
	})

	if err != nil {
		return e.String(http.StatusInternalServerError, fmt.Sprintf("Failed to subscribe: %v", err))
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("[sse] Warning: failed to unsubscribe: %v", err)
		}
	}()

	<-ctx.Done()
	return nil
}
