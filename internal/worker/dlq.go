package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

// handleDLQ publishes the message to the dead-letter queue after retries are
// exhausted and terminates it from the JetStream consumer.
func (w *Worker) handleDLQ(ctx context.Context, env msgEnvelope, err error) msgResult {
	reason := "retry_exhausted: " + err.Error()
	if dlqErr := w.publishScrapeDLQ(ctx, env, reason); dlqErr != nil {
		log.Printf("[worker] Failed to publish retry-exhausted message to DLQ: %v", dlqErr)
		if nakErr := env.msg.Nak(); nakErr != nil {
			log.Printf("[worker] Failed to NAK retry-exhausted message after DLQ publish failure: %v", nakErr)
		}
		return msgOK
	}
	if termErr := env.msg.Term(); termErr != nil {
		log.Printf("[worker] Failed to terminate retry-exhausted message: %v", termErr)
		return msgOK
	}
	if statusErr := w.updateArtistStatus(ctx, env.req.ArtistID, "failed", env.req.RequestID); statusErr != nil {
		log.Printf("[worker] Failed to mark retry-exhausted artist %s as failed: %v", env.req.ArtistID, statusErr)
	}
	w.setScrapeJobFinished(env.req.RequestID, "failed", "retry_exhausted")
	w.recordJobEventWarn(ctx, env.req.RequestID, env.req.ArtistID, "dead-lettered", func(j *scrapejob.Job) error {
		corr := eventsourcing.Correlation{RequestID: env.req.RequestID}
		if _, _, err := j.RecordFailed(reason, corr); err != nil {
			return err
		}
		_, _, err := j.RecordDeadLettered(reason, corr)
		return err
	})
	w.recordDLQ(env.label)
	return msgOK
}

// publishScrapeDLQ encapsulates the payload formatting and JetStream publication
// to the dead-letter stream when a scrape request fails permanently or is invalid.
func (w *Worker) publishScrapeDLQ(ctx context.Context, env msgEnvelope, reason string) error {
	payload := map[string]any{
		"reason":      reason,
		"at":          time.Now().Format(time.RFC3339),
		"subject":     env.msg.Subject(),
		"payload_b64": base64.StdEncoding.EncodeToString(env.msg.Data()),
		"request_id":  env.req.RequestID,
		"artist_id":   env.req.ArtistID,
		"spotify_id":  env.req.SpotifyID,
	}
	if env.meta != nil {
		payload["num_delivered"] = env.meta.NumDelivered
		payload["stream_seq"] = env.meta.Sequence.Stream
		payload["consumer_seq"] = env.meta.Sequence.Consumer
		payload["stream"] = env.meta.Stream
		payload["consumer"] = env.meta.Consumer
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal DLQ envelope: %w", err)
	}

	pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := w.js.Publish(pubCtx, messaging.SubjectScrapeDLQ, data); err != nil {
		return fmt.Errorf("publish DLQ message: %w", err)
	}

	return nil
}
