// Package eventsourcing JetStream-backed event store.
//
// JetStreamStore implements Store against the DOMAIN_EVENTS JetStream stream
// (Delaney flip: JetStream is the authority, SQLite is disposable). Append
// uses Nats-Expected-Last-Sequence-Per-Subject for optimistic concurrency on
// the stream's subject, MsgID=event ID for short-window dedup. Load replays
// one stream's subjects via an ephemeral ordered consumer.
//
// Phase 1: write-behind dual-write only; SQLite stays the Load source.
// Phase 2 flips Load here. Not used by the runtime yet.
package eventsourcing

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
)

// JetStreamStore is an event store backed by JetStream DOMAIN_EVENTS.
type JetStreamStore struct {
	js jetstream.JetStream
}

// NewJetStreamStore creates a JetStreamStore over an ensured DOMAIN_EVENTS stream.
func NewJetStreamStore(js jetstream.JetStream) *JetStreamStore {
	return &JetStreamStore{js: js}
}

// Compile-time: JetStreamStore satisfies the write-path read contract.
var _ Store = (*JetStreamStore)(nil)

// Append publishes events to their per-type subjects with OCC on the
// stream's last sequence for that subject and MsgID dedup on the event ID.
// A CAS miss surfaces as ErrConcurrencyConflict, same contract as SQLite.
func (s *JetStreamStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	if s.js == nil {
		return fmt.Errorf("jetstream store: JetStream not configured")
	}
	if len(events) == 0 {
		return nil
	}
	for i, evt := range events {
		want := expectedVersion + int64(i) + 1
		if evt.Version != want {
			return fmt.Errorf("%w: stream %s event %d has version %d, want %d",
				ErrConcurrencyConflict, streamID, i, evt.Version, want)
		}
		subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
		data, err := evt.Bytes()
		if err != nil {
			return fmt.Errorf("jetstream store: marshal %s: %w", evt.ID, err)
		}
		opts := []jetstream.PublishOpt{jetstream.WithMsgID(evt.ID)}
		if seq, ok := lastSubjectSeq(ctx, s.js, subject); ok && seq > 0 {
			opts = append(opts, jetstream.WithExpectLastSequencePerSubject(seq))
		}
		if _, err := s.js.Publish(ctx, subject, data, opts...); err != nil {
			if isWrongLastSeq(err) {
				return fmt.Errorf("%w: stream %s: %v", ErrConcurrencyConflict, streamID, err)
			}
			return fmt.Errorf("jetstream store: publish %s: %w", evt.ID, err)
		}
	}
	return nil
}

// Load replays one stream oldest-to-newest by walking the log with GetMsg
// and keeping this stream's events.
//
// ponytail: O(stream) scan; fine while Load is test-only (Phase 1 runtime
// still loads from SQLite). Upgrade path for Phase 2 reads: durable
// filtered pull consumers per stream. (Ordered-consumer FetchNoWait hangs
// in consumer creation here, so Load avoids the consumer API entirely.)
func (s *JetStreamStore) Load(ctx context.Context, streamID string) ([]Event, error) {
	if s.js == nil {
		return nil, fmt.Errorf("jetstream store: JetStream not configured")
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return nil, fmt.Errorf("jetstream store: stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("jetstream store: info: %w", err)
	}
	var out []Event
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			continue // deleted/expired slot: skip
		}
		evt, err := UnmarshalEvent(raw.Data)
		if err != nil {
			return nil, fmt.Errorf("jetstream store: decode seq %d: %w", seq, err)
		}
		if evt.StreamID == streamID {
			out = append(out, evt)
		}
	}
	return out, nil
}

// lastSubjectSeq returns the stream sequence of the last message on subject,
// or false when the subject has no messages yet (first append: no CAS).
func lastSubjectSeq(ctx context.Context, js jetstream.JetStream, subject string) (uint64, bool) {
	stream, err := js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return 0, false
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil || msg == nil {
		return 0, false
	}
	return msg.Sequence, true
}

// isWrongLastSeq reports a JetStream CAS miss (both single and clustered codes).
func isWrongLastSeq(err error) bool {
	var jsErr jetstream.JetStreamError
	if !errors.As(err, &jsErr) {
		return false
	}
	apiErr := jsErr.APIError()
	if apiErr == nil {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}
