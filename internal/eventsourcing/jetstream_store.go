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
	"time"

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

// Append publishes events to their per-type subjects with per-aggregate OCC
// and MsgID dedup on the event ID. A CAS miss surfaces as
// ErrConcurrencyConflict, same contract as SQLite.
//
// OCC is scoped to the aggregate ID across all stream types (wildcard
// domain.events.*.<id>.*), matching the Load replay scope: SQLite keys
// history by stream ID with a type-consistency guard, so the JetStream check
// must too. A lookup of the aggregate's latest persisted event both rejects
// a stale expectedVersion and anchors the publishes with
// WithExpectLastSequenceForSubject, so a concurrent append to this aggregate
// between the reload and the publish fails the batch. Concurrent appends to
// *other* aggregates share nothing in this scope (the old stream-wide anchor
// false-conflicted on those). The wildcard check is server-enforced —
// verified against the pinned server sources, not assumed.
//
// Each publish in a multi-event batch anchors to the preceding publish's
// acknowledgement Sequence, not to base+i: scope sequences are not
// consecutive within one aggregate when other aggregates interleave, so
// base+i would false-conflict after the first event persisted.
//
// Partial-write contract: publishes are per-event (JetStream has no
// multi-publish transaction in this client), so a mid-batch failure can
// leave a prefix persisted. On any error the caller must reload the stream
// and retry only the unwritten tail; already-persisted versions resurface as
// ErrConcurrencyConflict, never as silent duplicates.
func (s *JetStreamStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	if s.js == nil {
		return fmt.Errorf("jetstream store: JetStream not configured")
	}
	if len(events) == 0 {
		return nil
	}
	// Validate the whole batch before publishing anything: a version skew
	// must fail without mutating the stream. Batch identity matches SQLite
	// (one stream ID, one aggregate type, consecutive versions).
	streamType := events[0].StreamType
	if streamType == "" {
		return fmt.Errorf("jetstream store: event %s has an empty stream type", events[0].ID)
	}
	for i, evt := range events {
		want := expectedVersion + int64(i) + 1
		if evt.Version != want {
			return fmt.Errorf("%w: stream %s event %d has version %d, want %d",
				ErrConcurrencyConflict, streamID, i, evt.Version, want)
		}
		if evt.StreamID != streamID {
			return fmt.Errorf("%w: event %s belongs to stream %s, want %s",
				ErrConcurrencyConflict, evt.ID, evt.StreamID, streamID)
		}
		if evt.StreamType != streamType {
			return fmt.Errorf("jetstream store: stream %s has mixed aggregate types %q and %q",
				streamID, streamType, evt.StreamType)
		}
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return fmt.Errorf("jetstream store: stream: %w", err)
	}
	scope := messaging.SubjectDomainEventScope(streamID)
	// Layer 1: reject a stale reader. Reload the aggregate's latest
	// persisted event and compare its version against expectedVersion. Fail
	// closed on read errors — an unreadable log must not accept writes.
	last, err := stream.GetLastMsgForSubject(ctx, scope)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("jetstream store: reload %s: %w", streamID, err)
	}
	var anchor uint64
	var maxVersion int64
	var existingType string
	if err == nil && last != nil {
		lastEvt, err := UnmarshalEvent(last.Data)
		if err != nil {
			return fmt.Errorf("jetstream store: decode last %s: %w", streamID, err)
		}
		anchor = last.Sequence
		maxVersion = lastEvt.Version
		existingType = lastEvt.StreamType
	}
	if maxVersion != expectedVersion {
		return fmt.Errorf("%w: stream %s expected version %d, but found %d",
			ErrConcurrencyConflict, streamID, expectedVersion, maxVersion)
	}
	// Same order as SQLite: version first, then the aggregate-type guard.
	if existingType != "" && existingType != streamType {
		return fmt.Errorf("jetstream store: stream %s has aggregate type %q, cannot append type %q",
			streamID, existingType, streamType)
	}
	// Layer 2: publish under the per-aggregate anchor, advancing it from
	// each acknowledgement. A concurrent append to this aggregate between
	// the reload and now moved its latest sequence, so the publish misses
	// and the batch fails without partial writes from a stale view.
	for _, evt := range events {
		subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
		data, err := evt.Bytes()
		if err != nil {
			return fmt.Errorf("jetstream store: marshal %s: %w", evt.ID, err)
		}
		opts := []jetstream.PublishOpt{
			jetstream.WithMsgID(evt.ID),
			jetstream.WithExpectLastSequenceForSubject(anchor, scope),
		}
		ack, err := s.js.Publish(ctx, subject, data, opts...)
		if err != nil {
			if isWrongLastSeq(err) {
				return fmt.Errorf("%w: stream %s: %w", ErrConcurrencyConflict, streamID, err)
			}
			return fmt.Errorf("jetstream store: publish %s: %w", evt.ID, err)
		}
		if ack != nil {
			anchor = ack.Sequence
		}
	}
	return nil
}

// Load replays one stream oldest-to-newest via an ephemeral filtered pull
// consumer: the server transfers only this aggregate's subjects, so the cost
// is O(stream), not O(log). (The prior GetMsg walk over FirstSeq..LastSeq
// transferred every aggregate on every load.)
//
// The replay is bounded by the stream's LastSeq captured before consuming:
// events published after Load starts (sequence past the cutoff) are skipped
// and stop the replay, so a continuously-written aggregate cannot extend the
// loop until the context expires. Draining uses FetchNoWait — persisted
// history is already available, so there is no reason to burn a MaxWait
// expiry on the short final batch (or on an absent aggregate).
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
	if info.State.Msgs == 0 {
		return nil, nil // empty stream: nothing to replay
	}
	cutoff := info.State.LastSeq
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject:     messaging.SubjectDomainEventFilter(streamID),
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream store: consumer for %s: %w", streamID, err)
	}
	// Ephemeral consumer: delete explicitly; the inactivity threshold reaps
	// stragglers if the context is already dead here.
	defer func() { _ = stream.DeleteConsumer(ctx, cons.CachedInfo().Name) }()
	var out []Event
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := cons.FetchNoWait(100)
		if err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) {
				break // caught up: no more matching messages
			}
			return nil, fmt.Errorf("jetstream store: fetch %s: %w", streamID, err)
		}
		pastCutoff := false
		got := 0
		for msg := range batch.Messages() {
			meta, err := msg.Metadata()
			if err != nil {
				return nil, fmt.Errorf("jetstream store: metadata %s: %w", streamID, err)
			}
			if meta.Sequence.Stream > cutoff {
				pastCutoff = true // live arrival: stop, don't error
				continue
			}
			evt, err := UnmarshalEvent(msg.Data())
			if err != nil {
				return nil, fmt.Errorf("jetstream store: decode %s: %w", streamID, err)
			}
			if evt.StreamID != streamID {
				continue // defensive: filter is exact, trust the data
			}
			out = append(out, evt)
			got++
		}
		if err := batch.Error(); err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) {
				break
			}
			return nil, fmt.Errorf("jetstream store: batch %s: %w", streamID, err)
		}
		if pastCutoff {
			break
		}
		if got == 0 {
			break // drained: no more available within the cutoff
		}
	}
	return out, nil
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
