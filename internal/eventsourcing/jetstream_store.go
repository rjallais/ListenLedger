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
// OCC is scoped to the aggregate: a lookup of the aggregate's latest
// persisted event (via the wildcard subject domain.events.<type>.<id>.*)
// both rejects a stale expectedVersion and anchors the publishes with
// WithExpectLastSequenceForSubject, so a concurrent append to this aggregate
// between the reload and the publish fails the batch. Concurrent appends to
// *other* aggregates share nothing in this scope (the old stream-wide anchor
// false-conflicted on those). The wildcard check is server-enforced —
// verified against the pinned server sources, not assumed.
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
	// must fail without mutating the stream.
	for i, evt := range events {
		want := expectedVersion + int64(i) + 1
		if evt.Version != want {
			return fmt.Errorf("%w: stream %s event %d has version %d, want %d",
				ErrConcurrencyConflict, streamID, i, evt.Version, want)
		}
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return fmt.Errorf("jetstream store: stream: %w", err)
	}
	scope := messaging.SubjectDomainEventScope(events[0].StreamType, streamID)
	// Layer 1: reject a stale reader. Reload the aggregate's latest
	// persisted event and compare its version against expectedVersion. Fail
	// closed on read errors — an unreadable log must not accept writes.
	last, err := stream.GetLastMsgForSubject(ctx, scope)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("jetstream store: reload %s: %w", streamID, err)
	}
	var base uint64
	var maxVersion int64
	if err == nil && last != nil {
		lastEvt, err := UnmarshalEvent(last.Data)
		if err != nil {
			return fmt.Errorf("jetstream store: decode last %s: %w", streamID, err)
		}
		base = last.Sequence
		maxVersion = lastEvt.Version
	}
	if maxVersion != expectedVersion {
		return fmt.Errorf("%w: stream %s expected version %d, but found %d",
			ErrConcurrencyConflict, streamID, expectedVersion, maxVersion)
	}
	// Layer 2: publish under the per-aggregate anchor. A concurrent append
	// to this aggregate between the reload and now moved its latest
	// sequence, so the publish misses and the batch fails without partial
	// writes from a stale view.
	var published uint64
	for _, evt := range events {
		subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
		data, err := evt.Bytes()
		if err != nil {
			return fmt.Errorf("jetstream store: marshal %s: %w", evt.ID, err)
		}
		opts := []jetstream.PublishOpt{
			jetstream.WithMsgID(evt.ID),
			jetstream.WithExpectLastSequenceForSubject(base+published, scope),
		}
		if _, err := s.js.Publish(ctx, subject, data, opts...); err != nil {
			if isWrongLastSeq(err) {
				return fmt.Errorf("%w: stream %s: %w", ErrConcurrencyConflict, streamID, err)
			}
			return fmt.Errorf("jetstream store: publish %s: %w", evt.ID, err)
		}
		published++
	}
	return nil
}

// Load replays one stream oldest-to-newest via an ephemeral filtered pull
// consumer: the server transfers only this aggregate's subjects, so the cost
// is O(stream), not O(log). (The prior GetMsg walk over FirstSeq..LastSeq
// transferred every aggregate on every load.)
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
		batch, err := cons.Fetch(100, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) {
				break // caught up: no more matching messages
			}
			return nil, fmt.Errorf("jetstream store: fetch %s: %w", streamID, err)
		}
		got := 0
		for msg := range batch.Messages() {
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
		if got == 0 {
			break // empty batch with no error: caught up
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
