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

// Append publishes events to their per-type subjects with stream-wide OCC
// and MsgID dedup on the event ID. A CAS miss surfaces as
// ErrConcurrencyConflict, same contract as SQLite.
//
// OCC is two-layered: (1) reload the stream's current max version and
// reject a stale expectedVersion before publishing; (2) publish under a
// stream-wide WithExpectLastSequence anchor so a concurrent append between
// the reload and the publish fails the batch. JetStream subjects are per
// (type, id, event-type), so no subject primitive enforces one aggregate's
// version — the reload is the version check, the anchor is the race guard.
// True single-round-trip per-aggregate CAS needs a Phase 2 version anchor
// (e.g. KV streamID->version with revision-conditional update).
//
// Partial-write contract: publishes are per-event (JetStream has no
// multi-publish transaction), so a mid-batch failure can leave a prefix
// persisted. On any error the caller must reload the stream and retry only
// the unwritten tail; already-persisted versions resurface as
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
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("jetstream store: read stream state: %w", err)
	}
	floor := info.State.LastSeq // stream-wide CAS anchor; 0 on empty stream
	// Layer 1: reject a stale reader. Reload the aggregate's current max
	// version from the log and compare against expectedVersion. Fail closed
	// on read errors — an unreadable log must not accept writes.
	current, err := s.Load(ctx, streamID)
	if err != nil {
		return fmt.Errorf("jetstream store: reload %s: %w", streamID, err)
	}
	var maxVersion int64
	for _, evt := range current {
		maxVersion = max(maxVersion, evt.Version)
	}
	if maxVersion != expectedVersion {
		return fmt.Errorf("%w: stream %s expected version %d, but found %d",
			ErrConcurrencyConflict, streamID, expectedVersion, maxVersion)
	}
	// Layer 2: publish under the CAS anchor. A concurrent append between
	// the reload and now moved LastSeq, so the first publish misses and
	// the batch fails without partial writes from a stale view.
	var published uint64
	for _, evt := range events {
		subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
		data, err := evt.Bytes()
		if err != nil {
			return fmt.Errorf("jetstream store: marshal %s: %w", evt.ID, err)
		}
		opts := []jetstream.PublishOpt{
			jetstream.WithMsgID(evt.ID),
			jetstream.WithExpectLastSequence(floor + published),
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
	if info.State.Msgs == 0 {
		return nil, nil // empty stream: nothing to replay
	}
	var out []Event
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue // deleted/expired slot: skip
			}
			return nil, fmt.Errorf("jetstream store: get seq %d: %w", seq, err)
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
