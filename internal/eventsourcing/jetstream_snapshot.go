package eventsourcing

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"ListenLedger/internal/messaging"
)

// JetStream snapshots live in the KVSnapshots bucket (one freeze-frame per
// stream, derivable from the log, so losing one only costs replay time). Each
// record is anchored to the DOMAIN_EVENTS sequence of its last folded event:
// loaders validate the anchor with one direct get and then start the tail
// consumer right behind it, so an artist stream with thousands of facts costs
// O(tail) per command instead of O(stream). A record whose anchor no longer
// matches the log (stream rebuilt by replay, store restored from another
// tree) is treated as absent — the caller degrades to a full replay and the
// next SaveSnapshot overwrites it.

// Compile-time: JetStreamStore satisfies the snapshot contract.
var _ SnapshotStore = (*JetStreamStore)(nil)

// jetStreamSnapshot is the KV record for one stream's snapshot.
type jetStreamSnapshot struct {
	StreamType string    `json:"stream_type"`
	Version    int64     `json:"version"`
	Seq        uint64    `json:"seq"`
	Payload    []byte    `json:"payload"`
	CreatedAt  time.Time `json:"created_at"`
}

// snapshotKey encodes a stream ID as a legal KV key. KV keys allow only
// [-/_=.A-Za-z0-9] (and may not start with a dot), while aggregate IDs are
// arbitrary strings; unpadded URL-safe base64 is injective and always legal.
func snapshotKey(streamID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(streamID))
}

// SaveSnapshot upserts the stream's snapshot. The snapshot is anchored to the
// stream's current tip, so it is stored only when the tip is exactly
// snap.Version: if another writer already appended past it the snapshot is
// skipped (nil, nothing stored) — the next interval boundary retries, and an
// anchor guessed for a non-tip version would be unverifiable.
func (s *JetStreamStore) SaveSnapshot(ctx context.Context, snap Snapshot) error {
	if s.js == nil {
		return fmt.Errorf("jetstream store: JetStream not configured")
	}
	if snap.StreamID == "" {
		return fmt.Errorf("snapshot stream ID cannot be empty")
	}
	if snap.StreamType == "" {
		return fmt.Errorf("snapshot stream type cannot be empty")
	}
	if snap.Version <= 0 {
		return fmt.Errorf("snapshot version must be positive, got %d", snap.Version)
	}
	if len(snap.Payload) == 0 {
		return fmt.Errorf("snapshot payload cannot be empty")
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return fmt.Errorf("jetstream store: stream: %w", err)
	}
	last, err := stream.GetLastMsgForSubject(ctx, messaging.SubjectDomainEventScope(snap.StreamID))
	if err != nil {
		return fmt.Errorf("jetstream store: tip for snapshot of %s: %w", snap.StreamID, err)
	}
	_, _, _, _, tipVersion, err := rawMsgEnvelope(last)
	if err != nil {
		return fmt.Errorf("jetstream store: decode tip for snapshot of %s: %w", snap.StreamID, err)
	}
	if tipVersion != snap.Version {
		return nil // tip moved on (or snapshot is ahead): skip, retry next interval
	}
	createdAt := snap.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	body, err := json.Marshal(jetStreamSnapshot{
		StreamType: snap.StreamType,
		Version:    snap.Version,
		Seq:        last.Sequence,
		Payload:    snap.Payload,
		CreatedAt:  createdAt,
	})
	if err != nil {
		return fmt.Errorf("jetstream store: encode snapshot for %s: %w", snap.StreamID, err)
	}
	if err := messaging.KVPut(ctx, s.js, messaging.KVSnapshots, snapshotKey(snap.StreamID), string(body)); err != nil {
		return fmt.Errorf("jetstream store: save snapshot for %s: %w", snap.StreamID, err)
	}
	return nil
}

// LoadSnapshot returns the stream's snapshot after verifying its anchor still
// names the same event in the log. An absent bucket, key, or anchor mismatch
// all surface as ErrSnapshotNotFound so the repository replays in full.
func (s *JetStreamStore) LoadSnapshot(ctx context.Context, streamID string) (Snapshot, error) {
	rec, err := s.loadSnapshotRecord(ctx, streamID)
	if err != nil {
		return Snapshot{}, err
	}
	stream, err := s.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		return Snapshot{}, fmt.Errorf("jetstream store: stream: %w", err)
	}
	msg, err := stream.GetMsg(ctx, rec.Seq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return Snapshot{}, fmt.Errorf("stream %s: anchor seq %d gone: %w", streamID, rec.Seq, ErrSnapshotNotFound)
		}
		return Snapshot{}, fmt.Errorf("jetstream store: verify snapshot anchor for %s: %w", streamID, err)
	}
	_, anchorStreamID, _, _, anchorVersion, err := rawMsgEnvelope(msg)
	if err != nil || anchorStreamID != streamID || anchorVersion != rec.Version {
		return Snapshot{}, fmt.Errorf("stream %s: anchor seq %d does not match v%d: %w", streamID, rec.Seq, rec.Version, ErrSnapshotNotFound)
	}
	return Snapshot{
		StreamID:       streamID,
		StreamType:     rec.StreamType,
		Version:        rec.Version,
		GlobalPosition: int64(rec.Seq),
		Payload:        rec.Payload,
		CreatedAt:      rec.CreatedAt,
	}, nil
}

// LoadAfter returns the events with version greater than afterVersion, oldest
// first. When the stored snapshot is exactly afterVersion its anchor sequence
// starts the tail consumer behind the folded history; any other afterVersion
// scans from the first message and filters. The tail must start at
// afterVersion+1 — a gap means the anchor was unreliable, so it errors rather
// than hand the caller a state with missing facts.
func (s *JetStreamStore) LoadAfter(ctx context.Context, streamID string, afterVersion int64) ([]Event, error) {
	var afterSeq uint64
	if afterVersion > 0 {
		if rec, err := s.loadSnapshotRecord(ctx, streamID); err == nil && rec.Version == afterVersion {
			afterSeq = rec.Seq
		}
	}
	events, err := s.loadSince(ctx, streamID, afterVersion, afterSeq)
	if err != nil {
		return nil, err
	}
	if len(events) > 0 && events[0].Version != afterVersion+1 {
		return nil, fmt.Errorf("jetstream store: stream %s tail after v%d starts at v%d", streamID, afterVersion, events[0].Version)
	}
	return events, nil
}

// loadSnapshotRecord reads the raw KV record, mapping every "no usable
// snapshot" condition (missing bucket, key, or undecodable record) to
// ErrSnapshotNotFound. Transport failures stay distinct.
func (s *JetStreamStore) loadSnapshotRecord(ctx context.Context, streamID string) (jetStreamSnapshot, error) {
	if s.js == nil {
		return jetStreamSnapshot{}, fmt.Errorf("jetstream store: JetStream not configured")
	}
	raw, ok, err := messaging.KVGet(ctx, s.js, messaging.KVSnapshots, snapshotKey(streamID))
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return jetStreamSnapshot{}, fmt.Errorf("stream %s: %w", streamID, ErrSnapshotNotFound)
		}
		return jetStreamSnapshot{}, fmt.Errorf("jetstream store: load snapshot for %s: %w", streamID, err)
	}
	if !ok {
		return jetStreamSnapshot{}, fmt.Errorf("stream %s: %w", streamID, ErrSnapshotNotFound)
	}
	var rec jetStreamSnapshot
	if err := json.Unmarshal([]byte(raw), &rec); err != nil || rec.Version <= 0 || rec.Seq == 0 || len(rec.Payload) == 0 {
		return jetStreamSnapshot{}, fmt.Errorf("stream %s: undecodable snapshot record: %w", streamID, ErrSnapshotNotFound)
	}
	return rec, nil
}
