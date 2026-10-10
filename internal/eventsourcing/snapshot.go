package eventsourcing

import (
	"context"
	"fmt"
	"time"

	"zombiezen.com/go/sqlite"
)

// Snapshot is an aggregate freeze-frame for write validation (right side of
// CQRS). Unlike projections (rebuildable perfect indexes for queries),
// snapshots let loaders skip replaying history before Snapshot.Version.
//
// Closing the books: once a stream grows past a few thousand events
// (TalkPython #548 guidance: <2000 events replay directly), writers store a
// snapshot and loaders replay only events after it.
type Snapshot struct {
	StreamID   string
	StreamType string
	Version    int64
	Payload    []byte
	CreatedAt  time.Time
}

// SnapshotStore is a Store with snapshot support for long-lived streams.
// SQLiteStore implements it; repositories should type-assert for it and
// fall back to full replay when the store does not support snapshots.
type SnapshotStore interface {
	Store
	LoadSnapshot(ctx context.Context, streamID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, snap Snapshot) error
	LoadAfter(ctx context.Context, streamID string, afterVersion int64) ([]Event, error)
}

// SaveSnapshot upserts the snapshot for a stream.
func (s *SQLiteStore) SaveSnapshot(ctx context.Context, snap Snapshot) error {
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

	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO snapshots (stream_id, stream_type, version, payload, created_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(stream_id) DO UPDATE SET
				stream_type = excluded.stream_type,
				version = excluded.version,
				payload = excluded.payload,
				created_at = excluded.created_at;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, snap.StreamID)
		stmt.BindText(2, snap.StreamType)
		stmt.BindInt64(3, snap.Version)
		stmt.BindText(4, string(snap.Payload))
		createdAt := snap.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		stmt.BindText(5, createdAt.Format(time.RFC3339Nano))
		_, err := stmt.Step()
		if err != nil {
			return fmt.Errorf("saving snapshot for stream %s: %w", snap.StreamID, err)
		}
		return nil
	})
}

// LoadSnapshot returns the latest snapshot for a stream.
func (s *SQLiteStore) LoadSnapshot(ctx context.Context, streamID string) (Snapshot, error) {
	var snap Snapshot
	var found bool

	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT stream_id, stream_type, version, payload, created_at FROM snapshots WHERE stream_id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, streamID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		found = true
		tStr := stmt.ColumnText(4)
		t, _ := time.Parse(time.RFC3339Nano, tStr)
		if t.IsZero() {
			t, _ = time.Parse("2006-01-02 15:04:05", tStr)
		}
		snap = Snapshot{
			StreamID:   stmt.ColumnText(0),
			StreamType: stmt.ColumnText(1),
			Version:    stmt.ColumnInt64(2),
			Payload:    []byte(stmt.ColumnText(3)),
			CreatedAt:  t,
		}
		return nil
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("loading snapshot for stream %s: %w", streamID, err)
	}
	if !found {
		return Snapshot{}, fmt.Errorf("stream %s: %w", streamID, ErrSnapshotNotFound)
	}
	return snap, nil
}

// LoadAfter loads events for a stream with version greater than afterVersion,
// ordered oldest to newest. Used with snapshots to skip replayed history.
func (s *SQLiteStore) LoadAfter(ctx context.Context, streamID string, afterVersion int64) ([]Event, error) {
	var events []Event

	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		query := tx.Prep("SELECT id, stream_id, stream_type, version, event_type, payload, metadata, created_at FROM events WHERE stream_id = ? AND version > ? ORDER BY version ASC;")
		defer func() { _ = query.Reset() }()
		query.BindText(1, streamID)
		query.BindInt64(2, afterVersion)

		for {
			hasRow, err := query.Step()
			if err != nil {
				return fmt.Errorf("stepping through events: %w", err)
			}
			if !hasRow {
				break
			}

			tStr := query.ColumnText(7)
			t, _ := time.Parse(time.RFC3339Nano, tStr)
			if t.IsZero() {
				t, _ = time.Parse("2006-01-02 15:04:05", tStr)
			}

			events = append(events, Event{
				ID:         query.ColumnText(0),
				StreamID:   query.ColumnText(1),
				StreamType: query.ColumnText(2),
				Version:    query.ColumnInt64(3),
				EventType:  query.ColumnText(4),
				Payload:    []byte(query.ColumnText(5)),
				Metadata:   []byte(query.ColumnText(6)),
				CreatedAt:  t,
			})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("loading stream %s after v%d: %w", streamID, afterVersion, err)
	}

	return events, nil
}