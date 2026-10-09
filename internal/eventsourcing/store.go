package eventsourcing

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"
)

// Store defines persistence operations for event streams.
type Store interface {
	Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error
	Load(ctx context.Context, streamID string) ([]Event, error)
}

// SQLiteStore is an event store backed by SQLite with optimistic concurrency protection.
type SQLiteStore struct {
	db *toolbeltdb.Database
}

// StreamInfo identifies a stream and its aggregate type without loading payloads.
type StreamInfo struct {
	ID   string
	Type string
}

// NewSQLiteStore creates a new SQLiteStore.
func NewSQLiteStore(db *toolbeltdb.Database) *SQLiteStore {
	return &SQLiteStore{db: db}
}

// insertEvent binds one event to a prepared INSERT statement and steps it.
// The statement is reset before binding so callers can reuse it across events.
func insertEvent(stmt *sqlite.Stmt, evt Event) error {
	_ = stmt.Reset()
	stmt.BindText(1, evt.ID)
	stmt.BindText(2, evt.StreamID)
	stmt.BindText(3, evt.StreamType)
	stmt.BindInt64(4, evt.Version)
	stmt.BindText(5, evt.EventType)
	stmt.BindText(6, string(evt.Payload))
	stmt.BindText(7, string(evt.Metadata))
	stmt.BindText(8, evt.CreatedAt.Format(time.RFC3339Nano))
	_, err := stmt.Step()
	return err
}

// Append persists uncommitted events to SQLite in an atomic transaction.
func (s *SQLiteStore) Append(ctx context.Context, streamID string, expectedVersion int64, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := validateEventBatch(streamID, expectedVersion, events); err != nil {
		return fmt.Errorf("validating event batch: %w", err)
	}

	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		// Use the stream index to fetch only the latest event instead of scanning
		// an artist's full history for every append.
		queryVersion := tx.Prep("SELECT version, stream_type FROM events WHERE stream_id = ? ORDER BY version DESC LIMIT 1;")
		defer func() { _ = queryVersion.Reset() }()
		queryVersion.BindText(1, streamID)

		hasRow, err := queryVersion.Step()
		if err != nil {
			return fmt.Errorf("checking stream version: %w", err)
		}

		var currentVersion int64
		var existingStreamType string
		if hasRow {
			currentVersion = queryVersion.ColumnInt64(0)
			existingStreamType = queryVersion.ColumnText(1)
		}

		if currentVersion != expectedVersion {
			return fmt.Errorf("%w: stream %s expected version %d, but found %d",
				ErrConcurrencyConflict, streamID, expectedVersion, currentVersion)
		}
		if existingStreamType != "" && existingStreamType != events[0].StreamType {
			return fmt.Errorf("stream %s has aggregate type %q, cannot append type %q", streamID, existingStreamType, events[0].StreamType)
		}

		// 2. Insert all events atomically
		insertStmt := tx.Prep("INSERT INTO events (id, stream_id, stream_type, version, event_type, payload, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = insertStmt.Reset() }()

		for _, evt := range events {
			if err := insertEvent(insertStmt, evt); err != nil {
				if strings.Contains(err.Error(), "UNIQUE constraint failed") {
					return fmt.Errorf("%w: %s v%d", ErrConcurrencyConflict, streamID, evt.Version)
				}
				return fmt.Errorf("inserting event %s v%d: %w", streamID, evt.Version, err)
			}
		}

		// 3. Enqueue the same events in the outbox in the SAME transaction.
		// The relay publishes them to JetStream DOMAIN_EVENTS; if this commit
		// succeeds but the synchronous projection publish crashes, the relay
		// backstops it. MsgID = event ID keeps the double-publish idempotent.
		outboxStmt := tx.Prep("INSERT OR IGNORE INTO outbox (event_id) VALUES (?);")
		defer func() { _ = outboxStmt.Reset() }()
		for _, evt := range events {
			_ = outboxStmt.Reset()
			outboxStmt.BindText(1, evt.ID)
			if _, err := outboxStmt.Step(); err != nil {
				return fmt.Errorf("enqueueing outbox event %s: %w", evt.ID, err)
			}
		}

		return nil
	})
}

// Load retrieves all events for a given stream ordered from first to last.
func (s *SQLiteStore) Load(ctx context.Context, streamID string) ([]Event, error) {
	var events []Event

	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		query := tx.Prep("SELECT rowid, id, stream_id, stream_type, version, event_type, payload, metadata, created_at FROM events WHERE stream_id = ? ORDER BY version ASC;")
		defer func() { _ = query.Reset() }()
		query.BindText(1, streamID)

		for {
			hasRow, err := query.Step()
			if err != nil {
				return fmt.Errorf("stepping through events: %w", err)
			}
			if !hasRow {
				break
			}

			tStr := query.ColumnText(8)
			t, _ := time.Parse(time.RFC3339Nano, tStr)
			if t.IsZero() {
				t, _ = time.Parse("2006-01-02 15:04:05", tStr)
			}

			events = append(events, Event{
				GlobalPosition: query.ColumnInt64(0),
				ID:             query.ColumnText(1),
				StreamID:       query.ColumnText(2),
				StreamType:     query.ColumnText(3),
				Version:        query.ColumnInt64(4),
				EventType:      query.ColumnText(5),
				Payload:        []byte(query.ColumnText(6)),
				Metadata:       []byte(query.ColumnText(7)),
				CreatedAt:      t,
			})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("loading stream %s: %w", streamID, err)
	}

	return events, nil
}

// Streams lists streams in first-event order and rejects inconsistent aggregate types.
func (s *SQLiteStore) Streams(ctx context.Context) ([]StreamInfo, error) {
	var streams []StreamInfo
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		query := tx.Prep(`SELECT stream_id, MIN(stream_type), COUNT(DISTINCT stream_type)
			FROM events GROUP BY stream_id ORDER BY MIN(rowid) ASC;`)
		defer func() { _ = query.Reset() }()
		for {
			hasRow, err := query.Step()
			if err != nil {
				return fmt.Errorf("reading stream metadata: %w", err)
			}
			if !hasRow {
				return nil
			}

			streamID := query.ColumnText(0)
			if typeCount := query.ColumnInt64(2); typeCount != 1 {
				return fmt.Errorf("stream %s has %d aggregate types", streamID, typeCount)
			}
			streams = append(streams, StreamInfo{
				ID:   streamID,
				Type: query.ColumnText(1),
			})
		}
	})
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}
	return streams, nil
}

// StreamIDs returns every stream id that has events, in insertion order.
func (s *SQLiteStore) StreamIDs(ctx context.Context) ([]string, error) {
	var ids []string
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		query := tx.Prep("SELECT DISTINCT stream_id FROM events ORDER BY rowid ASC;")
		defer func() { _ = query.Reset() }()
		for {
			hasRow, err := query.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			ids = append(ids, query.ColumnText(0))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}
	return ids, nil
}

// Replace rewrites one stream's events and is how history gets corrected.
//
// Safety: rewriting history violates append-only immutability. Production
// must append compensating events (new facts that correct) and rebuild
// projections via replay, never edit past facts. This method refuses unless
// LISTENLEDGER_ALLOW_EVENT_REWRITE=1 (tests / offline repair tooling only).
// It deletes the stream's events and snapshot, then re-enqueues the outbox
// like Append; callers must re-project.
func (s *SQLiteStore) Replace(ctx context.Context, streamID string, events []Event) error {
	if os.Getenv("LISTENLEDGER_ALLOW_EVENT_REWRITE") != "1" {
		return fmt.Errorf("refusing to rewrite stream %s: LISTENLEDGER_ALLOW_EVENT_REWRITE!=1 (append a compensating event instead)", streamID)
	}
	if err := validateEventBatch(streamID, 0, events); err != nil {
		return fmt.Errorf("validating replacement stream: %w", err)
	}
	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		del := tx.Prep("DELETE FROM events WHERE stream_id = ?;")
		defer func() { _ = del.Reset() }()
		del.BindText(1, streamID)
		if _, err := del.Step(); err != nil {
			return fmt.Errorf("clearing stream %s: %w", streamID, err)
		}

		// Drop the stream's snapshot too: it freezes pre-rewrite state and
		// loaders would otherwise restore stale facts plus a wrong tail.
		delSnap := tx.Prep("DELETE FROM snapshots WHERE stream_id = ?;")
		defer func() { _ = delSnap.Reset() }()
		delSnap.BindText(1, streamID)
		if _, err := delSnap.Step(); err != nil {
			return fmt.Errorf("clearing snapshot for stream %s: %w", streamID, err)
		}

		ins := tx.Prep("INSERT INTO events (id, stream_id, stream_type, version, event_type, payload, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = ins.Reset() }()
		for _, evt := range events {
			if err := insertEvent(ins, evt); err != nil {
				return fmt.Errorf("inserting replacement %s v%d: %w", streamID, evt.Version, err)
			}
		}

		// Re-enqueue the outbox like Append does: the DELETE above cascades
		// the stream's outbox rows, so without this the relay would never
		// publish the rewritten facts to JetStream.
		outboxStmt := tx.Prep("INSERT OR IGNORE INTO outbox (event_id) VALUES (?);")
		defer func() { _ = outboxStmt.Reset() }()
		for _, evt := range events {
			_ = outboxStmt.Reset()
			outboxStmt.BindText(1, evt.ID)
			if _, err := outboxStmt.Step(); err != nil {
				return fmt.Errorf("enqueueing outbox event %s: %w", evt.ID, err)
			}
		}
		return nil
	})
}

func validateEventBatch(streamID string, expectedVersion int64, events []Event) error {
	if streamID == "" {
		return fmt.Errorf("stream ID cannot be empty")
	}
	if expectedVersion < 0 {
		return fmt.Errorf("expected version cannot be negative: %d", expectedVersion)
	}
	if len(events) == 0 {
		return nil
	}

	streamType := events[0].StreamType
	if streamType == "" {
		return fmt.Errorf("event %s has an empty stream type", events[0].ID)
	}
	for i, evt := range events {
		if evt.ID == "" {
			return fmt.Errorf("event at version %d has an empty ID", evt.Version)
		}
		if evt.StreamID != streamID {
			return fmt.Errorf("event %s belongs to stream %s, want %s", evt.ID, evt.StreamID, streamID)
		}
		if evt.StreamType != streamType {
			return fmt.Errorf("stream %s has mixed aggregate types %q and %q", streamID, streamType, evt.StreamType)
		}
		if evt.EventType == "" {
			return fmt.Errorf("event %s has an empty event type", evt.ID)
		}
		wantVersion := expectedVersion + int64(i) + 1
		if evt.Version != wantVersion {
			return fmt.Errorf("event %s must be version %d, got %d", evt.ID, wantVersion, evt.Version)
		}
	}
	return nil
}

// UnpublishedEvents returns up to limit events whose outbox rows have no
// published_at timestamp, in append order. The outbox relay publishes these
// to JetStream DOMAIN_EVENTS and marks them via MarkPublished.
func (s *SQLiteStore) UnpublishedEvents(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	var events []Event
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		query := tx.Prep(`SELECT e.rowid, e.id, e.stream_id, e.stream_type, e.version, e.event_type, e.payload, e.metadata, e.created_at
			FROM events e JOIN outbox o ON o.event_id = e.id
			WHERE o.published_at IS NULL ORDER BY e.rowid ASC LIMIT ?;`)
		defer func() { _ = query.Reset() }()
		query.BindInt64(1, int64(limit))
		for {
			hasRow, err := query.Step()
			if err != nil {
				return fmt.Errorf("stepping through unpublished events: %w", err)
			}
			if !hasRow {
				break
			}
			tStr := query.ColumnText(8)
			t, _ := time.Parse(time.RFC3339Nano, tStr)
			if t.IsZero() {
				t, _ = time.Parse("2006-01-02 15:04:05", tStr)
			}
			events = append(events, Event{
				GlobalPosition: query.ColumnInt64(0),
				ID:             query.ColumnText(1),
				StreamID:       query.ColumnText(2),
				StreamType:     query.ColumnText(3),
				Version:        query.ColumnInt64(4),
				EventType:      query.ColumnText(5),
				Payload:        []byte(query.ColumnText(6)),
				Metadata:       []byte(query.ColumnText(7)),
				CreatedAt:      t,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("loading unpublished events: %w", err)
	}
	return events, nil
}

// MarkPublished stamps outbox rows with the publish time. Publish-then-mark
// means a crash between the two republishes on the next relay tick; MsgID =
// event ID dedups within the stream's duplicates window and consumers stay
// idempotent regardless.
func (s *SQLiteStore) MarkPublished(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE outbox SET published_at = ? WHERE event_id = ? AND published_at IS NULL;")
		defer func() { _ = stmt.Reset() }()
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for _, id := range eventIDs {
			_ = stmt.Reset()
			stmt.BindText(1, now)
			stmt.BindText(2, id)
			if _, err := stmt.Step(); err != nil {
				return fmt.Errorf("marking event %s published: %w", id, err)
			}
		}
		return nil
	})
}

// UnpublishedCount returns the number of outbox rows awaiting publish.
// Powers relay lag logging and health checks.
func (s *SQLiteStore) UnpublishedCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL;")
		defer func() { _ = stmt.Reset() }()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			count = stmt.ColumnInt64(0)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("counting unpublished events: %w", err)
	}
	return count, nil
}

// LoadFromGlobalPosition loads events across all streams starting after afterPosition (exclusive), ordered by global sequence.
func (s *SQLiteStore) LoadFromGlobalPosition(ctx context.Context, afterPosition int64, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	var events []Event
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		query := tx.Prep(`SELECT rowid, id, stream_id, stream_type, version, event_type, payload, metadata, created_at
			FROM events WHERE rowid > ? ORDER BY rowid ASC LIMIT ?;`)
		defer func() { _ = query.Reset() }()
		query.BindInt64(1, afterPosition)
		query.BindInt64(2, int64(limit))
		for {
			hasRow, err := query.Step()
			if err != nil {
				return fmt.Errorf("stepping through events by position: %w", err)
			}
			if !hasRow {
				break
			}
			tStr := query.ColumnText(8)
			t, _ := time.Parse(time.RFC3339Nano, tStr)
			if t.IsZero() {
				t, _ = time.Parse("2006-01-02 15:04:05", tStr)
			}
			events = append(events, Event{
				GlobalPosition: query.ColumnInt64(0),
				ID:             query.ColumnText(1),
				StreamID:       query.ColumnText(2),
				StreamType:     query.ColumnText(3),
				Version:        query.ColumnInt64(4),
				EventType:      query.ColumnText(5),
				Payload:        []byte(query.ColumnText(6)),
				Metadata:       []byte(query.ColumnText(7)),
				CreatedAt:      t,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("loading events from position %d: %w", afterPosition, err)
	}
	return events, nil
}

// Projection checkpoint names. Checkpoints record the last global position
// (events.rowid) each consumer folded, so replays and relays resume instead
// of restarting from zero. Projection checkpoints use stream-type names;
// the relay tracks its own publish frontier separately.
const (
	CheckpointArtistProjection  = "artist"
	CheckpointAlbumProjection   = "album"
	CheckpointSongProjection    = "song"
	CheckpointBatchProjection   = "batch"
	CheckpointDomainEventsRelay = "domain_events_relay"
)

// GetCheckpoint retrieves the last processed global position for a given projection.
func (s *SQLiteStore) GetCheckpoint(ctx context.Context, projectionName string) (int64, error) {
	var position int64
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT last_position FROM projection_checkpoints WHERE projection_name = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, projectionName)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			position = stmt.ColumnInt64(0)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("getting checkpoint for %s: %w", projectionName, err)
	}
	return position, nil
}

// SaveCheckpoint saves or updates the last processed global position for a given projection.
func (s *SQLiteStore) SaveCheckpoint(ctx context.Context, projectionName string, position int64) error {
	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
			VALUES (?, ?, ?)
			ON CONFLICT(projection_name) DO UPDATE SET
				last_position = excluded.last_position,
				updated_at = excluded.updated_at;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, projectionName)
		stmt.BindInt64(2, position)
		stmt.BindText(3, time.Now().UTC().Format(time.RFC3339Nano))
		_, err := stmt.Step()
		return err
	})
}
