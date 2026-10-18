// Package readside tracks how far each stream's SQLite read model has been
// projected. Projections advance the position inside the same transaction as
// their writes, so the position is exactly as durable as the read model; the
// healer reactor compares incoming event versions against it to skip streams
// already caught up and to find streams the inline path left behind.
package readside

import (
	"context"
	"fmt"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/eventsourcing"
)

// AdvancePosition records that the read model has been projected through
// version for one stream. It must run inside the same transaction as the
// projection writes: a crash rolls both back, leaving the healer to repair
// from the log. Callers pass the stream ID/type and the highest applied
// event version.
func AdvancePosition(tx *sqlite.Conn, streamID, streamType string, version int64) error {
	if streamID == "" || streamType == "" || version <= 0 {
		return fmt.Errorf("readside: invalid position stream=%q type=%q version=%d", streamID, streamType, version)
	}
	stmt := tx.Prep(`INSERT INTO read_model_positions (stream_id, stream_type, version, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(stream_id) DO UPDATE SET
			stream_type = excluded.stream_type,
			version = excluded.version,
			updated_at = excluded.updated_at
		WHERE excluded.version > read_model_positions.version;`)
	defer func() { _ = stmt.Reset() }()
	stmt.BindText(1, streamID)
	stmt.BindText(2, streamType)
	stmt.BindInt64(3, version)
	stmt.BindText(4, time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("readside: updating position for %s: %w", streamID, err)
	}
	return nil
}

// AdvancePositions records the highest version reached per stream among
// events, in the caller's transaction. Projections call it once per batch so
// the recorded position always commits with the rows it describes.
//
// Contiguity: a stream advances only when its lowest version in the batch is
// at most stored position + 1, so the position never jumps across an
// unprojected event. A gap would let a later write claim the skipped fact
// was folded and hide it from the healer, leaving cmd/replay as the only
// repair. With a gap the position stays put and the next heal refolds the
// whole stream, which is always contiguous.
func AdvancePositions(tx *sqlite.Conn, events []eventsourcing.Event) error {
	type span struct {
		lo, hi int64
		typ    string
	}
	spans := make(map[string]*span, 1)
	for _, evt := range events {
		s, ok := spans[evt.StreamID]
		if !ok {
			spans[evt.StreamID] = &span{lo: evt.Version, hi: evt.Version, typ: evt.StreamType}
			continue
		}
		if evt.Version < s.lo {
			s.lo = evt.Version
		}
		if evt.Version > s.hi {
			s.hi, s.typ = evt.Version, evt.StreamType
		}
	}
	for streamID, s := range spans {
		pos, err := PositionTx(tx, streamID)
		if err != nil {
			return err
		}
		if s.lo > pos+1 {
			continue
		}
		if err := AdvancePosition(tx, streamID, s.typ, s.hi); err != nil {
			return err
		}
	}
	return nil
}

// ClearPositionTx removes the recorded position for a stream inside the
// caller's transaction, forcing the next projection to re-fold every event.
// Only offline repair paths (cmd/replay --since-checkpoint on drifted rows)
// should use it; the healer relies on positions to skip caught-up streams.
func ClearPositionTx(tx *sqlite.Conn, streamID string) error {
	stmt := tx.Prep("DELETE FROM read_model_positions WHERE stream_id = ?;")
	defer func() { _ = stmt.Reset() }()
	stmt.BindText(1, streamID)
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("readside: clearing position for %s: %w", streamID, err)
	}
	return nil
}

// Position returns the version through which a stream's read model has been
// projected (0 when nothing is recorded). The healer uses it to skip caught-up
// streams; a value at or above an incoming event's version means the inline
// projection already committed that event.
func Position(ctx context.Context, db *toolbeltdb.Database, streamID string) (int64, error) {
	var version int64
	err := db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT version FROM read_model_positions WHERE stream_id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, streamID)
		hasRow, err := stmt.Step()
		if err != nil {
			return fmt.Errorf("readside: reading position for %s: %w", streamID, err)
		}
		if hasRow {
			version = stmt.ColumnInt64(0)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

// PositionTx is Position on the writer connection, inside the caller's
// transaction. Projections read it to skip events a concurrent projection
// already committed: SQLite serializes write transactions, so the check is
// race-free, and re-applying older events would regress rows while the
// monotonic position stays ahead — drift the healer could never repair.
func PositionTx(tx *sqlite.Conn, streamID string) (int64, error) {
	stmt := tx.Prep("SELECT version FROM read_model_positions WHERE stream_id = ? LIMIT 1;")
	defer func() { _ = stmt.Reset() }()
	stmt.BindText(1, streamID)
	hasRow, err := stmt.Step()
	if err != nil {
		return 0, fmt.Errorf("readside: reading position for %s: %w", streamID, err)
	}
	if hasRow {
		return stmt.ColumnInt64(0), nil
	}
	return 0, nil
}
