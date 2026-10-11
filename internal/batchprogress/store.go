// Package batchprogress projects batch aggregates into SQLite: batches +
// batch_members replace the in-memory progress maps so batch state survives
// restarts. completed is always recomputed from members (never blindly
// incremented), making duplicate completion events harmless.
package batchprogress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/batch"
	"ListenLedger/internal/eventsourcing"
)

// Snapshot is the read-side view of one batch.
type Snapshot struct {
	ID        string         `json:"id"`
	Stats     map[string]int `json:"stats"`
	Total     int            `json:"total"`
	Completed int            `json:"completed"`
	Done      bool           `json:"done"`
}

// Store projects batch lifecycle into SQLite.
type Store struct {
	db *toolbeltdb.Database
	// events is the log the projection derives from. Every lifecycle fact is
	// appended here first (past-tense events on one bounded stream per batch);
	// projection writes follow. If the projection write fails, replay rebuilds
	// it — the log, never the tables, is the source of truth.
	events eventsourcing.Store
}

// NewStore creates a Store. events is the authoritative event log (SQLite by
// default, JetStream when LISTENLEDGER_EVENT_STORE=jetstream); callers pass
// the selected store so reads and writes follow the flip together.
func NewStore(db *toolbeltdb.Database, events eventsourcing.Store) *Store {
	return &Store{db: db, events: events}
}

func encodeStats(stats map[string]int) string {
	if len(stats) == 0 {
		return "{}"
	}
	body, err := json.Marshal(stats)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func decodeStats(raw string) map[string]int {
	stats := make(map[string]int)
	if strings.TrimSpace(raw) == "" {
		return stats
	}
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		return make(map[string]int)
	}
	return stats
}

// Start records BatchStarted: one batch row plus one pending member per
// artist, and prunes done batches older than pruneBefore (zero time skips).
func (s *Store) Start(ctx context.Context, id string, artistIDs []string, stats map[string]int, pruneBefore time.Time) (Snapshot, error) {
	if strings.TrimSpace(id) == "" {
		return Snapshot{}, fmt.Errorf("batch id cannot be empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	snap := Snapshot{ID: id, Stats: stats, Total: len(artistIDs)}
	if snap.Stats == nil {
		snap.Stats = make(map[string]int)
	}

	// Log first: the BatchStarted fact on the batch's bounded stream.
	// A concurrency conflict means this batch already started (retry-safe);
	// the idempotent projection write below still converges.
	if err := s.appendStarted(ctx, id, artistIDs, stats); err != nil {
		return Snapshot{}, err
	}

	err := s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO batches (id, total, completed, done, stats, created_at, updated_at)
			VALUES (?, ?, 0, ?, ?, ?, ?)
			ON CONFLICT(id) DO NOTHING;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		stmt.BindInt64(2, int64(len(artistIDs)))
		done := int64(0)
		if len(artistIDs) == 0 {
			done = 1
			snap.Done = true
		}
		stmt.BindInt64(3, done)
		stmt.BindText(4, encodeStats(stats))
		stmt.BindText(5, now)
		stmt.BindText(6, now)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("inserting batch %s: %w", id, err)
		}

		mem := tx.Prep("INSERT OR IGNORE INTO batch_members (batch_id, artist_id, done) VALUES (?, ?, 0);")
		defer func() { _ = mem.Reset() }()
		for _, artistID := range artistIDs {
			_ = mem.Reset()
			mem.BindText(1, id)
			mem.BindText(2, artistID)
			if _, err := mem.Step(); err != nil {
				return fmt.Errorf("inserting member %s/%s: %w", id, artistID, err)
			}
		}

		if !pruneBefore.IsZero() {
			prune := tx.Prep("DELETE FROM batches WHERE done = 1 AND updated_at < ?;")
			defer func() { _ = prune.Reset() }()
			prune.BindText(1, pruneBefore.UTC().Format(time.RFC3339Nano))
			if _, err := prune.Step(); err != nil {
				return fmt.Errorf("pruning batches: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// IsCompletedStatus reports whether a fetch status terminally completes a
// batch member (idle or failed; pending never advances).
func IsCompletedStatus(fetchStatus string) bool {
	return fetchStatus == "idle" || fetchStatus == "failed"
}

// CompleteArtist records BatchArtistCompleted for the artist's most recent
// unfinished batch, then folds it into the projection. Duplicate completions
// record nothing (no new fact) and leave counts converged.
// Returns the completed batch ID, or "" when the artist tracks nothing.
//
// Fail-closed on the log write: the projection stays untouched so Reconcile
// (which reads artist fetch states) backstops the missed fact.
func (s *Store) CompleteArtist(ctx context.Context, artistID, fetchStatus string) (string, error) {
	if strings.TrimSpace(artistID) == "" || !IsCompletedStatus(fetchStatus) {
		return "", nil
	}
	batchID, err := s.findOpenBatchForArtist(ctx, artistID)
	if err != nil {
		return "", err
	}
	if batchID == "" {
		return "", nil
	}
	if err := s.appendCompletion(ctx, batchID, artistID); err != nil {
		return "", err
	}
	if err := s.markMemberDone(ctx, batchID, artistID); err != nil {
		return "", err
	}
	return batchID, nil
}

func (s *Store) findOpenBatchForArtist(ctx context.Context, artistID string) (string, error) {
	var batchID string
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		find := tx.Prep(`SELECT m.batch_id FROM batch_members m
			JOIN batches b ON b.id = m.batch_id
			WHERE m.artist_id = ? AND m.done = 0 AND b.done = 0
			ORDER BY b.updated_at DESC, b.rowid DESC LIMIT 1;`)
		defer func() { _ = find.Reset() }()
		find.BindText(1, artistID)
		hasRow, err := find.Step()
		if err != nil {
			return fmt.Errorf("finding batch for artist %s: %w", artistID, err)
		}
		if !hasRow {
			return nil
		}
		batchID = find.ColumnText(0)
		return nil
	})
	if err != nil {
		return "", err
	}
	return batchID, nil
}

func (s *Store) markMemberDone(ctx context.Context, batchID, artistID string) error {
	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		mark := tx.Prep("UPDATE batch_members SET done = 1 WHERE batch_id = ? AND artist_id = ? AND done = 0;")
		defer func() { _ = mark.Reset() }()
		mark.BindText(1, batchID)
		mark.BindText(2, artistID)
		if _, err := mark.Step(); err != nil {
			return fmt.Errorf("completing member %s/%s: %w", batchID, artistID, err)
		}

		if err := s.recomputeLocked(tx, batchID); err != nil {
			return err
		}
		return nil
	})
}

// appendStarted logs BatchStarted on the batch's bounded stream. A conflict
// means the batch already started (Start is retry-safe); the idempotent
// projection write still converges.
func (s *Store) appendStarted(ctx context.Context, id string, artistIDs []string, stats map[string]int) error {
	agg, err := batch.NewBatch(id, artistIDs, stats)
	if err != nil {
		return fmt.Errorf("starting batch aggregate %s: %w", id, err)
	}
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil
	}
	if err := eventsourcing.AppendWithRetry(ctx, s.events, id, 0, uncommitted...); err != nil {
		if isBatchStreamConflict(err) {
			return nil
		}
		return fmt.Errorf("appending BatchStarted %s: %w", id, err)
	}
	return nil
}

// appendCompletion logs BatchArtistCompleted (plus BatchCompleted when the
// lifecycle ends) for one member. No new fact (unknown/duplicate member)
// appends nothing. Missing streams (batches born before the event log) are
// seeded from current projection state first.
func (s *Store) appendCompletion(ctx context.Context, batchID, artistID string) error {
	agg, err := s.loadAggregate(ctx, batchID)
	if err != nil {
		return err
	}
	base := agg.Version()
	if _, recorded, err := agg.RecordCompletion(artistID); err != nil {
		return fmt.Errorf("recording completion %s/%s: %w", batchID, artistID, err)
	} else if !recorded {
		return nil
	}
	if _, _, err := agg.RecordClosed(); err != nil {
		return fmt.Errorf("recording close %s: %w", batchID, err)
	}
	uncommitted := agg.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil
	}
	if err := eventsourcing.AppendWithRetry(ctx, s.events, batchID, base, uncommitted...); err != nil {
		return fmt.Errorf("appending completion %s/%s: %w", batchID, artistID, err)
	}
	return nil
}

func isBatchStreamConflict(err error) bool {
	return err != nil && (errors.Is(err, eventsourcing.ErrConcurrencyConflict) ||
		strings.Contains(err.Error(), "UNIQUE constraint failed"))
}

// loadAggregate reconstitutes a batch from its stream, seeding pre-log
// batches from projection state on first touch.
func (s *Store) loadAggregate(ctx context.Context, batchID string) (*batch.Batch, error) {
	evts, err := s.events.Load(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("loading batch stream %s: %w", batchID, err)
	}
	if len(evts) == 0 {
		if err := s.seedStreamFromProjection(ctx, batchID); err != nil {
			return nil, err
		}
		evts, err = s.events.Load(ctx, batchID)
		if err != nil {
			return nil, fmt.Errorf("reloading seeded batch stream %s: %w", batchID, err)
		}
		if len(evts) == 0 {
			return nil, fmt.Errorf("batch stream %s has no events after seeding", batchID)
		}
	}
	return batch.Replay(batchID, evts)
}

// seedStreamFromProjection bootstraps a BatchStarted fact for batches born
// before the event log, from current projection rows (member list + stats
// are real state, not invention). Races resolve via stream conflict.
func (s *Store) seedStreamFromProjection(ctx context.Context, batchID string) error {
	var memberIDs []string
	var statsRaw string
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		bstmt := tx.Prep("SELECT stats FROM batches WHERE id = ? LIMIT 1;")
		defer func() { _ = bstmt.Reset() }()
		bstmt.BindText(1, batchID)
		hasRow, err := bstmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return fmt.Errorf("batch %s not found for seeding", batchID)
		}
		statsRaw = bstmt.ColumnText(0)

		mstmt := tx.Prep("SELECT artist_id FROM batch_members WHERE batch_id = ? ORDER BY rowid ASC;")
		defer func() { _ = mstmt.Reset() }()
		mstmt.BindText(1, batchID)
		for {
			hasRow, err := mstmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			memberIDs = append(memberIDs, mstmt.ColumnText(0))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("seeding batch %s: %w", batchID, err)
	}
	stats := make(map[string]int)
	if strings.TrimSpace(statsRaw) != "" && strings.TrimSpace(statsRaw) != "{}" {
		_ = json.Unmarshal([]byte(statsRaw), &stats)
	}
	agg, err := batch.NewBatch(batchID, memberIDs, stats)
	if err != nil {
		return fmt.Errorf("seeding batch aggregate %s: %w", batchID, err)
	}
	if err := eventsourcing.AppendWithRetry(ctx, s.events, batchID, 0, agg.UncommittedEvents()...); err != nil {
		if isBatchStreamConflict(err) {
			return nil
		}
		return fmt.Errorf("appending seeded BatchStarted %s: %w", batchID, err)
	}
	return nil
}

func (s *Store) recomputeLocked(tx *sqlite.Conn, batchID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	count := tx.Prep(`UPDATE batches SET completed = (SELECT COUNT(*) FROM batch_members WHERE batch_id = ? AND done = 1), updated_at = ? WHERE id = ?;`)
	defer func() { _ = count.Reset() }()
	count.BindText(1, batchID)
	count.BindText(2, now)
	count.BindText(3, batchID)
	if _, err := count.Step(); err != nil {
		return fmt.Errorf("recomputing batch %s: %w", batchID, err)
	}
	flag := tx.Prep("UPDATE batches SET done = 1 WHERE id = ? AND total > 0 AND completed >= total;")
	defer func() { _ = flag.Reset() }()
	flag.BindText(1, batchID)
	if _, err := flag.Step(); err != nil {
		return fmt.Errorf("closing batch %s: %w", batchID, err)
	}
	return nil
}

// Reconcile completes members whose artist already left fetch_status='pending'
// (missed NATS events, restarts), logs each as a BatchArtistCompleted fact,
// and recomputes every unfinished batch. Returns the number flipped.
func (s *Store) Reconcile(ctx context.Context) (int64, error) {
	type flip struct{ batchID, artistID string }
	var flips []flip
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		sel := tx.Prep(`SELECT m.batch_id, m.artist_id FROM batch_members m
			JOIN batches b ON b.id = m.batch_id
			WHERE m.done = 0 AND b.done = 0
			AND m.artist_id NOT IN (SELECT id FROM artists WHERE fetch_status = 'pending');`)
		defer func() { _ = sel.Reset() }()
		for {
			hasRow, err := sel.Step()
			if err != nil {
				return fmt.Errorf("selecting reconciled members: %w", err)
			}
			if !hasRow {
				break
			}
			flips = append(flips, flip{batchID: sel.ColumnText(0), artistID: sel.ColumnText(1)})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(flips) == 0 {
		return 0, nil
	}

	err = s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		// Flip exactly the members collected above: re-running the status
		// predicate here could mark a member done that left pending after
		// the read, while its completion event below would never be logged.
		flip := tx.Prep(`UPDATE batch_members SET done = 1 WHERE done = 0 AND batch_id = ? AND artist_id = ?;`)
		defer func() { _ = flip.Reset() }()
		for _, f := range flips {
			_ = flip.Reset()
			flip.BindText(1, f.batchID)
			flip.BindText(2, f.artistID)
			if _, err := flip.Step(); err != nil {
				return fmt.Errorf("flipping reconciled member %s/%s: %w", f.batchID, f.artistID, err)
			}
		}

		now := time.Now().UTC().Format(time.RFC3339Nano)
		count := tx.Prep(`UPDATE batches SET completed = (SELECT COUNT(*) FROM batch_members WHERE batch_id = batches.id AND done = 1), updated_at = ? WHERE done = 0;`)
		defer func() { _ = count.Reset() }()
		count.BindText(1, now)
		if _, err := count.Step(); err != nil {
			return fmt.Errorf("recomputing batches: %w", err)
		}
		flag := tx.Prep("UPDATE batches SET done = 1 WHERE done = 0 AND total > 0 AND completed >= total;")
		defer func() { _ = flag.Reset() }()
		if _, err := flag.Step(); err != nil {
			return fmt.Errorf("closing batches: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Log each catch-up completion after the projection converges. A crash
	// between the two reruns the same convergent outcome; RecordCompletion
	// emits nothing for already-logged facts.
	for _, f := range flips {
		if err := ctx.Err(); err != nil {
			return int64(len(flips)), err
		}
		if err := s.appendCompletion(ctx, f.batchID, f.artistID); err != nil {
			return int64(len(flips)), fmt.Errorf("logging reconciled completion %s/%s: %w", f.batchID, f.artistID, err)
		}
	}
	return int64(len(flips)), nil
}

// ResetForReplay clears batch projections before event replay.
func (s *Store) ResetForReplay(ctx context.Context) error {
	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		for _, query := range []string{"DELETE FROM batch_members;", "DELETE FROM batches;"} {
			stmt := tx.Prep(query)
			_, err := stmt.Step()
			_ = stmt.Reset()
			if err != nil {
				return fmt.Errorf("resetting batches for replay: %w", err)
			}
		}
		return nil
	})
}

// ProjectAggregate writes a fully folded batch aggregate (replay path).
func (s *Store) ProjectAggregate(ctx context.Context, agg *batch.Batch) error {
	members := agg.Members()
	stats := agg.Stats()
	completed := agg.Completed()
	done := 0
	if agg.Done() {
		done = 1
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO batches (id, total, completed, done, stats, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET total = excluded.total, completed = excluded.completed,
				done = excluded.done, stats = excluded.stats, updated_at = excluded.updated_at;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, agg.AggregateID())
		stmt.BindInt64(2, int64(len(members)))
		stmt.BindInt64(3, int64(completed))
		stmt.BindInt64(4, int64(done))
		stmt.BindText(5, encodeStats(stats))
		stmt.BindText(6, now)
		stmt.BindText(7, now)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("projecting batch %s: %w", agg.AggregateID(), err)
		}
		mem := tx.Prep("INSERT INTO batch_members (batch_id, artist_id, done) VALUES (?, ?, ?) ON CONFLICT(batch_id, artist_id) DO UPDATE SET done = excluded.done;")
		defer func() { _ = mem.Reset() }()
		for artistID, isDone := range members {
			_ = mem.Reset()
			mem.BindText(1, agg.AggregateID())
			mem.BindText(2, artistID)
			doneFlag := int64(0)
			if isDone {
				doneFlag = 1
			}
			mem.BindInt64(3, doneFlag)
			if _, err := mem.Step(); err != nil {
				return fmt.Errorf("projecting member %s/%s: %w", agg.AggregateID(), artistID, err)
			}
		}
		return nil
	})
}

// Prune deletes done batches updated before `before` (members cascade).
func (s *Store) Prune(ctx context.Context, before time.Time) (int64, error) {
	var pruned int64
	err := s.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("DELETE FROM batches WHERE done = 1 AND updated_at < ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, before.UTC().Format(time.RFC3339Nano))
		if _, err := stmt.Step(); err != nil {
			return err
		}
		pruned = int64(tx.Changes())
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("pruning batches: %w", err)
	}
	return pruned, nil
}

func scanSnapshot(stmt *sqlite.Stmt) Snapshot {
	return Snapshot{
		ID:        stmt.ColumnText(0),
		Total:     int(stmt.ColumnInt64(1)),
		Completed: int(stmt.ColumnInt64(2)),
		Done:      stmt.ColumnInt64(3) != 0,
		Stats:     decodeStats(stmt.ColumnText(4)),
	}
}

// Snapshot returns one batch by ID.
func (s *Store) Snapshot(ctx context.Context, id string) (Snapshot, bool, error) {
	var snap Snapshot
	var found bool
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT id, total, completed, done, stats FROM batches WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		found = true
		snap = scanSnapshot(stmt)
		return nil
	})
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("reading batch %s: %w", id, err)
	}
	return snap, found, nil
}

// latestSnapshot returns the newest batch matching the done predicate.
func (s *Store) latestSnapshot(ctx context.Context, unfinishedOnly bool) (Snapshot, bool, error) {
	query := "SELECT id, total, completed, done, stats FROM batches"
	if unfinishedOnly {
		query += " WHERE done = 0"
	}
	query += " ORDER BY updated_at DESC, rowid DESC LIMIT 1;"
	var snap Snapshot
	var found bool
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		found = true
		snap = scanSnapshot(stmt)
		return nil
	})
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("reading latest batch: %w", err)
	}
	return snap, found, nil
}

// ActiveSnapshot returns the most recently updated unfinished batch.
func (s *Store) ActiveSnapshot(ctx context.Context) (Snapshot, bool, error) {
	return s.latestSnapshot(ctx, true)
}

// LatestSnapshot returns the most recently updated batch, done or not.
func (s *Store) LatestSnapshot(ctx context.Context) (Snapshot, bool, error) {
	return s.latestSnapshot(ctx, false)
}
