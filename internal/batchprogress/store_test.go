package batchprogress

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/batch"
	"ListenLedger/internal/eventsourcing"

	"zombiezen.com/go/sqlite"
)

func setupTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	tmpDir := t.TempDir()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })
	return NewStore(sqliteDB, eventsourcing.NewSQLiteStore(sqliteDB)), ctx
}

func seedArtist(t *testing.T, ctx context.Context, s *Store, id, fetchStatus string) {
	t.Helper()
	err := s.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs) VALUES (?, ?, ?, 0, 'rock_metal', 'included', ?, 0, 0);")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		stmt.BindText(2, "Artist "+id)
		stmt.BindText(3, "sp_"+id)
		stmt.BindText(4, fetchStatus)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("seed artist %s: %v", id, err)
	}
}

func setFetchStatus(t *testing.T, ctx context.Context, s *Store, id, status string) {
	t.Helper()
	err := s.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE artists SET fetch_status = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, status)
		stmt.BindText(2, id)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("set fetch status %s: %v", id, err)
	}
}

func TestStartAndSnapshot(t *testing.T) {
	s, ctx := setupTestStore(t)

	snap, err := s.Start(ctx, "b1", []string{"ar_1", "ar_2"}, map[string]int{"P5RockIncluded": 2}, time.Time{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if snap.Total != 2 || snap.Completed != 0 || snap.Done {
		t.Fatalf("start snap = %+v", snap)
	}

	got, found, err := s.Snapshot(ctx, "b1")
	if err != nil || !found {
		t.Fatalf("Snapshot = %+v, %t, %v", got, found, err)
	}
	if got.Total != 2 || got.Stats["P5RockIncluded"] != 2 {
		t.Fatalf("snapshot = %+v", got)
	}
	if _, found, _ := s.Snapshot(ctx, "missing"); found {
		t.Fatal("Snapshot(missing) should not be found")
	}

	empty, err := s.Start(ctx, "b_empty", nil, nil, time.Time{})
	if err != nil {
		t.Fatalf("Start empty: %v", err)
	}
	if !empty.Done {
		t.Fatal("empty batch should be Done at creation")
	}
}

func TestCompleteArtist(t *testing.T) {
	s, ctx := setupTestStore(t)

	if _, err := s.Start(ctx, "b1", []string{"ar_1", "ar_2"}, nil, time.Time{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Pending never advances.
	if id, err := s.CompleteArtist(ctx, "ar_1", "pending"); err != nil || id != "" {
		t.Fatalf("CompleteArtist(pending) = %q, %v", id, err)
	}
	// Unknown artist tracks nothing.
	if id, err := s.CompleteArtist(ctx, "ghost", "idle"); err != nil || id != "" {
		t.Fatalf("CompleteArtist(ghost) = %q, %v", id, err)
	}

	if id, err := s.CompleteArtist(ctx, "ar_1", "failed"); err != nil || id != "b1" {
		t.Fatalf("CompleteArtist(failed) = %q, %v", id, err)
	}
	got, _, _ := s.Snapshot(ctx, "b1")
	if got.Completed != 1 || got.Done {
		t.Fatalf("after 1/2: %+v", got)
	}

	// Duplicate completion recomputes the same counts.
	if _, err := s.CompleteArtist(ctx, "ar_1", "idle"); err != nil {
		t.Fatalf("duplicate CompleteArtist: %v", err)
	}
	got, _, _ = s.Snapshot(ctx, "b1")
	if got.Completed != 1 {
		t.Fatalf("after duplicate: %+v", got)
	}

	if _, err := s.CompleteArtist(ctx, "ar_2", "idle"); err != nil {
		t.Fatalf("CompleteArtist: %v", err)
	}
	got, _, _ = s.Snapshot(ctx, "b1")
	if got.Completed != 2 || !got.Done {
		t.Fatalf("after 2/2: %+v", got)
	}
}

func TestReconcileFromArtists(t *testing.T) {
	s, ctx := setupTestStore(t)

	seedArtist(t, ctx, s, "ar_1", "pending")
	seedArtist(t, ctx, s, "ar_2", "pending")
	if _, err := s.Start(ctx, "b1", []string{"ar_1", "ar_2"}, nil, time.Time{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// ar_1 finished without an event (missed NATS message).
	setFetchStatus(t, ctx, s, "ar_1", "idle")

	flipped, err := s.Reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if flipped != 1 {
		t.Fatalf("flipped = %d, want 1", flipped)
	}
	got, _, _ := s.Snapshot(ctx, "b1")
	if got.Completed != 1 || got.Done {
		t.Fatalf("after reconcile: %+v", got)
	}

	setFetchStatus(t, ctx, s, "ar_2", "failed")
	if _, err := s.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _, _ = s.Snapshot(ctx, "b1")
	if got.Completed != 2 || !got.Done {
		t.Fatalf("after full reconcile: %+v", got)
	}
}

func eventLog(t *testing.T, ctx context.Context, s *Store, streamID string) []string {
	t.Helper()
	var types []string
	err := s.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT event_type FROM events WHERE stream_id = ? ORDER BY version ASC;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, streamID)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			types = append(types, stmt.ColumnText(0))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("eventLog: %v", err)
	}
	return types
}

func TestLifecycleEventsLogged(t *testing.T) {
	s, ctx := setupTestStore(t)

	if _, err := s.Start(ctx, "b_log", []string{"ar_1", "ar_2"}, nil, time.Time{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := eventLog(t, ctx, s, "b_log"); len(got) != 1 || got[0] != batch.EventTypeBatchStarted {
		t.Fatalf("after Start: %v", got)
	}

	if _, err := s.CompleteArtist(ctx, "ar_1", "idle"); err != nil {
		t.Fatalf("CompleteArtist: %v", err)
	}
	if got := eventLog(t, ctx, s, "b_log"); len(got) != 2 || got[1] != batch.EventTypeBatchArtistCompleted {
		t.Fatalf("after completion: %v", got)
	}

	// Duplicate completion appends nothing.
	if _, err := s.CompleteArtist(ctx, "ar_1", "idle"); err != nil {
		t.Fatalf("duplicate CompleteArtist: %v", err)
	}
	if got := eventLog(t, ctx, s, "b_log"); len(got) != 2 {
		t.Fatalf("after duplicate: %v", got)
	}

	// Closing completion ends the bounded lifecycle in the log.
	if _, err := s.CompleteArtist(ctx, "ar_2", "failed"); err != nil {
		t.Fatalf("CompleteArtist: %v", err)
	}
	got := eventLog(t, ctx, s, "b_log")
	if len(got) != 4 || got[2] != batch.EventTypeBatchArtistCompleted || got[3] != batch.EventTypeBatchCompleted {
		t.Fatalf("after close: %v", got)
	}
}

func TestReplayRebuildsProjection(t *testing.T) {
	s, ctx := setupTestStore(t)

	if _, err := s.Start(ctx, "b_rb", []string{"ar_1", "ar_2"}, map[string]int{"P1RockRecent": 2}, time.Time{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := s.CompleteArtist(ctx, "ar_1", "idle"); err != nil {
		t.Fatalf("CompleteArtist: %v", err)
	}

	// Wipe projections (disposable) and rebuild purely from the log.
	if err := s.ResetForReplay(ctx); err != nil {
		t.Fatalf("ResetForReplay: %v", err)
	}
	if _, found, _ := s.Snapshot(ctx, "b_rb"); found {
		t.Fatal("snapshot should be gone after reset")
	}

	evts, err := s.events.Load(ctx, "b_rb")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	agg, err := batch.Replay("b_rb", evts)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if err := s.ProjectAggregate(ctx, agg); err != nil {
		t.Fatalf("ProjectAggregate: %v", err)
	}

	snap, found, err := s.Snapshot(ctx, "b_rb")
	if err != nil || !found {
		t.Fatalf("Snapshot = %+v, %t, %v", snap, found, err)
	}
	if snap.Total != 2 || snap.Completed != 1 || snap.Done || snap.Stats["P1RockRecent"] != 2 {
		t.Fatalf("rebuilt = %+v", snap)
	}
}

func TestActiveLatestAndPrune(t *testing.T) {
	s, ctx := setupTestStore(t)

	if _, err := s.Start(ctx, "b_old", []string{"ar_1"}, nil, time.Time{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := s.CompleteArtist(ctx, "ar_1", "idle"); err != nil {
		t.Fatalf("CompleteArtist: %v", err)
	}
	if _, err := s.Start(ctx, "b_new", []string{"ar_2"}, nil, time.Time{}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	active, found, err := s.ActiveSnapshot(ctx)
	if err != nil || !found || active.ID != "b_new" {
		t.Fatalf("Active = %+v, %t, %v", active, found, err)
	}
	latest, found, err := s.LatestSnapshot(ctx)
	if err != nil || !found || latest.ID != "b_new" {
		t.Fatalf("Latest = %+v, %t, %v", latest, found, err)
	}

	// Prune done batches older than now: b_old goes, b_new stays.
	pruned, err := s.Prune(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("pruned = %d, want 1", pruned)
	}
	if _, found, _ := s.Snapshot(ctx, "b_old"); found {
		t.Fatal("b_old should be pruned")
	}
	if _, found, _ := s.Snapshot(ctx, "b_new"); !found {
		t.Fatal("b_new should survive prune")
	}
}
