package handlers

import (
	"context"
	"os"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/domain/artist"
)

const queueTestTS = "2006-01-02 15:04:05.000Z"

func seedQueueTestArtist(t *testing.T, ctx context.Context, h *Handler, id, fetchStatus string) {
	t.Helper()
	err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
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

func seedQueueTestJob(t *testing.T, ctx context.Context, h *Handler, id, requestID, artistID, status, queuedAt string) {
	t.Helper()
	err := h.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at) VALUES (?, ?, ?, ?, 0, '', ?);")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		stmt.BindText(2, requestID)
		stmt.BindText(3, artistID)
		stmt.BindText(4, status)
		stmt.BindText(5, queuedAt)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("seed job %s: %v", id, err)
	}
}

func queryJobStatus(t *testing.T, ctx context.Context, h *Handler, requestID string) string {
	t.Helper()
	var status string
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status FROM scrape_jobs WHERE request_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			status = stmt.ColumnText(0)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query job %s: %v", requestID, err)
	}
	return status
}

func queryArtistFetchStatus(t *testing.T, ctx context.Context, h *Handler, artistID string) string {
	t.Helper()
	var status string
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT fetch_status FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			status = stmt.ColumnText(0)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query artist %s: %v", artistID, err)
	}
	return status
}

func TestCountQueueState_SQLite(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	seedQueueTestArtist(t, ctx, h, "ar_q1", "pending")
	seedQueueTestArtist(t, ctx, h, "ar_q2", "idle")
	now := time.Now().UTC().Format(queueTestTS)
	seedQueueTestJob(t, ctx, h, "job_q1", "req_q1", "ar_q1", "queued", now)
	seedQueueTestJob(t, ctx, h, "job_q2", "req_q2", "ar_q1", "processing", now)
	seedQueueTestJob(t, ctx, h, "job_q3", "req_q3", "ar_q2", "failed", now)

	queued, processing, failed, pending := h.countQueueState(ctx)
	if queued != 1 || processing != 1 || failed != 1 || pending != 1 {
		t.Fatalf("countQueueState = (%d,%d,%d,%d), want (1,1,1,1)", queued, processing, failed, pending)
	}
}

func TestExpireStaleQueuedJobs_SQLite(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	seedQueueTestArtist(t, ctx, h, "ar_e1", "idle")
	stale := time.Now().UTC().Add(-25 * time.Hour).Format(queueTestTS)
	fresh := time.Now().UTC().Format(queueTestTS)
	seedQueueTestJob(t, ctx, h, "job_e1", "req_e1", "ar_e1", "queued", stale)
	seedQueueTestJob(t, ctx, h, "job_e2", "req_e2", "ar_e1", "queued", fresh)

	var stats queueRetryStats
	if err := h.expireStaleQueuedJobs(ctx, &stats); err != nil {
		t.Fatalf("expireStaleQueuedJobs: %v", err)
	}
	if stats.StaleQueuedExpired != 1 {
		t.Fatalf("StaleQueuedExpired = %d, want 1", stats.StaleQueuedExpired)
	}
	if got := queryJobStatus(t, ctx, h, "req_e1"); got != "failed" {
		t.Fatalf("stale job status = %q, want failed", got)
	}
	if got := queryJobStatus(t, ctx, h, "req_e2"); got != "queued" {
		t.Fatalf("fresh job status = %q, want queued", got)
	}
	// The expired request keeps its own ScrapeFailed fact (seed + failure
	// commit atomically); the fresh job has no stream.
	if got := streamEventTypes(t, ctx, h, "req_e1"); len(got) != 2 || got[0] != "ScrapeRequested" || got[1] != "ScrapeFailed" {
		t.Fatalf("req_e1 stream = %v", got)
	}
	if got := streamEventTypes(t, ctx, h, "req_e2"); len(got) != 0 {
		t.Fatalf("req_e2 stream = %v, want empty", got)
	}
}

func streamEventTypes(t *testing.T, ctx context.Context, h *Handler, streamID string) []string {
	t.Helper()
	var types []string
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
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
		t.Fatalf("stream %s: %v", streamID, err)
	}
	return types
}

func TestResetOrphanPendingArtists_SQLite(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	// Orphan: pending with no job row at all.
	seedQueueTestArtist(t, ctx, h, "ar_o1", "pending")
	// Guarded: pending with a live queued job.
	seedQueueTestArtist(t, ctx, h, "ar_o2", "pending")
	now := time.Now().UTC().Format(queueTestTS)
	seedQueueTestJob(t, ctx, h, "job_o2", "req_o2", "ar_o2", "queued", now)
	correlation.Associate("ar_o1", "req_orphan")

	// Seed the orphan's aggregate stream so convergence has a log to extend.
	seedPendingAggregate(t, ctx, h, "ar_o1")

	var stats queueRetryStats
	if err := h.resetOrphanPendingArtists(ctx, &stats); err != nil {
		t.Fatalf("resetOrphanPendingArtists: %v", err)
	}
	if stats.OrphanPendingReset != 1 {
		t.Fatalf("OrphanPendingReset = %d, want 1", stats.OrphanPendingReset)
	}
	if got := queryArtistFetchStatus(t, ctx, h, "ar_o1"); got != "idle" {
		t.Fatalf("orphan fetch_status = %q, want idle", got)
	}
	if got := queryArtistFetchStatus(t, ctx, h, "ar_o2"); got != "pending" {
		t.Fatalf("guarded fetch_status = %q, want pending", got)
	}
	if got := correlation.Get("ar_o1"); got != "" {
		t.Fatalf("correlation for ar_o1 = %q, want cleared", got)
	}
	// Convergence: the table flip extended the aggregate stream.
	if got := streamEventTypes(t, ctx, h, "ar_o1"); len(got) != 3 || got[2] != "ArtistFetchStatusChanged" {
		t.Fatalf("ar_o1 stream = %v", got)
	}
}

// seedPendingAggregate writes Created + FetchStatusChanged(pending) events for
// an already-seeded artist row, giving convergence paths a stream to extend.
func seedPendingAggregate(t *testing.T, ctx context.Context, h *Handler, artistID string) {
	t.Helper()
	agg, err := artist.NewArtist(artistID, "Artist "+artistID, "sp_"+artistID, "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if _, err := h.artistRepo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := h.artistRepo.Load(ctx, artistID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := loaded.SetFetchStatus("pending", ""); err != nil {
		t.Fatalf("SetFetchStatus: %v", err)
	}
	if _, err := h.artistRepo.Save(ctx, loaded); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func TestMarkFailedJobArtists_SQLite(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	// Failed-only job: pending -> failed.
	seedQueueTestArtist(t, ctx, h, "ar_f1", "pending")
	// Failed + live queued job: stays pending.
	seedQueueTestArtist(t, ctx, h, "ar_f2", "pending")
	now := time.Now().UTC().Format(queueTestTS)
	seedQueueTestJob(t, ctx, h, "job_f1", "req_f1", "ar_f1", "failed", now)
	seedQueueTestJob(t, ctx, h, "job_f2a", "req_f2a", "ar_f2", "failed", now)
	seedQueueTestJob(t, ctx, h, "job_f2b", "req_f2b", "ar_f2", "queued", now)
	seedPendingAggregate(t, ctx, h, "ar_f1")

	var stats queueRetryStats
	if err := h.markFailedJobArtists(ctx, &stats); err != nil {
		t.Fatalf("markFailedJobArtists: %v", err)
	}
	if stats.FailedArtistsMarked != 1 {
		t.Fatalf("FailedArtistsMarked = %d, want 1", stats.FailedArtistsMarked)
	}
	if got := queryArtistFetchStatus(t, ctx, h, "ar_f1"); got != "failed" {
		t.Fatalf("ar_f1 fetch_status = %q, want failed", got)
	}
	if got := queryArtistFetchStatus(t, ctx, h, "ar_f2"); got != "pending" {
		t.Fatalf("ar_f2 fetch_status = %q, want pending", got)
	}
	if got := streamEventTypes(t, ctx, h, "ar_f1"); len(got) != 3 || got[2] != "ArtistFetchStatusChanged" {
		t.Fatalf("ar_f1 stream = %v", got)
	}
}

func TestIsArtistRefreshPending_AggregateState(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	// Unknown artist: fail open (transport dedup still guards).
	if dup, _ := h.isArtistRefreshPending(ctx, "ghost"); dup {
		t.Fatal("unknown artist should not read as pending")
	}

	agg, err := artist.NewArtist("ar_v1", "Validator", "sp_v1", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if _, err := h.artistRepo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if dup, _ := h.isArtistRefreshPending(ctx, "ar_v1"); dup {
		t.Fatal("idle artist should not read as pending")
	}

	loaded, err := h.artistRepo.Load(ctx, "ar_v1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := loaded.SetFetchStatus("pending", ""); err != nil {
		t.Fatalf("SetFetchStatus: %v", err)
	}
	if _, err := h.artistRepo.Save(ctx, loaded); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if dup, _ := h.isArtistRefreshPending(ctx, "ar_v1"); !dup {
		t.Fatal("pending artist should read as pending (restart-surviving duplicate detection)")
	}
}
