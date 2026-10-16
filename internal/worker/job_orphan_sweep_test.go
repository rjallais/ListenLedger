package worker

import (
	"context"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
)

// TestOrphanedQueuedSweep covers the never-dispatched crash window: a queued
// row with no job stream is swept to failed with a ScrapeFailed fact and its
// artist leaves pending, while dispatched (stream exists) and young rows stay
// untouched.
func TestOrphanedQueuedSweep(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	now := time.Now().UTC()
	cutoff := now.Add(-orphanedQueuedGrace).Format(jobTimestampFormat)
	old := now.Add(-time.Hour).Format(jobTimestampFormat)
	fresh := now.Format(jobTimestampFormat)

	seedSweepArtist(t, ctx, w, "ar_orphan", "pending")
	seedSweepArtist(t, ctx, w, "ar_dispatched", "pending")
	seedSweepArtist(t, ctx, w, "ar_young", "pending")
	seedSweepQueuedRow(t, ctx, w, "req_orphan", "ar_orphan", old)
	seedSweepQueuedRow(t, ctx, w, "req_dispatched", "ar_dispatched", old)
	seedSweepQueuedRow(t, ctx, w, "req_young", "ar_young", fresh)

	w.recordJobEventWarn(ctx, "req_dispatched", "ar_dispatched", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})

	w.markOrphanedQueuedJobs(ctx, cutoff)

	status, errText := sweepJobRow(t, ctx, w, "req_orphan")
	if status != "failed" || errText != orphanedQueuedReason {
		t.Fatalf("orphan row = %q/%q, want failed/%q", status, errText, orphanedQueuedReason)
	}
	types := jobStreamTypes(t, ctx, w, "req_orphan")
	if len(types) != 2 || types[0] != scrapejob.EventTypeScrapeRequested || types[1] != scrapejob.EventTypeScrapeFailed {
		t.Fatalf("orphan stream = %v, want [ScrapeRequested ScrapeFailed]", types)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_orphan"); got != "failed" {
		t.Fatalf("orphan artist fetch_status = %q, want failed", got)
	}

	if status, _ := sweepJobRow(t, ctx, w, "req_dispatched"); status != "queued" {
		t.Fatalf("dispatched row = %q, want queued", status)
	}
	if status, _ := sweepJobRow(t, ctx, w, "req_young"); status != "queued" {
		t.Fatalf("young row = %q, want queued", status)
	}

	// Second sweep over the same cutoff is a no-op: the row already left
	// queued and no second verdict is appended.
	w.markOrphanedQueuedJobs(ctx, cutoff)
	if types := jobStreamTypes(t, ctx, w, "req_orphan"); len(types) != 2 {
		t.Fatalf("orphan stream after second sweep = %v, want unchanged", types)
	}
}

func seedSweepArtist(t *testing.T, ctx context.Context, w *Worker, artistID, fetchStatus string) {
	t.Helper()
	err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT OR IGNORE INTO artists (id, name, genre_group, list_status, fetch_status) VALUES (?, ?, 'rock_metal', 'included', ?);")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		stmt.BindText(2, artistID)
		stmt.BindText(3, fetchStatus)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("seed artist %s: %v", artistID, err)
	}
}

func seedSweepQueuedRow(t *testing.T, ctx context.Context, w *Worker, requestID, artistID, queuedAt string) {
	t.Helper()
	err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at) VALUES (?, ?, ?, 'queued', 0, '', ?);")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		stmt.BindText(2, requestID)
		stmt.BindText(3, artistID)
		stmt.BindText(4, queuedAt)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("seed scrape_jobs %s: %v", requestID, err)
	}
}

func sweepJobRow(t *testing.T, ctx context.Context, w *Worker, requestID string) (string, string) {
	t.Helper()
	var status, errText string
	err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status, error FROM scrape_jobs WHERE request_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			status = stmt.ColumnText(0)
			errText = stmt.ColumnText(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read scrape_jobs %s: %v", requestID, err)
	}
	return status, errText
}

func sweepArtistFetchStatus(t *testing.T, ctx context.Context, w *Worker, artistID string) string {
	t.Helper()
	var status string
	err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
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
		t.Fatalf("read artist %s: %v", artistID, err)
	}
	return status
}
