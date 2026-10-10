package commands

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/messaging"
)

func mustExec(t *testing.T, ctx context.Context, sqliteDB *toolbeltdb.Database, query string, args ...string) {
	t.Helper()
	err := sqliteDB.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		for i, a := range args {
			stmt.BindText(i+1, a)
		}
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("mustExec: %v", err)
	}
}

func TestLog_Idempotent(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = sqliteDB.Close() }()

	cmd := Command{RequestID: "req_1", Type: TypeRefresh, ArtistID: "ar_1", SpotifyID: "sp_1", ArtistName: "Nirvana"}
	if err := Log(ctx, sqliteDB, cmd); err != nil {
		t.Fatalf("Log: %v", err)
	}
	// Same request_id twice: second is a no-op, never an error or duplicate row.
	if err := Log(ctx, sqliteDB, cmd); err != nil {
		t.Fatalf("Log(second): %v", err)
	}
	got, err := List(ctx, sqliteDB, Filter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("List len = %d, want 1", len(got))
	}
	if got[0].SpotifyID != "sp_1" || got[0].ArtistName != "Nirvana" {
		t.Fatalf("payload roundtrip = %+v", got[0])
	}

	if err := Log(ctx, sqliteDB, Command{}); err == nil {
		t.Fatal("Log(empty) should error")
	}
	if err := Log(ctx, sqliteDB, Command{RequestID: "x", ArtistID: "y", Type: "nope"}); err == nil {
		t.Fatal("Log(bad type) should error")
	}
}

func TestList_FiltersAndUnresolved(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = sqliteDB.Close() }()

	seed := []Command{
		{RequestID: "req_a1", Type: TypeRefresh, ArtistID: "ar_1", SpotifyID: "sp_1", ArtistName: "A"},
		{RequestID: "req_a2", Type: TypeBatch, ArtistID: "ar_1", SpotifyID: "sp_1", ArtistName: "A"},
		{RequestID: "req_b1", Type: TypeRefresh, ArtistID: "ar_2", SpotifyID: "sp_2", ArtistName: "B"},
	}
	for _, c := range seed {
		if err := Log(ctx, sqliteDB, c); err != nil {
			t.Fatalf("Log: %v", err)
		}
	}
	// scrape_jobs: req_a1 succeeded, req_a2 failed, req_b1 missing entirely.
	mustExec(t, ctx, sqliteDB, `INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated, created_at)
		VALUES ('ar_1','A','sp_1',0,'rock_metal','included','idle',0,0,?,?),
		       ('ar_2','B','sp_2',0,'rock_metal','included','idle',0,0,?,?);`,
		time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	mustExec(t, ctx, sqliteDB, `INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at)
		VALUES ('job_a1','req_a1','ar_1','succeeded',1,'',?), ('job_a2','req_a2','ar_1','failed',3,'boom',?);`,
		time.Now().UTC().Format("2006-01-02 15:04:05.000Z"), time.Now().UTC().Format("2006-01-02 15:04:05.000Z"))

	byArtist, err := List(ctx, sqliteDB, Filter{ArtistID: "ar_1"})
	if err != nil || len(byArtist) != 2 {
		t.Fatalf("List(artist) = %d, %v; want 2", len(byArtist), err)
	}
	byType, err := List(ctx, sqliteDB, Filter{Type: TypeBatch})
	if err != nil || len(byType) != 1 || byType[0].RequestID != "req_a2" {
		t.Fatalf("List(type=batch) = %+v, %v", byType, err)
	}
	unresolved, err := List(ctx, sqliteDB, Filter{UnresolvedOnly: true})
	if err != nil {
		t.Fatalf("List(unresolved): %v", err)
	}
	// req_a2 failed + req_b1 has no job row; req_a1 succeeded is excluded.
	want := map[string]bool{"req_a2": true, "req_b1": true}
	if len(unresolved) != 2 {
		t.Fatalf("List(unresolved) len = %d, want 2 (%v)", len(unresolved), unresolved)
	}
	for _, c := range unresolved {
		if !want[c.RequestID] {
			t.Fatalf("unexpected unresolved command %s", c.RequestID)
		}
	}
}

func TestRedrive_PublishesAndDryRun(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = sqliteDB.Close() }()

	for _, c := range []Command{
		{RequestID: "req_1", Type: TypeRefresh, ArtistID: "ar_1", SpotifyID: "sp_1", ArtistName: "A"},
		{RequestID: "req_2", Type: TypeBatch, ArtistID: "ar_2", SpotifyID: "sp_2", ArtistName: "B"},
	} {
		if err := Log(ctx, sqliteDB, c); err != nil {
			t.Fatalf("Log: %v", err)
		}
	}

	var published []messaging.ScrapeRequested
	fake := func(ctx context.Context, req messaging.ScrapeRequested) error {
		if req.RequestID == "req_2" {
			return fmt.Errorf("nats down")
		}
		published = append(published, req)
		return nil
	}

	stats, err := Redrive(ctx, sqliteDB, fake, Filter{}, true)
	if err != nil {
		t.Fatalf("Redrive(dry): %v", err)
	}
	if stats.Total != 2 || stats.Published != 0 || !stats.DryRun {
		t.Fatalf("dry stats = %+v, want total=2 published=0 dry=true", stats)
	}
	if len(published) != 0 {
		t.Fatalf("dry run published %d requests", len(published))
	}

	stats, err = Redrive(ctx, sqliteDB, fake, Filter{}, false)
	if err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	if stats.Total != 2 || stats.Published != 1 || stats.Failed != 1 {
		t.Fatalf("stats = %+v, want total=2 published=1 failed=1", stats)
	}
	if len(published) != 1 || published[0].SpotifyID != "sp_1" {
		t.Fatalf("published = %+v", published)
	}
	// Log is untouched by redrive (re-execution, never new rows).
	got, err := List(ctx, sqliteDB, Filter{})
	if err != nil {
		t.Fatalf("List after redrive: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List after redrive len = %d, want 2", len(got))
	}
}
