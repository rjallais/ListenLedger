package main

import (
	"context"
	"log/slog"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
)

func mirrorRowFor(id string, fields map[string]any) mirrorRow {
	return mirrorRow{id: id, fields: fields}
}

func TestCompareMirrorsClean(t *testing.T) {
	pb := []mirrorRow{
		mirrorRowFor("a1", map[string]any{"monthly_listeners": int64(100), "fetch_status": "idle"}),
	}
	lite := []mirrorRow{
		mirrorRowFor("a1", map[string]any{"monthly_listeners": int64(100), "fetch_status": "idle"}),
	}
	rep := compareMirrors("artist", []string{"monthly_listeners", "fetch_status"}, pb, lite, 20)
	if rep.hasMismatch() {
		t.Fatalf("clean mirrors should not mismatch: %+v", rep)
	}
	if rep.PBTotal != 1 || rep.SQLiteTotal != 1 {
		t.Fatalf("totals = %d/%d, want 1/1", rep.PBTotal, rep.SQLiteTotal)
	}
}

func TestCompareMirrorsFindsDrift(t *testing.T) {
	pb := []mirrorRow{
		mirrorRowFor("a1", map[string]any{"monthly_listeners": int64(200), "fetch_status": "idle"}),
		mirrorRowFor("a2", map[string]any{"monthly_listeners": int64(50), "fetch_status": "pending"}),
		mirrorRowFor("a3", map[string]any{"monthly_listeners": int64(10), "fetch_status": "idle"}),
	}
	lite := []mirrorRow{
		mirrorRowFor("a1", map[string]any{"monthly_listeners": int64(100), "fetch_status": "idle"}),
		mirrorRowFor("a2", map[string]any{"monthly_listeners": int64(50), "fetch_status": "pending"}),
		mirrorRowFor("a4", map[string]any{"monthly_listeners": int64(5), "fetch_status": "idle"}),
	}
	rep := compareMirrors("artist", []string{"monthly_listeners", "fetch_status"}, pb, lite, 20)
	if rep.Mismatched != 1 {
		t.Fatalf("mismatched = %d, want 1 (a1 listeners)", rep.Mismatched)
	}
	if rep.PBOnly != 1 {
		t.Fatalf("pbOnly = %d, want 1 (a3)", rep.PBOnly)
	}
	if rep.SQLiteOnly != 1 {
		t.Fatalf("sqliteOnly = %d, want 1 (a4)", rep.SQLiteOnly)
	}
	if !rep.hasMismatch() {
		t.Fatal("hasMismatch should be true")
	}
	// Only the first differing field is sampled per row.
	if len(rep.Samples) != 3 {
		t.Fatalf("samples = %d, want 3", len(rep.Samples))
	}
}

func TestFetchSQLiteSongsLoadsTidalID(t *testing.T) {
	ctx := context.Background()
	database, err := db.SetupDB(ctx, slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO songs (id, title, artist_name, tidal_id) VALUES ('s1', 'One', 'Artist', 'td_s1');")
		defer func() { _ = stmt.Reset() }()
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("insert song: %v", err)
	}

	rows, err := fetchSQLiteSongs(ctx, database)
	if err != nil {
		t.Fatalf("fetchSQLiteSongs: %v", err)
	}
	if len(rows) != 1 || rows[0].id != "s1" {
		t.Fatalf("rows = %+v, want one row for s1", rows)
	}
	if rows[0].fields["tidal_id"] != "td_s1" {
		t.Fatalf("tidal_id = %v, want td_s1", rows[0].fields["tidal_id"])
	}

	fields := []string{"title", "artist_name", "is_recent", "album", "release_date", "release_year", "release_type", "spotify_id", "tidal_id", "duplicate_of"}
	pb := []mirrorRow{mirrorRowFor("s1", map[string]any{
		"title": "One", "artist_name": "Artist", "is_recent": int64(0), "album": "",
		"release_date": "", "release_year": int64(0), "release_type": "", "spotify_id": "",
		"tidal_id": "td_s1", "duplicate_of": "",
	})}
	if rep := compareMirrors("song", fields, pb, rows, 20); rep.hasMismatch() {
		t.Fatalf("matching mirrors should not mismatch: %+v", rep)
	}

	pb[0].fields["tidal_id"] = "td_drift"
	if rep := compareMirrors("song", fields, pb, rows, 20); rep.Mismatched != 1 {
		t.Fatalf("mismatched = %d, want 1 (tidal_id drift)", rep.Mismatched)
	}
}

func TestCompareMirrorsSampleCap(t *testing.T) {
	pb := []mirrorRow{
		mirrorRowFor("a1", map[string]any{"x": "pb"}),
		mirrorRowFor("a2", map[string]any{"x": "pb"}),
		mirrorRowFor("a3", map[string]any{"x": "pb"}),
	}
	lite := []mirrorRow{
		mirrorRowFor("a1", map[string]any{"x": "lite"}),
		mirrorRowFor("a2", map[string]any{"x": "lite"}),
		mirrorRowFor("a3", map[string]any{"x": "lite"}),
	}
	rep := compareMirrors("thing", []string{"x"}, pb, lite, 2)
	if rep.Mismatched != 3 {
		t.Fatalf("mismatched = %d, want 3", rep.Mismatched)
	}
	if len(rep.Samples) != 2 {
		t.Fatalf("samples = %d, want capped at 2", len(rep.Samples))
	}
}

func TestCompareMirrorsSongsWithTidalFields(t *testing.T) {
	fields := []string{"title", "artist_name", "is_recent", "album", "release_date", "release_year", "release_type", "spotify_id", "tidal_id", "duplicate_of"}
	pb := []mirrorRow{
		mirrorRowFor("s1", map[string]any{
			"title": "Master of Puppets", "artist_name": "Metallica", "is_recent": int64(0),
			"album": "Master of Puppets", "release_date": "1986-03-03", "release_year": int64(1986),
			"release_type": "album", "spotify_id": "sp_s1", "tidal_id": "td_s1", "duplicate_of": "",
		}),
		mirrorRowFor("s2", map[string]any{
			"title": "Battery", "artist_name": "Metallica", "is_recent": int64(1),
			"album": "Master of Puppets", "release_date": "1986-03-03", "release_year": int64(1986),
			"release_type": "album", "spotify_id": "sp_s2", "tidal_id": "td_s2_pb", "duplicate_of": "",
		}),
	}
	lite := []mirrorRow{
		mirrorRowFor("s1", map[string]any{
			"title": "Master of Puppets", "artist_name": "Metallica", "is_recent": int64(0),
			"album": "Master of Puppets", "release_date": "1986-03-03", "release_year": int64(1986),
			"release_type": "album", "spotify_id": "sp_s1", "tidal_id": "td_s1", "duplicate_of": "",
		}),
		mirrorRowFor("s2", map[string]any{
			"title": "Battery", "artist_name": "Metallica", "is_recent": int64(1),
			"album": "Master of Puppets", "release_date": "1986-03-03", "release_year": int64(1986),
			"release_type": "album", "spotify_id": "sp_s2", "tidal_id": "td_s2_sqlite", "duplicate_of": "",
		}),
	}
	rep := compareMirrors("song", fields, pb, lite, 20)
	if !rep.hasMismatch() {
		t.Fatal("hasMismatch should be true because s2 tidal_id differs")
	}
	if rep.Mismatched != 1 {
		t.Fatalf("mismatched = %d, want 1", rep.Mismatched)
	}
}
