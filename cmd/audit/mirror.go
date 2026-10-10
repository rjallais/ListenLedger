package main

import (
	"context"
	"fmt"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"
)

// pbMirrorReport tallies PocketBase vs SQLite read-model convergence for one
// collection. Both sides derive from the same event log; any gap is mirror
// drift (PB-only writes, lost projections, or in-flight rows on a live DB).
type pbMirrorReport struct {
	PBTotal     int      `json:"pb_total"`
	SQLiteTotal int      `json:"sqlite_total"`
	Mismatched  int      `json:"mismatched"`
	PBOnly      int      `json:"pb_only"`
	SQLiteOnly  int      `json:"sqlite_only"`
	Samples     []string `json:"samples,omitempty"`
}

func (r *pbMirrorReport) addSample(cap int, sample string) {
	if len(r.Samples) < cap {
		r.Samples = append(r.Samples, sample)
	}
}

func (r *pbMirrorReport) hasMismatch() bool {
	return r.Mismatched > 0 || r.PBOnly > 0 || r.SQLiteOnly > 0
}

// mirrorRow is one side of a PB/SQLite comparison, keyed by record ID.
type mirrorRow struct {
	id     string
	fields map[string]any
}

// compareMirrors diffs PB rows against SQLite rows on the given fields.
// Missing counterparts and unequal fields are reported; ordering is by PB id.
func compareMirrors(label string, fields []string, pb, lite []mirrorRow, maxSamples int) *pbMirrorReport {
	rep := &pbMirrorReport{PBTotal: len(pb), SQLiteTotal: len(lite)}
	liteByID := make(map[string]mirrorRow, len(lite))
	for _, row := range lite {
		liteByID[row.id] = row
	}
	seen := make(map[string]struct{}, len(pb))
	for _, prow := range pb {
		seen[prow.id] = struct{}{}
		lrow, ok := liteByID[prow.id]
		if !ok {
			rep.PBOnly++
			rep.addSample(maxSamples, fmt.Sprintf("%s %s: PocketBase row has no SQLite row (projection never ran)", label, prow.id))
			continue
		}
		for _, field := range fields {
			if fmt.Sprintf("%v", prow.fields[field]) != fmt.Sprintf("%v", lrow.fields[field]) {
				rep.Mismatched++
				rep.addSample(maxSamples, fmt.Sprintf("%s %s: pb.%s=%v != sqlite.%s=%v",
					label, prow.id, field, prow.fields[field], field, lrow.fields[field]))
				break
			}
		}
	}
	for _, lrow := range lite {
		if _, ok := seen[lrow.id]; !ok {
			rep.SQLiteOnly++
			rep.addSample(maxSamples, fmt.Sprintf("%s %s: SQLite row has no PocketBase row", label, lrow.id))
		}
	}
	return rep
}

func fetchPBRecords(ctx context.Context, app *pocketbase.PocketBase, collection string, fields []string) ([]mirrorRow, error) {
	records := make([]*core.Record, 0, 512)
	if err := app.RecordQuery(collection).WithContext(ctx).All(&records); err != nil {
		return nil, fmt.Errorf("querying PocketBase %s: %w", collection, err)
	}
	rows := make([]mirrorRow, 0, len(records))
	for _, rec := range records {
		mapped := make(map[string]any, len(fields))
		for _, field := range fields {
			mapped[field] = pbFieldValue(rec, field)
		}
		rows = append(rows, mirrorRow{id: rec.Id, fields: mapped})
	}
	return rows, nil
}

// pbFieldValue reads a record field in SQLite-comparable form: integers as
// int64 (matching ColumnInt64), booleans as 0/1, everything else as string.
func pbFieldValue(rec *core.Record, field string) any {
	switch field {
	case "is_recent":
		if rec.GetBool(field) {
			return int64(1)
		}
		return int64(0)
	case "monthly_listeners", "collection_songs", "total_songs":
		return rec.GetInt64(field)
	default:
		return rec.GetString(field)
	}
}

func fetchSQLiteArtists(ctx context.Context, database *toolbeltdb.Database) ([]mirrorRow, error) {
	return fetchSQLiteRows(ctx, database,
		"SELECT id, monthly_listeners, genre_group, list_status, fetch_status FROM artists;",
		[]string{"monthly_listeners", "genre_group", "list_status", "fetch_status"})
}

func fetchSQLiteAlbums(ctx context.Context, database *toolbeltdb.Database) ([]mirrorRow, error) {
	return fetchSQLiteRows(ctx, database,
		"SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums;",
		[]string{"title", "artist_name", "collection_songs", "total_songs", "status"})
}

func fetchSQLiteSongs(ctx context.Context, database *toolbeltdb.Database) ([]mirrorRow, error) {
	return fetchSQLiteRows(ctx, database,
		"SELECT id, title, artist_name, is_recent FROM songs;",
		[]string{"title", "artist_name", "is_recent"})
}

func fetchSQLiteRows(ctx context.Context, database *toolbeltdb.Database, query string, fields []string) ([]mirrorRow, error) {
	var rows []mirrorRow
	err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return fmt.Errorf("scanning read-model rows: %w", err)
			}
			if !hasRow {
				return nil
			}
			mapped := make(map[string]any, len(fields))
			id := stmt.ColumnText(0)
			for i, field := range fields {
				if stmt.ColumnType(i+1) == sqlite.TypeInteger {
					mapped[field] = stmt.ColumnInt64(i + 1)
				} else {
					mapped[field] = stmt.ColumnText(i + 1)
				}
			}
			rows = append(rows, mirrorRow{id: id, fields: mapped})
		}
	})
	if err != nil {
		return nil, fmt.Errorf("reading read-model rows: %w", err)
	}
	return rows, nil
}

// auditPBMirror compares PocketBase collections against their SQLite
// projections. Run quiescent: live event-first writes mean PB briefly leads
// SQLite, which reads as a false mismatch under load.
func auditPBMirror(ctx context.Context, app *pocketbase.PocketBase, database *toolbeltdb.Database, maxSamples int) (map[string]*pbMirrorReport, error) {
	out := make(map[string]*pbMirrorReport)

	// total_songs rank is derived (PB recalc cache), not a fact: the SQLite
	// projection keeps stream truth, so rank drift is expected noise.
	// Artist comparison covers counts and identity, not the rank column.
	artistFields := []string{"monthly_listeners", "genre_group", "list_status", "fetch_status"}
	pbArtists, err := fetchPBRecords(ctx, app, "artists", artistFields)
	if err != nil {
		return nil, fmt.Errorf("fetching PocketBase artists: %w", err)
	}
	liteArtists, err := fetchSQLiteArtists(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("fetching SQLite artists: %w", err)
	}
	out["artists"] = compareMirrors("artist", artistFields, pbArtists, liteArtists, maxSamples)

	albumFields := []string{"title", "artist_name", "collection_songs", "total_songs", "status"}
	pbAlbums, err := fetchPBRecords(ctx, app, "albums", albumFields)
	if err != nil {
		return nil, fmt.Errorf("fetching PocketBase albums: %w", err)
	}
	liteAlbums, err := fetchSQLiteAlbums(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("fetching SQLite albums: %w", err)
	}
	out["albums"] = compareMirrors("album", albumFields, pbAlbums, liteAlbums, maxSamples)

	songFields := []string{"title", "artist_name", "is_recent"}
	pbSongs, err := fetchPBRecords(ctx, app, "songs", songFields)
	if err != nil {
		return nil, fmt.Errorf("fetching PocketBase songs: %w", err)
	}
	liteSongs, err := fetchSQLiteSongs(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("fetching SQLite songs: %w", err)
	}
	out["songs"] = compareMirrors("song", songFields, pbSongs, liteSongs, maxSamples)

	return out, nil
}
