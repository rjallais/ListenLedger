package heal

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
	"ListenLedger/internal/reactor"
)

func setupHeal(t *testing.T) (*toolbeltdb.Database, *eventsourcing.SQLiteStore, *Healer, func()) {
	t.Helper()
	database, err := db.SetupDB(context.Background(), slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	h := NewHealer(slog.Default(), database, store,
		projections.NewArtistProjection(slog.Default(), database, nil),
		projections.NewCatalogProjection(slog.Default(), database),
	)
	return database, store, h, func() { _ = database.Close() }
}

func appendEvent(t *testing.T, ctx context.Context, store *eventsourcing.SQLiteStore, streamID string, version int64, evt eventsourcing.Event) {
	t.Helper()
	evt.Version = version
	if err := store.Append(ctx, streamID, version-1, evt); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestHealerRepairsLaggingReadModel(t *testing.T) {
	ctx := context.Background()
	database, store, h, cleanup := setupHeal(t)
	defer cleanup()

	artistID := "ar_heal"
	created, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, "ArtistCreated",
		map[string]any{"id": artistID, "name": "Heal Artist", "spotify_id": "sp_heal",
			"genre_group": "rock_metal", "list_status": "waiting"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	created.CreatedAt = time.Now().Add(-2 * healGrace)
	appendEvent(t, ctx, store, artistID, 1, created)

	// Position 0: nothing has been projected yet.
	if err := h.HandleEvent(ctx, created); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	var name string
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT name FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		name = stmt.ColumnText(0)
		return nil
	}); err != nil {
		t.Fatalf("read artists: %v", err)
	}
	if name != "Heal Artist" {
		t.Fatalf("artist read model = %q, want %q", name, "Heal Artist")
	}

	// A second identical delivery is a no-op (position already caught up).
	if err := h.HandleEvent(ctx, created); err != nil {
		t.Fatalf("second HandleEvent: %v", err)
	}
}

// TestHealerSkipsCaughtUpStreams asserts an event whose version is at or
// below the recorded position is acked without touching the read model.
func TestHealerSkipsCaughtUpStreams(t *testing.T) {
	ctx := context.Background()
	database, store, h, cleanup := setupHeal(t)
	defer cleanup()

	artistID := "ar_skip"
	created, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, "ArtistCreated",
		map[string]any{"id": artistID, "name": "Skip Artist", "spotify_id": "sp_skip",
			"genre_group": "everything_else", "list_status": "included"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	appendEvent(t, ctx, store, artistID, 1, created)

	// Pretend the read model already reflects v1 (inline path committed it).
	if err := recordPosition(ctx, t, database, artistID, artist.StreamTypeArtist, 1); err != nil {
		t.Fatalf("recordPosition: %v", err)
	}

	if err := h.HandleEvent(ctx, created); err != nil {
		t.Fatalf("HandleEvent for caught-up stream: %v", err)
	}

	var count int
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT count(*) FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			count = int(stmt.ColumnInt(0))
		}
		return nil
	}); err != nil {
		t.Fatalf("read artists: %v", err)
	}
	if count != 0 {
		t.Fatalf("caught-up stream was re-projected: %d rows, want 0", count)
	}
}

// TestHealerNaksYoungLagForRecheck asserts a lag younger than healGrace is
// NAKed untouched (the inline projection may still be in flight) with an
// error the reactor treats as retryable, and heals once the fact ages past
// the grace window.
func TestHealerNaksYoungLagForRecheck(t *testing.T) {
	ctx := context.Background()
	database, store, h, cleanup := setupHeal(t)
	defer cleanup()

	artistID := "ar_young"
	created, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, "ArtistCreated",
		map[string]any{"id": artistID, "name": "Young Artist", "spotify_id": "sp_young",
			"genre_group": "rock_metal", "list_status": "waiting"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	appendEvent(t, ctx, store, artistID, 1, created)

	rowCount := func() int {
		var count int
		if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT count(*) FROM artists WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, artistID)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				count = int(stmt.ColumnInt(0))
			}
			return nil
		}); err != nil {
			t.Fatalf("read artists: %v", err)
		}
		return count
	}

	// Fresh event: the inline projection may still be in flight, so NAK
	// without touching the read model; the reactor redelivers after its
	// delay (>= healGrace) for a re-check.
	err = h.HandleEvent(ctx, created)
	if err == nil {
		t.Fatal("HandleEvent for young lag: nil, want a NAK for re-check")
	}
	if errors.Is(err, reactor.ErrUnprocessable) {
		t.Fatalf("HandleEvent: %v is terminal, want a retryable NAK", err)
	}
	if !errors.Is(err, errYoungLag) {
		t.Fatalf("HandleEvent: %v, want errYoungLag", err)
	}
	if count := rowCount(); count != 0 {
		t.Fatalf("young lag was projected: %d rows, want 0", count)
	}

	// The same lag heals once the stored fact ages past the grace window:
	// in production redelivery carries the stored (now aged) timestamp.
	agedTime := time.Now().Add(-2 * healGrace).UTC()
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE events SET created_at = ? WHERE stream_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, agedTime.Format(time.RFC3339Nano))
		stmt.BindText(2, artistID)
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("aging the stored event: %v", err)
	}
	created.CreatedAt = agedTime
	if err := h.HandleEvent(ctx, created); err != nil {
		t.Fatalf("HandleEvent for aged lag: %v", err)
	}
	if count := rowCount(); count != 1 {
		t.Fatalf("aged lag was not healed: %d rows, want 1", count)
	}
}

// TestHealerUnknownStreamTerms asserts a fact this binary cannot read from
// the log is terminated rather than retried forever.
func TestHealerUnknownStreamTerms(t *testing.T) {
	ctx := context.Background()
	_, _, h, cleanup := setupHeal(t)
	defer cleanup()

	evt, err := eventsourcing.NewEvent("ar_missing", artist.StreamTypeArtist, 1, "ArtistCreated",
		map[string]any{}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	evt.CreatedAt = time.Now().Add(-2 * healGrace)
	if err := h.HandleEvent(ctx, evt); err != nil {
		if !errors.Is(err, reactor.ErrUnprocessable) {
			t.Fatalf("HandleEvent = %v, want ErrUnprocessable-wrapped", err)
		}
		return
	}
	t.Fatal("HandleEvent = nil, want ErrUnprocessable for a stream with no readable events")
}

// TestHealerTermsUnfoldableArtistStream asserts a deterministic fold failure
// (an event type this binary cannot apply) terminates the delivery instead
// of nak-looping the same replay on every redelivery.
func TestHealerTermsUnfoldableArtistStream(t *testing.T) {
	ctx := context.Background()
	_, store, h, cleanup := setupHeal(t)
	defer cleanup()

	artistID := "ar_poison"
	poison, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, "ArtistTurnedIntoANewt",
		map[string]any{"kind": "amphibian"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	poison.CreatedAt = time.Now().Add(-2 * healGrace)
	appendEvent(t, ctx, store, artistID, 1, poison)

	if err := h.HandleEvent(ctx, poison); !errors.Is(err, reactor.ErrUnprocessable) {
		t.Fatalf("HandleEvent = %v, want ErrUnprocessable-wrapped", err)
	}
}

// TestHealerTermsUnfoldableCatalogStreams asserts a deterministic fold
// failure on an album or song stream (an event type this binary cannot
// apply) terminates the delivery instead of nak-looping the same replay
// on every redelivery.
func TestHealerTermsUnfoldableCatalogStreams(t *testing.T) {
	ctx := context.Background()
	_, store, h, cleanup := setupHeal(t)
	defer cleanup()

	cases := []struct {
		streamID   string
		streamType string
	}{
		{"al_poison", album.StreamTypeAlbum},
		{"sg_poison", song.StreamTypeSong},
	}
	for _, c := range cases {
		poison, err := eventsourcing.NewEvent(c.streamID, c.streamType, 1, "CatalogTurnedIntoANewt",
			map[string]any{"kind": "amphibian"}, nil)
		if err != nil {
			t.Fatalf("NewEvent(%s): %v", c.streamType, err)
		}
		poison.CreatedAt = time.Now().Add(-2 * healGrace)
		appendEvent(t, ctx, store, c.streamID, 1, poison)

		if err := h.HandleEvent(ctx, poison); !errors.Is(err, reactor.ErrUnprocessable) {
			t.Fatalf("HandleEvent(%s) = %v, want ErrUnprocessable-wrapped", c.streamType, err)
		}
	}
}

// TestHealerHandlesAllStreamTypes asserts the healer routes each owned stream
// type to its projection without erroring on unsupported types.
func TestHealerHandlesAllStreamTypes(t *testing.T) {
	ctx := context.Background()
	database, store, h, cleanup := setupHeal(t)
	defer cleanup()

	cases := []struct {
		streamID   string
		streamType string
		eventType  string
		payload    map[string]any
		version    int64
	}{
		{"ar_all", artist.StreamTypeArtist, artist.EventTypeArtistCreated,
			map[string]any{"id": "ar_all", "name": "All", "spotify_id": "sp_all",
				"genre_group": "rock_metal", "list_status": "waiting"}, 1},
		{"al_all", album.StreamTypeAlbum, album.EventTypeAlbumCreated,
			map[string]any{"id": "al_all", "title": "All", "artist_name": "Artist",
				"status": "waiting", "collection_songs": 0, "total_songs": 0}, 1},
		{"sg_all", song.StreamTypeSong, song.EventTypeSongCreated,
			map[string]any{"id": "sg_all", "title": "All", "artist_name": "Artist",
				"album": "", "release_date": "", "release_year": 0, "release_type": "",
				"spotify_id": "sp_all", "is_recent": false, "recent_batch_seq": 0,
				"recent_batch_pos": 0}, 1},
	}
	for _, c := range cases {
		evt, err := eventsourcing.NewEvent(c.streamID, c.streamType, c.version, c.eventType, c.payload, nil)
		if err != nil {
			t.Fatalf("NewEvent(%s): %v", c.eventType, err)
		}
		evt.CreatedAt = time.Now().Add(-2 * healGrace)
		appendEvent(t, ctx, store, c.streamID, c.version, evt)
		if err := h.HandleEvent(ctx, evt); err != nil {
			t.Fatalf("HandleEvent(%s): %v", c.eventType, err)
		}
	}

	// scrapejob is intentionally not owned by this healer: ack without error.
	scrapeEvt, err := eventsourcing.NewEvent("req_all", scrapejobStreamType, 1, "ScrapeRequested",
		map[string]any{}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := h.HandleEvent(ctx, scrapeEvt); err != nil {
		t.Fatalf("HandleEvent(scrapejob): %v", err)
	}

	for _, table := range []struct {
		table string
		id    string
	}{
		{"artists", "ar_all"},
		{"albums", "al_all"},
		{"songs", "sg_all"},
	} {
		var count int
		if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT count(*) FROM " + table.table + " WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, table.id)
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if hasRow {
				count = int(stmt.ColumnInt(0))
			}
			return nil
		}); err != nil {
			t.Fatalf("read %s: %v", table.table, err)
		}
		if count != 1 {
			t.Fatalf("%s row for %s: count %d, want 1", table.table, table.id, count)
		}
	}
}

// TestHealerHealsAgedGapBehindYoungEvent asserts the grace ages the oldest
// unprojected fact, not the delivered event: a young v2 delivery must heal a
// stream whose unprojected v1 has aged past the grace window.
func TestHealerHealsAgedGapBehindYoungEvent(t *testing.T) {
	ctx := context.Background()
	database, store, h, cleanup := setupHeal(t)
	defer cleanup()

	artistID := "ar_gap"
	created, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, artist.EventTypeArtistCreated,
		map[string]any{"id": artistID, "name": "Gap Artist", "spotify_id": "sp_gap",
			"genre_group": "rock_metal", "list_status": "waiting"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	created.CreatedAt = time.Now().Add(-2 * healGrace)
	appendEvent(t, ctx, store, artistID, 1, created)

	changed, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 2, artist.EventTypeArtistStatusChanged,
		map[string]any{"artist_id": artistID, "old_status": "waiting", "new_status": "included"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	appendEvent(t, ctx, store, artistID, 2, changed)

	// Deliver the young v2 while position is still 0: the oldest unprojected
	// fact (v1) is aged, so the gap heals despite the fresh delivery.
	if err := h.HandleEvent(ctx, changed); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	var name, status string
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT name, list_status FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			name = stmt.ColumnText(0)
			status = stmt.ColumnText(1)
		}
		return nil
	}); err != nil {
		t.Fatalf("read artists: %v", err)
	}
	if name != "Gap Artist" || status != "included" {
		t.Fatalf("healed artist = (%q, %q), want (%q, %q): both facts projected",
			name, status, "Gap Artist", "included")
	}
}

func TestHealerFastPathYoungLagNaksThenHealsOnRedelivery(t *testing.T) {
	ctx := context.Background()
	database, store, h, cleanup := setupHeal(t)
	defer cleanup()

	artistID := "ar_fastpath"
	created, err := eventsourcing.NewEvent(artistID, artist.StreamTypeArtist, 1, "ArtistCreated",
		map[string]any{"id": artistID, "name": "FastPath Artist", "spotify_id": "sp_fp",
			"genre_group": "everything_else", "list_status": "included"}, nil)
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	appendEvent(t, ctx, store, artistID, 1, created)

	// Step 1: position is 0, delivered event is v1 (position+1 == evt.Version).
	// Because created.CreatedAt is young (just created), the fast path must return errYoungLag without loading the stream.
	err = h.HandleEvent(ctx, created)
	if !errors.Is(err, errYoungLag) {
		t.Fatalf("HandleEvent young fast-path: %v, want errYoungLag", err)
	}

	// Verify no artist row was projected yet.
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT count(*) FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow && stmt.ColumnInt(0) != 0 {
			t.Fatalf("artist row projected prematurely")
		}
		return nil
	}); err != nil {
		t.Fatalf("read artists: %v", err)
	}

	// Step 2: redelivery after grace period. In production, NATS redelivery delivers the same stored event,
	// whose timestamp has aged past healGrace.
	agedTime := time.Now().Add(-2 * healGrace).UTC()
	created.CreatedAt = agedTime
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE events SET created_at = ? WHERE stream_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, agedTime.Format(time.RFC3339Nano))
		stmt.BindText(2, artistID)
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("aging event: %v", err)
	}

	if err := h.HandleEvent(ctx, created); err != nil {
		t.Fatalf("HandleEvent on aged redelivery: %v", err)
	}

	// Verify artist row is now projected.
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT name FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow || stmt.ColumnText(0) != "FastPath Artist" {
			t.Fatalf("artist name = %q, want %q", stmt.ColumnText(0), "FastPath Artist")
		}
		return nil
	}); err != nil {
		t.Fatalf("read artists: %v", err)
	}
}

const scrapejobStreamType = "scrapejob"

func recordPosition(ctx context.Context, t *testing.T, database *toolbeltdb.Database, streamID, streamType string, version int64) error {
	t.Helper()
	return database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT INTO read_model_positions (stream_id, stream_type, version, updated_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(stream_id) DO UPDATE SET version = excluded.version;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, streamID)
		stmt.BindText(2, streamType)
		stmt.BindInt64(3, version)
		stmt.BindText(4, time.Now().UTC().Format(time.RFC3339Nano))
		_, err := stmt.Step()
		return err
	})
}
