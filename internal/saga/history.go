// Package saga links one scrape request across its command, job-stream,
// and artist-stream facts by the saga instance key (request_id).
//
// Job streams are keyed by request_id already; artist facts now carry
// request_id in metadata (see eventsourcing.Correlation). Commands and
// scrape_jobs rows carry both IDs durably, so history survives restarts —
// unlike the in-memory correlation registry (5m TTL fast path only).
package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/commands"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
)

// History is one saga instance traced end to end.
type History struct {
	RequestID string
	ArtistID  string
	// State is the orchestrator's durable saga state for this request
	// (requested/processing/failed/done/dead). Empty when the orchestrator
	// has not observed the instance yet.
	State   string
	Command *commands.Command
	// JobEvents is the full request stream (Requested → Started × N →
	// Succeeded|Failed|DeadLettered). Never filtered: the stream IS the saga.
	JobEvents []eventsourcing.Event
	// ArtistEvents are the artist-stream facts caused by this request
	// (metadata request_id match). Empty for pre-correlation events.
	ArtistEvents []eventsourcing.Event
}

// Loader reads saga history from the durable stores. No new tables: commands
// + scrape_jobs rows plus the event log are the index.
type Loader struct {
	db    *toolbeltdb.Database
	store *eventsourcing.SQLiteStore
}

// NewLoader creates a Loader. Either may be nil (command lookup / event
// loading degrade gracefully to what is available).
func NewLoader(db *toolbeltdb.Database, store *eventsourcing.SQLiteStore) *Loader {
	if store == nil && db != nil {
		store = eventsourcing.NewSQLiteStore(db)
	}
	return &Loader{db: db, store: store}
}

// LoadByRequest traces one saga instance. Returns the job stream, the artist
// facts it caused, and its command row (when present).
func (l *Loader) LoadByRequest(ctx context.Context, requestID string) (*History, error) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return nil, fmt.Errorf("saga: request_id cannot be empty")
	}
	h := &History{RequestID: requestID}

	if l.store != nil {
		jobEvents, err := l.store.Load(ctx, requestID)
		if err != nil {
			return nil, fmt.Errorf("saga: load job stream %s: %w", requestID, err)
		}
		h.JobEvents = jobEvents
		if agg, err := scrapejob.Replay(requestID, jobEvents); err == nil {
			h.ArtistID = agg.ArtistID()
		}
	}

	if l.db != nil {
		if cmd, err := commandByRequestID(ctx, l.db, requestID); err == nil && cmd != nil {
			h.Command = cmd
			if h.ArtistID == "" {
				h.ArtistID = cmd.ArtistID
			}
		}
		if h.ArtistID == "" {
			h.ArtistID = scrapeJobArtist(ctx, l.db, requestID)
		}
	}

	if l.store != nil && h.ArtistID != "" {
		artistEvents, err := l.store.Load(ctx, h.ArtistID)
		if err == nil {
			h.ArtistEvents = filterByRequest(artistEvents, requestID)
		}
	}

	if l.db != nil {
		h.State = instanceState(ctx, l.db, requestID)
	}

	return h, nil
}

// RecentRequestIDs lists the latest saga instances for an artist, newest
// first, from the durable command log with scrape_jobs fallback. Bounded by
// limit (default 20, max 100).
func (l *Loader) RecentRequestIDs(ctx context.Context, artistID string, limit int) ([]string, error) {
	artistID = strings.TrimSpace(artistID)
	if artistID == "" {
		return nil, fmt.Errorf("saga: artist_id cannot be empty")
	}
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, 100)
	if l.db == nil {
		return nil, fmt.Errorf("saga: database not configured")
	}

	seen := make(map[string]struct{})
	var out []string
	_ = l.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		collect := func(query string, args ...string) {
			stmt := tx.Prep(query)
			defer func() { _ = stmt.Reset() }()
			for i, a := range args {
				stmt.BindText(i+1, a)
			}
			for {
				hasRow, err := stmt.Step()
				if err != nil || !hasRow {
					break
				}
				id := strings.TrimSpace(stmt.ColumnText(0))
				if id == "" {
					continue
				}
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		collect(`SELECT request_id FROM commands WHERE artist_id = ? ORDER BY queued_at DESC, request_id ASC LIMIT ?;`,
			artistID, strconv.Itoa(limit))
		if len(out) < limit {
			collect(`SELECT request_id FROM scrape_jobs WHERE artist_id = ? ORDER BY queued_at DESC, request_id ASC LIMIT ?;`,
				artistID, strconv.Itoa(limit))
		}
		return nil
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// filterByRequest returns events whose metadata request_id matches.
// Pre-correlation events (no metadata) never match — callers see only the
// facts their saga caused, which is the honest subset.
func filterByRequest(events []eventsourcing.Event, requestID string) []eventsourcing.Event {
	var out []eventsourcing.Event
	for _, evt := range events {
		meta, err := eventsourcing.DecodeMetadata(evt.Metadata)
		if err != nil || meta == nil {
			continue
		}
		if eventsourcing.RequestIDFromMetadata(meta) == requestID {
			out = append(out, evt)
		}
	}
	return out
}

// commandByRequestID fetches one command row by its idempotency key.
func commandByRequestID(ctx context.Context, db *toolbeltdb.Database, requestID string) (*commands.Command, error) {
	var cmd *commands.Command
	err := db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT request_id, command_type, artist_id, payload, queued_at FROM commands WHERE request_id = ? LIMIT 1;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		queuedAt, _ := time.Parse(time.RFC3339Nano, stmt.ColumnText(4))
		if queuedAt.IsZero() {
			queuedAt, _ = time.Parse("2006-01-02 15:04:05.000Z", stmt.ColumnText(4))
		}
		c := commands.Command{
			RequestID: stmt.ColumnText(0),
			Type:      stmt.ColumnText(1),
			ArtistID:  stmt.ColumnText(2),
			QueuedAt:  queuedAt,
		}
		payload := stmt.ColumnText(3)
		if payload != "" {
			var p struct {
				SpotifyID  string `json:"spotify_id"`
				ArtistName string `json:"artist_name"`
			}
			if err := json.Unmarshal([]byte(payload), &p); err == nil {
				c.SpotifyID = p.SpotifyID
				c.ArtistName = p.ArtistName
			}
		}
		cmd = &c
		return nil
	})
	return cmd, err
}

// instanceState reads the orchestrator state for a request. Fail-open: ""
// when the instance is unobserved (orchestrator not yet ticked) or on error.
func instanceState(ctx context.Context, db *toolbeltdb.Database, requestID string) string {
	var state string
	_ = db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT state FROM saga_instances WHERE request_id = ? LIMIT 1;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil || !hasRow {
			return nil
		}
		state = stmt.ColumnText(0)
		return nil
	})
	return state
}

// scrapeJobArtist resolves the artist for a request from the operational row.
func scrapeJobArtist(ctx context.Context, db *toolbeltdb.Database, requestID string) string {
	var artistID string
	_ = db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT artist_id FROM scrape_jobs WHERE request_id = ? LIMIT 1;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil || !hasRow {
			return nil
		}
		artistID = stmt.ColumnText(0)
		return nil
	})
	return artistID
}
