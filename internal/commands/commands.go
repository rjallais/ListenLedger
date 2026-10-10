// Package commands implements command sourcing: a durable, append-only log
// of accepted scrape commands for reproducible command history.
//
// Model: queue points (single refresh, batch refresh) Log one row per
// request_id. Retries and redrives are re-executions of the stored command,
// never new rows — the worker treats request_id as the idempotency key.
// Operational lifecycle stays in scrape_jobs; this package answers "what was
// commanded" and replays it via Redrive (pure republish; already-succeeded
// dispatches ack-skip by design).
package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/messaging"
)

// Command types for the log. Retries/redrives reuse the original row.
const (
	TypeRefresh    = "refresh"
	TypeBatch      = "batch"
	TypeBackfilled = "backfilled"
)

// timestampFormat is the fixed-width UTC layout for queued_at writes and
// Since/Until bounds. Fixed width keeps SQLite lexicographic comparison
// chronological; RFC3339Nano's variable fraction would misorder rows within
// the same second. Reads still parse RFC3339Nano as a fallback.
const timestampFormat = "2006-01-02 15:04:05.000Z"

// Command is one accepted scrape command.
type Command struct {
	RequestID  string    `json:"request_id"`
	Type       string    `json:"command_type"`
	ArtistID   string    `json:"artist_id"`
	SpotifyID  string    `json:"spotify_id"`
	ArtistName string    `json:"artist_name"`
	QueuedAt   time.Time `json:"queued_at"`
}

// payload is the JSON body stored in commands.payload.
type payload struct {
	SpotifyID  string `json:"spotify_id"`
	ArtistName string `json:"artist_name"`
}

// Log appends a command idempotently (INSERT OR IGNORE on request_id).
// Callers log only accepted commands: after publish success, never on the
// duplicate-ack path (the original dispatch already logged it).
func Log(ctx context.Context, db *toolbeltdb.Database, cmd Command) error {
	if strings.TrimSpace(cmd.RequestID) == "" {
		return fmt.Errorf("command request_id cannot be empty")
	}
	if strings.TrimSpace(cmd.ArtistID) == "" {
		return fmt.Errorf("command artist_id cannot be empty")
	}
	switch cmd.Type {
	case TypeRefresh, TypeBatch, TypeBackfilled:
	default:
		return fmt.Errorf("unknown command type %q", cmd.Type)
	}

	body, err := json.Marshal(payload{SpotifyID: cmd.SpotifyID, ArtistName: cmd.ArtistName})
	if err != nil {
		return fmt.Errorf("marshaling command payload: %w", err)
	}
	queuedAt := cmd.QueuedAt
	if queuedAt.IsZero() {
		queuedAt = time.Now().UTC()
	}

	return db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`INSERT OR IGNORE INTO commands (request_id, command_type, artist_id, payload, queued_at)
			VALUES (?, ?, ?, ?, ?);`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, cmd.RequestID)
		stmt.BindText(2, cmd.Type)
		stmt.BindText(3, cmd.ArtistID)
		stmt.BindText(4, string(body))
		stmt.BindText(5, queuedAt.UTC().Format(timestampFormat))
		_, err := stmt.Step()
		return err
	})
}

// Filter scopes List and Redrive.
type Filter struct {
	ArtistID string
	Type     string
	Since    time.Time
	Until    time.Time
	// UnresolvedOnly returns commands whose scrape_jobs row is failed or
	// missing entirely — the honest retry set. (Succeeded/processing rows are
	// either done or live; republishing them would ack-skip anyway.)
	UnresolvedOnly bool
	Limit          int
}

// List returns commands newest-first.
func List(ctx context.Context, db *toolbeltdb.Database, f Filter) ([]Command, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}

	var conds []string
	var args []string
	if strings.TrimSpace(f.ArtistID) != "" {
		conds = append(conds, "c.artist_id = ?")
		args = append(args, strings.TrimSpace(f.ArtistID))
	}
	if strings.TrimSpace(f.Type) != "" {
		conds = append(conds, "c.command_type = ?")
		args = append(args, strings.TrimSpace(f.Type))
	}
	if !f.Since.IsZero() {
		conds = append(conds, "c.queued_at >= ?")
		args = append(args, f.Since.UTC().Format(timestampFormat))
	}
	if !f.Until.IsZero() {
		conds = append(conds, "c.queued_at <= ?")
		args = append(args, f.Until.UTC().Format(timestampFormat))
	}

	query := `SELECT c.request_id, c.command_type, c.artist_id, c.payload, c.queued_at FROM commands c`
	if f.UnresolvedOnly {
		query += ` LEFT JOIN scrape_jobs j ON j.request_id = c.request_id
			WHERE (j.request_id IS NULL OR j.status = 'failed')`
		if len(conds) > 0 {
			query += " AND " + strings.Join(conds, " AND ")
		}
	} else if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	query += " ORDER BY c.queued_at DESC, c.request_id ASC LIMIT ?;"

	var out []Command
	err := db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		for i, a := range args {
			stmt.BindText(i+1, a)
		}
		stmt.BindInt64(len(args)+1, int64(limit))
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			var p payload
			if err := json.Unmarshal([]byte(stmt.ColumnText(3)), &p); err != nil {
				return fmt.Errorf("decoding command payload: %w", err)
			}
			queuedAt, _ := time.Parse(timestampFormat, stmt.ColumnText(4))
			if queuedAt.IsZero() {
				queuedAt, _ = time.Parse(time.RFC3339Nano, stmt.ColumnText(4))
			}
			out = append(out, Command{
				RequestID:  stmt.ColumnText(0),
				Type:       stmt.ColumnText(1),
				ArtistID:   stmt.ColumnText(2),
				SpotifyID:  p.SpotifyID,
				ArtistName: p.ArtistName,
				QueuedAt:   queuedAt,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing commands: %w", err)
	}
	return out, nil
}

// PublishFunc republishes one scrape request. Injected so Redrive stays free
// of NATS wiring and tests can fake it.
type PublishFunc func(ctx context.Context, req messaging.ScrapeRequested) error

// RedriveStats summarizes a redrive pass.
type RedriveStats struct {
	Total     int `json:"total"`
	Published int `json:"published"`
	Failed    int `json:"failed"`
	DryRun    bool `json:"dry_run"`
}

// Redrive republishes logged commands (pure republish, no state mutation).
// Already-succeeded dispatches ack-skip in the worker by request_id design;
// use UnresolvedOnly to target commands that never completed.
func Redrive(ctx context.Context, db *toolbeltdb.Database, publish PublishFunc, f Filter, dryRun bool) (RedriveStats, error) {
	cmds, err := List(ctx, db, f)
	if err != nil {
		return RedriveStats{}, err
	}
	stats := RedriveStats{Total: len(cmds), DryRun: dryRun}
	for _, cmd := range cmds {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if dryRun {
			continue
		}
		req := messaging.NewScrapeRequested(cmd.ArtistID, cmd.SpotifyID, cmd.ArtistName, cmd.RequestID)
		if err := publish(ctx, req); err != nil {
			stats.Failed++
			continue
		}
		stats.Published++
	}
	return stats, nil
}
