package eventsourcing

import (
	"context"
	"errors"
	"fmt"
)

// appendRetryAttempts bounds tail-retry loops. Each attempt reloads, so this
// only trips under sustained contention; normal partial writes converge on
// the second attempt.
const appendRetryAttempts = 5

// AppendWithRetry appends events, resuming only the unwritten tail when a
// mid-batch failure leaves a prefix persisted. It bridges the atomicity gap
// between SQLiteStore (one transaction: all-or-nothing) and JetStreamStore
// (per-event publishes). After a conflict — or an indeterminate publish
// failure (timeout / lost ack, where a prefix may have persisted) — it
// reloads the stream and retries only the unwritten tail, verified by event
// ID — never by version alone, so a foreign writer's events can never be
// silently dropped from the batch.
// Success means every event is in the stream (verified by ID on the final
// attempt); ErrConcurrencyConflict means the batch no longer fits the tip
// and the caller must rebuild it from a fresh Load. Other errors
// (validation, type mismatch, I/O) return immediately — retrying those is
// pointless. Bounded attempts guarantee termination.
func AppendWithRetry(ctx context.Context, store Store, streamID string, expectedVersion int64, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	base, remaining := expectedVersion, events
	var lastErr error
	for attempt := 0; attempt < appendRetryAttempts; attempt++ {
		err := store.Append(ctx, streamID, base, remaining...)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConcurrencyConflict) && !errors.Is(err, ErrAppendIndeterminate) {
			return err
		}
		lastErr = err
		// Dead context: reconciliation Load and further attempts cannot
		// succeed — return early, preserving the outcome signal.
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("reconciling %s after %w: %w", streamID, lastErr, cerr)
		}
		loaded, err := store.Load(ctx, streamID)
		if err != nil {
			// Preserve the publish outcome: a timeout or lost ack may have
			// persisted a prefix that the failed Load cannot rule out.
			// Multi-%w keeps every signal detectable (conflict for
			// version races, indeterminate for unknown outcomes, plus the
			// load I/O error itself).
			return fmt.Errorf("reconciling %s after %w: %w", streamID, lastErr, err)
		}
		present := make(map[string]struct{}, len(loaded))
		var maxVersion int64
		for _, evt := range loaded {
			present[evt.ID] = struct{}{}
			maxVersion = max(maxVersion, evt.Version)
		}
		// Count our persisted prefix by ID, not version: versions alone
		// cannot tell our prefix from a foreign writer's events.
		head := 0
		for head < len(remaining) {
			if _, ok := present[remaining[head].ID]; !ok {
				break
			}
			head++
		}
		if head == len(remaining) {
			return nil // all persisted (e.g. ack lost after success)
		}
		if head == 0 {
			if maxVersion != base {
				return fmt.Errorf("%w: stream %s advanced to v%d: %w", ErrConcurrencyConflict, streamID, maxVersion, lastErr)
			}
			continue // transient miss with nothing persisted: retry as-is
		}
		// Our prefix persisted; the tail must land exactly on the tip.
		if remaining[head].Version != maxVersion+1 {
			return fmt.Errorf("%w: stream %s at v%d, tail needs v%d: %w", ErrConcurrencyConflict, streamID, maxVersion, remaining[head].Version, lastErr)
		}
		base, remaining = maxVersion, remaining[head:]
	}
	return lastErr
}
