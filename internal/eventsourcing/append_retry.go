// AppendWithRetry bridges the atomicity gap between SQLiteStore (one
// transaction: all-or-nothing) and JetStreamStore (per-event publishes: a
// mid-batch failure can leave a prefix persisted). After a conflict it
// reloads the stream and retries only the unwritten tail, verified by event
// ID — never by version alone, so a foreign writer's events can never be
// silently dropped from the batch. Bounded attempts guarantee termination;
// a batch that cannot fit the tip returns the conflict for the caller to
// rebuild.
package eventsourcing

import (
	"context"
	"errors"
)

// appendRetryAttempts bounds tail-retry loops. Each attempt reloads, so this
// only trips under sustained contention; normal partial writes converge on
// the second attempt.
const appendRetryAttempts = 5

// AppendWithRetry appends events, resuming only the unwritten tail when a
// mid-batch failure leaves a prefix persisted. Success means every event is
// in the stream (verified by ID on the final attempt); ErrConcurrencyConflict
// means the batch no longer fits the tip and the caller must rebuild it from
// a fresh Load. Non-conflict errors (validation, type mismatch, I/O) return
// immediately — retrying those is pointless.
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
		if !errors.Is(err, ErrConcurrencyConflict) {
			return err
		}
		lastErr = err
		loaded, err := store.Load(ctx, streamID)
		if err != nil {
			return err
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
				return lastErr // foreign writer won; our versions can't fit
			}
			continue // transient miss with nothing persisted: retry as-is
		}
		// Our prefix persisted; the tail must land exactly on the tip.
		if remaining[head].Version != maxVersion+1 {
			return lastErr
		}
		base, remaining = maxVersion, remaining[head:]
	}
	return lastErr
}
