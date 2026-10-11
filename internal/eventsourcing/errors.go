package eventsourcing

import "errors"

var (
	// ErrConcurrencyConflict is returned when an event version does not match expected aggregate version.
	ErrConcurrencyConflict = errors.New("eventsourcing: optimistic concurrency conflict")

	// ErrAppendIndeterminate marks a publish failure with unknown outcome
	// (timeout, lost ack, disconnect after a prefix may have persisted).
	// Callers must reconcile by reloading and retrying the unwritten tail
	// (MsgID dedup keeps that safe), never by assuming success or failure.
	ErrAppendIndeterminate = errors.New("eventsourcing: append outcome indeterminate")

	// ErrStreamNotFound is returned when attempting to load an event stream that has no events.
	ErrStreamNotFound = errors.New("eventsourcing: stream not found")

	// ErrSnapshotNotFound is returned when no snapshot exists for a stream.
	ErrSnapshotNotFound = errors.New("eventsourcing: snapshot not found")
)
