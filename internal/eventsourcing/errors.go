package eventsourcing

import "errors"

var (
	// ErrConcurrencyConflict is returned when an event version does not match expected aggregate version.
	ErrConcurrencyConflict = errors.New("eventsourcing: optimistic concurrency conflict")

	// ErrStreamNotFound is returned when attempting to load an event stream that has no events.
	ErrStreamNotFound = errors.New("eventsourcing: stream not found")

	// ErrSnapshotNotFound is returned when no snapshot exists for a stream.
	ErrSnapshotNotFound = errors.New("eventsourcing: snapshot not found")
)
