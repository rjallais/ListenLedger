// Package scrapejob models the ScrapeJob aggregate: a bounded-lifecycle
// aggregate for one scrape request (Requested -> Started x N -> terminal).
// Each request_id owns its stream, so streams stay tiny and fixed-life
// instead of accumulating on a long-lived aggregate.
//
// scrape_jobs (SQLite/PocketBase) remains the rolling operational window over
// these streams (with retention purge); it is not a faithful projection and
// replay does not rebuild it. The streams are the complete log: they drive
// restart-safe dedup and audit.
package scrapejob

import (
	"fmt"

	"ListenLedger/internal/eventsourcing"
)

// StreamTypeScrapeJob identifies scrape-job event streams (stream ID = request_id).
const StreamTypeScrapeJob = "scrapejob"

const (
	EventTypeScrapeRequested   = "ScrapeRequested"
	EventTypeScrapeStarted     = "ScrapeStarted"
	EventTypeScrapeSucceeded   = "ScrapeSucceeded"
	EventTypeScrapeFailed      = "ScrapeFailed"
	EventTypeScrapeDeadLettered = "ScrapeDeadLettered"
)

// Statuses mirror the operational row states.
const (
	StatusQueued      = "queued"
	StatusProcessing  = "processing"
	StatusSucceeded   = "succeeded"
	StatusFailed      = "failed"
	StatusDeadLettered = "dead_lettered"
)

// RequestedPayload opens the lifecycle.
type RequestedPayload struct {
	RequestID   string `json:"request_id"`
	ArtistID    string `json:"artist_id"`
	CommandType string `json:"command_type,omitempty"`
}

// StartedPayload marks one processing attempt.
type StartedPayload struct {
	RequestID string `json:"request_id"`
	ArtistID  string `json:"artist_id"`
	Attempt   int    `json:"attempt"`
}

// SucceededPayload closes the lifecycle successfully.
type SucceededPayload struct {
	RequestID  string `json:"request_id"`
	ArtistID   string `json:"artist_id"`
	Provider   string `json:"provider,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
}

// FailedPayload closes (or re-closes, on retry loops) the lifecycle with a reason.
type FailedPayload struct {
	RequestID string `json:"request_id"`
	ArtistID  string `json:"artist_id"`
	Error     string `json:"error,omitempty"`
}

// DeadLetteredPayload marks poison removal from the queue.
type DeadLetteredPayload struct {
	RequestID string `json:"request_id"`
	ArtistID  string `json:"artist_id"`
	Error     string `json:"error,omitempty"`
}

// Job is the aggregate root for one scrape request.
type Job struct {
	eventsourcing.BaseAggregate

	requestID string
	artistID  string
	status    string
	attempts  int
	closed    bool
}

// NewScrapeJob opens the lifecycle with ScrapeRequested.
func NewScrapeJob(requestID, artistID, commandType string) (*Job, error) {
	if requestID == "" {
		return nil, fmt.Errorf("scrapejob: request_id cannot be empty")
	}
	if artistID == "" {
		return nil, fmt.Errorf("scrapejob: artist_id cannot be empty")
	}
	agg := &Job{
		BaseAggregate: eventsourcing.NewBaseAggregate(requestID, StreamTypeScrapeJob),
		requestID:     requestID,
		artistID:      artistID,
	}
	evt, err := agg.RecordThat(EventTypeScrapeRequested, RequestedPayload{
		RequestID:   requestID,
		ArtistID:    artistID,
		CommandType: commandType,
	}, nil)
	if err != nil {
		return nil, err
	}
	if err := agg.apply(evt); err != nil {
		return nil, err
	}
	return agg, nil
}

// Status returns the current lifecycle state.
func (j *Job) Status() string {
	return j.status
}

// RequestID returns the saga instance key (stream ID).
func (j *Job) RequestID() string {
	return j.requestID
}

// ArtistID returns the artist this saga instance scrapes.
func (j *Job) ArtistID() string {
	return j.artistID
}

// Attempts counts Started events (redeliveries included).
func (j *Job) Attempts() int {
	return j.attempts
}

// IsTerminal reports whether the lifecycle closed. Failed is re-openable by
// retry (failed -> processing); succeeded and dead-lettered are final.
func (j *Job) IsTerminal() bool {
	return j.closed
}

// RecordStarted appends a processing attempt. Allowed from any state except
// the final ones; failed streams re-open on retry.
func (j *Job) RecordStarted(corr ...eventsourcing.Correlation) (eventsourcing.Event, bool, error) {
	if j.closed {
		return eventsourcing.Event{}, false, nil
	}
	evt, err := j.RecordThat(EventTypeScrapeStarted, StartedPayload{
		RequestID: j.requestID,
		ArtistID:  j.artistID,
		Attempt:   j.attempts + 1,
	}, eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return eventsourcing.Event{}, false, err
	}
	if err := j.apply(evt); err != nil {
		return eventsourcing.Event{}, false, err
	}
	return evt, true, nil
}

// RecordSucceeded closes the lifecycle successfully. Final: further
// transitions record nothing.
func (j *Job) RecordSucceeded(provider string, durationMs int64, corr ...eventsourcing.Correlation) (eventsourcing.Event, bool, error) {
	if j.closed {
		return eventsourcing.Event{}, false, nil
	}
	evt, err := j.RecordThat(EventTypeScrapeSucceeded, SucceededPayload{
		RequestID:  j.requestID,
		ArtistID:   j.artistID,
		Provider:   provider,
		DurationMs: durationMs,
	}, eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return eventsourcing.Event{}, false, err
	}
	if err := j.apply(evt); err != nil {
		return eventsourcing.Event{}, false, err
	}
	return evt, true, nil
}

// RecordFailed records a failure with its reason. Failed streams stay open
// for retry; the reason text (stale_timeout, retry_exhausted, fetch errors)
// preserves what a row overwrite would destroy.
func (j *Job) RecordFailed(errMsg string, corr ...eventsourcing.Correlation) (eventsourcing.Event, bool, error) {
	if j.closed {
		return eventsourcing.Event{}, false, nil
	}
	evt, err := j.RecordThat(EventTypeScrapeFailed, FailedPayload{
		RequestID: j.requestID,
		ArtistID:  j.artistID,
		Error:     errMsg,
	}, eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return eventsourcing.Event{}, false, err
	}
	if err := j.apply(evt); err != nil {
		return eventsourcing.Event{}, false, err
	}
	return evt, true, nil
}

// RecordDeadLettered marks poison removal. Final.
func (j *Job) RecordDeadLettered(errMsg string, corr ...eventsourcing.Correlation) (eventsourcing.Event, bool, error) {
	if j.closed {
		return eventsourcing.Event{}, false, nil
	}
	evt, err := j.RecordThat(EventTypeScrapeDeadLettered, DeadLetteredPayload{
		RequestID: j.requestID,
		ArtistID:  j.artistID,
		Error:     errMsg,
	}, eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return eventsourcing.Event{}, false, err
	}
	if err := j.apply(evt); err != nil {
		return eventsourcing.Event{}, false, err
	}
	return evt, true, nil
}

// Apply reconstitutes state from one event.
func (j *Job) Apply(evt eventsourcing.Event) error {
	return j.apply(evt)
}

func (j *Job) apply(evt eventsourcing.Event) error {
	j.SetVersion(evt.Version)
	switch evt.EventType {
	case EventTypeScrapeRequested:
		var pl RequestedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		j.requestID = pl.RequestID
		j.artistID = pl.ArtistID
		j.status = StatusQueued
	case EventTypeScrapeStarted:
		var pl StartedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		j.attempts++
		j.status = StatusProcessing
	case EventTypeScrapeSucceeded:
		var pl SucceededPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		j.status = StatusSucceeded
		j.closed = true
	case EventTypeScrapeFailed:
		var pl FailedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		j.status = StatusFailed
	case EventTypeScrapeDeadLettered:
		var pl DeadLetteredPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		j.status = StatusDeadLettered
		j.closed = true
	}
	return nil
}

// Replay folds a request stream into its aggregate.
func Replay(requestID string, events []eventsourcing.Event) (*Job, error) {
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	agg := &Job{
		BaseAggregate: eventsourcing.NewBaseAggregate(requestID, StreamTypeScrapeJob),
	}
	for _, evt := range events {
		if err := agg.apply(evt); err != nil {
			return nil, fmt.Errorf("replaying scrapejob event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}
	return agg, nil
}
