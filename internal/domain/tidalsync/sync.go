// Package tidalsync models the TIDAL sync run aggregate: a bounded-lifecycle
// aggregate recording observations from a TIDAL user collection synchronization.
package tidalsync

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/tidal"
)

// StreamTypeTidalSync identifies TIDAL sync run streams.
const StreamTypeTidalSync = "tidalsync"

const (
	EventTypeTidalSyncStarted       = "TidalSyncStarted"
	EventTypeTidalFavoritesObserved = "TidalFavoritesObserved"
	EventTypeTidalSyncCompleted     = "TidalSyncCompleted"
	EventTypeTidalSyncFailed        = "TidalSyncFailed"
)

// StartedPayload is recorded when a sync run begins.
type StartedPayload struct {
	RunID       string    `json:"run_id"`
	UserID      string    `json:"user_id"`
	CountryCode string    `json:"country_code"`
	StartedAt   time.Time `json:"started_at"`
}

// FavoritesObservedPayload holds a page of items pulled from TIDAL.
type FavoritesObservedPayload struct {
	RunID   string                `json:"run_id"`
	Page    int                   `json:"page"`
	Tracks  []tidal.ObservedTrack `json:"tracks,omitempty"`
	Albums  []tidal.ObservedAlbum `json:"albums,omitempty"`
	Artists []tidal.ObservedArtist`json:"artists,omitempty"`
}

// CompletedPayload records successful completion of a sync run.
type CompletedPayload struct {
	RunID        string    `json:"run_id"`
	CompletedAt  time.Time `json:"completed_at"`
	TotalTracks  int       `json:"total_tracks"`
	TotalAlbums  int       `json:"total_albums"`
	TotalArtists int       `json:"total_artists"`
}

// FailedPayload records failure of a sync run.
type FailedPayload struct {
	RunID    string    `json:"run_id"`
	FailedAt time.Time `json:"failed_at"`
	Error    string    `json:"error"`
}

// Sync is the aggregate root for a TIDAL sync run.
type Sync struct {
	eventsourcing.BaseAggregate

	userID      string
	countryCode string
	startedAt   time.Time
	completedAt time.Time
	failedAt    time.Time
	failureErr  string
	closed      bool

	tracks  []tidal.ObservedTrack
	albums  []tidal.ObservedAlbum
	artists []tidal.ObservedArtist
}

// NewSync creates a new TIDAL sync run aggregate.
func NewSync(runID, userID, countryCode string, startedAt time.Time) (*Sync, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("tidalsync: runID cannot be empty")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, errors.New("tidalsync: userID cannot be empty")
	}

	agg := &Sync{
		BaseAggregate: eventsourcing.NewBaseAggregate(runID, StreamTypeTidalSync),
		userID:        userID,
		countryCode:   countryCode,
		startedAt:     startedAt,
	}

	evt, err := agg.RecordThat(EventTypeTidalSyncStarted, StartedPayload{
		RunID:       runID,
		UserID:      userID,
		CountryCode: countryCode,
		StartedAt:   startedAt,
	}, nil)
	if err != nil {
		return nil, err
	}
	if err := agg.apply(evt); err != nil {
		return nil, err
	}
	return agg, nil
}

// RecordFavoritesObserved appends a page of observed favorites.
func (s *Sync) RecordFavoritesObserved(page int, tracks []tidal.ObservedTrack, albums []tidal.ObservedAlbum, artists []tidal.ObservedArtist) error {
	if s.closed {
		return errors.New("tidalsync: cannot observe on closed sync")
	}
	evt, err := s.RecordThat(EventTypeTidalFavoritesObserved, FavoritesObservedPayload{
		RunID:   s.AggregateID(),
		Page:    page,
		Tracks:  tracks,
		Albums:  albums,
		Artists: artists,
	}, nil)
	if err != nil {
		return err
	}
	return s.apply(evt)
}

// RecordCompleted marks the sync run as successfully completed.
func (s *Sync) RecordCompleted(completedAt time.Time) error {
	if s.closed {
		return errors.New("tidalsync: sync already closed")
	}
	evt, err := s.RecordThat(EventTypeTidalSyncCompleted, CompletedPayload{
		RunID:        s.AggregateID(),
		CompletedAt:  completedAt,
		TotalTracks:  len(s.tracks),
		TotalAlbums:  len(s.albums),
		TotalArtists: len(s.artists),
	}, nil)
	if err != nil {
		return err
	}
	return s.apply(evt)
}

// RecordFailed marks the sync run as failed.
func (s *Sync) RecordFailed(failedAt time.Time, reason string) error {
	if s.closed {
		return errors.New("tidalsync: sync already closed")
	}
	evt, err := s.RecordThat(EventTypeTidalSyncFailed, FailedPayload{
		RunID:    s.AggregateID(),
		FailedAt: failedAt,
		Error:    reason,
	}, nil)
	if err != nil {
		return err
	}
	return s.apply(evt)
}

// Tracks returns all observed tracks.
func (s *Sync) Tracks() []tidal.ObservedTrack { return s.tracks }

// Albums returns all observed albums.
func (s *Sync) Albums() []tidal.ObservedAlbum { return s.albums }

// Artists returns all observed artists.
func (s *Sync) Artists() []tidal.ObservedArtist { return s.artists }

// IsClosed reports whether the sync has reached terminal state.
func (s *Sync) IsClosed() bool { return s.closed }

// Error returns failure error if failed.
func (s *Sync) Error() string { return s.failureErr }

// Apply implements eventsourcing.Aggregate.
func (s *Sync) Apply(evt eventsourcing.Event) error { return s.apply(evt) }

func (s *Sync) apply(evt eventsourcing.Event) error {
	s.SetVersion(evt.Version)
	switch evt.EventType {
	case EventTypeTidalSyncStarted:
		var p StartedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &p); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.userID = p.UserID
		s.countryCode = p.CountryCode
		s.startedAt = p.StartedAt
	case EventTypeTidalFavoritesObserved:
		var p FavoritesObservedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &p); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.tracks = append(s.tracks, p.Tracks...)
		s.albums = append(s.albums, p.Albums...)
		s.artists = append(s.artists, p.Artists...)
	case EventTypeTidalSyncCompleted:
		var p CompletedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &p); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.completedAt = p.CompletedAt
		s.closed = true
	case EventTypeTidalSyncFailed:
		var p FailedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &p); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.failedAt = p.FailedAt
		s.failureErr = p.Error
		s.closed = true
	}
	return nil
}

// Replay reconstructs a Sync aggregate from its events.
func Replay(id string, events []eventsourcing.Event) (*Sync, error) {
	agg := &Sync{BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeTidalSync)}
	for _, evt := range events {
		if err := agg.Apply(evt); err != nil {
			return nil, fmt.Errorf("replaying tidalsync event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}
	return agg, nil
}
