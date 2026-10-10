// Package song is the song aggregate: creation and recent-flag toggles are events.
package song

import (
	"errors"
	"fmt"
	"strings"

	"ListenLedger/internal/eventsourcing"
)

const StreamTypeSong = "song"

const (
	EventTypeSongCreated       = "SongCreated"
	EventTypeRecentFlagChanged = "RecentFlagChanged"
)

// ErrEmptyTitle is returned when a song has no title.
var ErrEmptyTitle = errors.New("song: title cannot be empty")

// CreatedPayload is the initial song registration.
type CreatedPayload struct {
	ID             string `json:"id"`
	Title          string `json:"title"`
	ArtistName     string `json:"artist_name"`
	Album          string `json:"album"`
	ReleaseDate    string `json:"release_date"`
	ReleaseYear    int64  `json:"release_year"`
	ReleaseType    string `json:"release_type"`
	SpotifyID      string `json:"spotify_id"`
	IsRecent       bool   `json:"is_recent"`
	RecentBatchSeq int64  `json:"recent_batch_seq"`
	RecentBatchPos int64  `json:"recent_batch_pos"`
}

// RecentFlagChangedPayload records a recent-playlist toggle.
type RecentFlagChangedPayload struct {
	SongID         string `json:"song_id"`
	IsRecent       bool   `json:"is_recent"`
	RecentBatchSeq int64  `json:"recent_batch_seq"`
	RecentBatchPos int64  `json:"recent_batch_pos"`
}

// Song is the event-sourced song.
type Song struct {
	eventsourcing.BaseAggregate

	Title          string
	ArtistName     string
	Album          string
	ReleaseDate    string
	ReleaseYear    int64
	ReleaseType    string
	SpotifyID      string
	IsRecent       bool
	RecentBatchSeq int64
	RecentBatchPos int64
}

// NewSong records SongCreated.
func NewSong(id, title, artistName, albumName, releaseDate, releaseType, spotifyID string, releaseYear, batchSeq, batchPos int64, isRecent bool) (*Song, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, ErrEmptyTitle
	}
	agg := &Song{BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeSong)}
	evt, err := agg.RecordThat(EventTypeSongCreated, CreatedPayload{
		ID: id, Title: title, ArtistName: strings.TrimSpace(artistName),
		Album: strings.TrimSpace(albumName), ReleaseDate: releaseDate,
		ReleaseYear: releaseYear, ReleaseType: releaseType, SpotifyID: spotifyID,
		IsRecent: isRecent, RecentBatchSeq: batchSeq, RecentBatchPos: batchPos,
	}, nil)
	if err != nil {
		return nil, err
	}
	if err := agg.apply(evt); err != nil {
		return nil, err
	}
	return agg, nil
}

// SetRecent records a recent-flag change. An unchanged flag returns nil.
func (s *Song) SetRecent(isRecent bool, batchSeq, batchPos int64) error {
	if s.IsRecent == isRecent && s.RecentBatchSeq == batchSeq && s.RecentBatchPos == batchPos {
		return nil
	}
	evt, err := s.RecordThat(EventTypeRecentFlagChanged, RecentFlagChangedPayload{
		SongID: s.AggregateID(), IsRecent: isRecent, RecentBatchSeq: batchSeq, RecentBatchPos: batchPos,
	}, nil)
	if err != nil {
		return err
	}
	return s.apply(evt)
}

// Apply folds one event into the aggregate.
func (s *Song) Apply(evt eventsourcing.Event) error { return s.apply(evt) }

func (s *Song) apply(evt eventsourcing.Event) error {
	s.SetVersion(evt.Version)
	switch evt.EventType {
	case EventTypeSongCreated:
		var pl CreatedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.Title, s.ArtistName, s.Album = pl.Title, pl.ArtistName, pl.Album
		s.ReleaseDate, s.ReleaseYear, s.ReleaseType, s.SpotifyID = pl.ReleaseDate, pl.ReleaseYear, pl.ReleaseType, pl.SpotifyID
		s.IsRecent, s.RecentBatchSeq, s.RecentBatchPos = pl.IsRecent, pl.RecentBatchSeq, pl.RecentBatchPos
	case EventTypeRecentFlagChanged:
		var pl RecentFlagChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.IsRecent, s.RecentBatchSeq, s.RecentBatchPos = pl.IsRecent, pl.RecentBatchSeq, pl.RecentBatchPos
	}
	return nil
}

// Replay rebuilds a song from its event stream.
func Replay(id string, events []eventsourcing.Event) (*Song, error) {
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	agg := &Song{BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeSong)}
	for _, evt := range events {
		if err := agg.apply(evt); err != nil {
			return nil, fmt.Errorf("replaying song event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}
	return agg, nil
}
