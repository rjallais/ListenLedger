// Package album is the album aggregate: every user edit is an event on its stream.
package album

import (
	"errors"
	"fmt"
	"strings"

	"ListenLedger/internal/eventsourcing"
)

const StreamTypeAlbum = "album"

const (
	EventTypeAlbumCreated            = "AlbumCreated"
	EventTypeAlbumStatusChanged      = "AlbumStatusChanged"
	EventTypeAlbumSongCountsAdjusted = "AlbumSongCountsAdjusted"
)

var (
	ErrEmptyTitle    = errors.New("album: title cannot be empty")
	ErrEmptyArtist   = errors.New("album: artist name cannot be empty")
	ErrInvalidStatus = errors.New("album: invalid status")
)

// CreatedPayload is the initial album registration.
type CreatedPayload struct {
	ID              string `json:"id"`
	Title           string `json:"title"`
	ArtistName      string `json:"artist_name"`
	Status          string `json:"status"`
	CollectionSongs int64  `json:"collection_songs"`
	TotalSongs      int64  `json:"total_songs"`
}

// StatusChangedPayload records a user status transition.
type StatusChangedPayload struct {
	AlbumID   string `json:"album_id"`
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
}

// SongCountsAdjustedPayload records a collection/total song count change.
type SongCountsAdjustedPayload struct {
	AlbumID         string `json:"album_id"`
	CollectionSongs int64  `json:"collection_songs"`
	TotalSongs      int64  `json:"total_songs"`
}

// Album is the event-sourced album.
type Album struct {
	eventsourcing.BaseAggregate

	Title           string
	ArtistName      string
	Status          string
	CollectionSongs int64
	TotalSongs      int64
}

func validStatus(status string) bool {
	switch status {
	case "full", "processed_once", "waiting":
		return true
	default:
		return false
	}
}

// NewAlbum records AlbumCreated. Status defaults to "waiting".
func NewAlbum(id, title, artistName, status string, collectionSongs, totalSongs int64) (*Album, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, ErrEmptyTitle
	}
	artistName = strings.TrimSpace(artistName)
	if artistName == "" {
		return nil, ErrEmptyArtist
	}
	if status == "" {
		status = "waiting"
	}
	if !validStatus(status) {
		return nil, ErrInvalidStatus
	}

	agg := &Album{BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeAlbum)}
	evt, err := agg.RecordThat(EventTypeAlbumCreated, CreatedPayload{
		ID: id, Title: title, ArtistName: artistName, Status: status,
		CollectionSongs: collectionSongs, TotalSongs: totalSongs,
	}, nil)
	if err != nil {
		return nil, err
	}
	if err := agg.apply(evt); err != nil {
		return nil, err
	}
	return agg, nil
}

// ChangeStatus records a status transition. A no-op status returns nil.
func (a *Album) ChangeStatus(newStatus string) error {
	if !validStatus(newStatus) {
		return ErrInvalidStatus
	}
	if a.Status == newStatus {
		return nil
	}
	evt, err := a.RecordThat(EventTypeAlbumStatusChanged, StatusChangedPayload{
		AlbumID: a.AggregateID(), OldStatus: a.Status, NewStatus: newStatus,
	}, nil)
	if err != nil {
		return err
	}
	return a.apply(evt)
}

// AdjustSongCounts records new song counts. Unchanged counts return nil.
func (a *Album) AdjustSongCounts(collectionSongs, totalSongs int64) error {
	if a.CollectionSongs == collectionSongs && a.TotalSongs == totalSongs {
		return nil
	}
	evt, err := a.RecordThat(EventTypeAlbumSongCountsAdjusted, SongCountsAdjustedPayload{
		AlbumID: a.AggregateID(), CollectionSongs: collectionSongs, TotalSongs: totalSongs,
	}, nil)
	if err != nil {
		return err
	}
	return a.apply(evt)
}

// Apply folds one event into the aggregate.
func (a *Album) Apply(evt eventsourcing.Event) error { return a.apply(evt) }

func (a *Album) apply(evt eventsourcing.Event) error {
	a.SetVersion(evt.Version)
	switch evt.EventType {
	case EventTypeAlbumCreated:
		var pl CreatedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.Title, a.ArtistName, a.Status = pl.Title, pl.ArtistName, pl.Status
		a.CollectionSongs, a.TotalSongs = pl.CollectionSongs, pl.TotalSongs
	case EventTypeAlbumStatusChanged:
		var pl StatusChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.Status = pl.NewStatus
	case EventTypeAlbumSongCountsAdjusted:
		var pl SongCountsAdjustedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.CollectionSongs, a.TotalSongs = pl.CollectionSongs, pl.TotalSongs
	}
	return nil
}

// Replay rebuilds an album from its event stream.
func Replay(id string, events []eventsourcing.Event) (*Album, error) {
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}
	agg := &Album{BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeAlbum)}
	for _, evt := range events {
		if err := agg.apply(evt); err != nil {
			return nil, fmt.Errorf("replaying album event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}
	return agg, nil
}
