// Package song is the song aggregate: creation and recent-flag toggles are events.
package song

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"ListenLedger/internal/eventsourcing"
)

const StreamTypeSong = "song"

const (
	EventTypeSongCreated       = "SongCreated"
	EventTypeRecentFlagChanged = "RecentFlagChanged"
	// EventTypeSongMetadataEnriched records external metadata (e.g. TIDAL)
	// filling fields the stream never knew. It carries the merged post-state
	// so replay converges; enrichment commands only record it when a
	// previously-empty field gains a value.
	EventTypeSongMetadataEnriched       = "SongMetadataEnriched"
	EventTypeTidalIDLinked              = "TidalIDLinked"
	EventTypeSongMetadataCorrected      = "SongMetadataCorrected"
	EventTypeSongArtistCreditsCorrected = "SongArtistCreditsCorrected"
	EventTypeSongMarkedDuplicate        = "SongMarkedDuplicate"
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

// MetadataEnrichedPayload is the merged metadata post-state after an
// external enrichment. ReleaseType joined later than the other fields:
// rows written before it carry none, and apply treats the empty value as
// "unchanged" (enrichment never clears fields), so no upcaster is needed.
type MetadataEnrichedPayload struct {
	SongID      string `json:"song_id"`
	Album       string `json:"album"`
	ReleaseDate string `json:"release_date"`
	ReleaseYear int64  `json:"release_year"`
	ReleaseType string `json:"release_type"`
	Source      string `json:"source"`
}

// TidalIDLinkedPayload records persistent association with a TIDAL track.
type TidalIDLinkedPayload struct {
	SongID      string  `json:"song_id"`
	TidalID     string  `json:"tidal_id"`
	MatchMethod string  `json:"match_method"`
	Confidence  float64 `json:"confidence"`
}

// MetadataCorrectedPayload records explicit corrections to existing metadata.
type MetadataCorrectedPayload struct {
	SongID      string `json:"song_id"`
	Album       string `json:"album"`
	ReleaseDate string `json:"release_date"`
	ReleaseYear int64  `json:"release_year"`
	ReleaseType string `json:"release_type"`
	Source      string `json:"source"`
}

// ArtistCreditsCorrectedPayload records corrections to artist credit strings.
type ArtistCreditsCorrectedPayload struct {
	SongID     string `json:"song_id"`
	ArtistName string `json:"artist_name"`
	Source     string `json:"source"`
}

// MarkedDuplicatePayload records that this song is a duplicate of a canonical song.
type MarkedDuplicatePayload struct {
	SongID      string `json:"song_id"`
	CanonicalID string `json:"canonical_id"`
	Source      string `json:"source"`
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
	TidalID        string
	DuplicateOf    string
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

// EnrichMetadata fills only fields the song is missing from external
// metadata, recording the merged post-state as SongMetadataEnriched. It
// reports whether an event was recorded: empty external values, or values
// for fields already present, record nothing (reruns converge).
func (s *Song) EnrichMetadata(album, releaseDate, releaseType, source string) (bool, error) {
	album = strings.TrimSpace(album)
	releaseDate = strings.TrimSpace(releaseDate)
	releaseType = strings.TrimSpace(releaseType)
	mergedAlbum, mergedDate, mergedYear, mergedType := s.Album, s.ReleaseDate, s.ReleaseYear, s.ReleaseType
	if mergedAlbum == "" {
		mergedAlbum = album
	}
	if mergedDate == "" {
		mergedDate = releaseDate
	}
	if mergedType == "" {
		mergedType = releaseType
	}
	if mergedYear == 0 && mergedDate != "" {
		mergedYear = releaseYearOf(mergedDate)
	}
	if mergedAlbum == s.Album && mergedDate == s.ReleaseDate && mergedYear == s.ReleaseYear && mergedType == s.ReleaseType {
		return false, nil
	}
	evt, err := s.RecordThat(EventTypeSongMetadataEnriched, MetadataEnrichedPayload{
		SongID: s.AggregateID(), Album: mergedAlbum, ReleaseDate: mergedDate,
		ReleaseYear: mergedYear, ReleaseType: mergedType, Source: strings.TrimSpace(source),
	}, nil)
	if err != nil {
		return false, err
	}
	return true, s.apply(evt)
}

// LinkTidalID records a TidalIDLinked event if the song's tidal ID differs.
func (s *Song) LinkTidalID(tidalID, matchMethod string, confidence float64) (bool, error) {
	tidalID = strings.TrimSpace(tidalID)
	if tidalID == "" || s.TidalID == tidalID {
		return false, nil
	}
	evt, err := s.RecordThat(EventTypeTidalIDLinked, TidalIDLinkedPayload{
		SongID:      s.AggregateID(),
		TidalID:     tidalID,
		MatchMethod: matchMethod,
		Confidence:  confidence,
	}, nil)
	if err != nil {
		return false, err
	}
	return true, s.apply(evt)
}

// CorrectMetadata records a SongMetadataCorrected event.
func (s *Song) CorrectMetadata(album, releaseDate, releaseType, source string) (bool, error) {
	album = strings.TrimSpace(album)
	releaseDate = strings.TrimSpace(releaseDate)
	releaseType = strings.TrimSpace(releaseType)
	year := s.ReleaseYear
	if releaseDate != "" {
		if parsed := releaseYearOf(releaseDate); parsed != 0 {
			year = parsed
		}
	}
	if album == s.Album && releaseDate == s.ReleaseDate && year == s.ReleaseYear && releaseType == s.ReleaseType {
		return false, nil
	}
	evt, err := s.RecordThat(EventTypeSongMetadataCorrected, MetadataCorrectedPayload{
		SongID:      s.AggregateID(),
		Album:       album,
		ReleaseDate: releaseDate,
		ReleaseYear: year,
		ReleaseType: releaseType,
		Source:      source,
	}, nil)
	if err != nil {
		return false, err
	}
	return true, s.apply(evt)
}

// CorrectArtistCredits records a SongArtistCreditsCorrected event.
func (s *Song) CorrectArtistCredits(artistName, source string) (bool, error) {
	artistName = strings.TrimSpace(artistName)
	if artistName == "" || artistName == s.ArtistName {
		return false, nil
	}
	evt, err := s.RecordThat(EventTypeSongArtistCreditsCorrected, ArtistCreditsCorrectedPayload{
		SongID:     s.AggregateID(),
		ArtistName: artistName,
		Source:     source,
	}, nil)
	if err != nil {
		return false, err
	}
	return true, s.apply(evt)
}

// MarkDuplicate records a SongMarkedDuplicate event.
func (s *Song) MarkDuplicate(canonicalID, source string) (bool, error) {
	canonicalID = strings.TrimSpace(canonicalID)
	if canonicalID == "" || canonicalID == s.AggregateID() || s.DuplicateOf == canonicalID {
		return false, nil
	}
	evt, err := s.RecordThat(EventTypeSongMarkedDuplicate, MarkedDuplicatePayload{
		SongID:      s.AggregateID(),
		CanonicalID: canonicalID,
		Source:      source,
	}, nil)
	if err != nil {
		return false, err
	}
	return true, s.apply(evt)
}

// releaseYearOf extracts the leading 4-digit year from a release date
// ("1992-09-29", "1992-09", or "1992").
func releaseYearOf(date string) int64 {
	if len(date) < 4 {
		return 0
	}
	year, err := strconv.Atoi(date[:4])
	if err != nil || year < 1000 {
		return 0
	}
	return int64(year)
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
	case EventTypeSongMetadataEnriched:
		var pl MetadataEnrichedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.Album, s.ReleaseDate, s.ReleaseYear = pl.Album, pl.ReleaseDate, pl.ReleaseYear
		// Pre-field (v1) rows decode release_type as empty; absent means
		// unchanged, and enrichment never clears fields.
		if pl.ReleaseType != "" {
			s.ReleaseType = pl.ReleaseType
		}
	case EventTypeTidalIDLinked:
		var pl TidalIDLinkedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.TidalID = pl.TidalID
	case EventTypeSongMetadataCorrected:
		var pl MetadataCorrectedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.Album, s.ReleaseDate, s.ReleaseYear, s.ReleaseType = pl.Album, pl.ReleaseDate, pl.ReleaseYear, pl.ReleaseType
	case EventTypeSongArtistCreditsCorrected:
		var pl ArtistCreditsCorrectedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.ArtistName = pl.ArtistName
	case EventTypeSongMarkedDuplicate:
		var pl MarkedDuplicatePayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		s.DuplicateOf = pl.CanonicalID
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
