package artist

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"ListenLedger/internal/eventsourcing"
)

var (
	ErrEmptyArtistName  = errors.New("artist: name cannot be empty")
	ErrInvalidListStatus = errors.New("artist: invalid list status")
	ErrInvalidFetchStatus = errors.New("artist: invalid fetch status")
)

const StreamTypeArtist = "artist"

// Artist represents the domain aggregate for a musical artist.
type Artist struct {
	eventsourcing.BaseAggregate

	Name             string
	SpotifyID        string
	MonthlyListeners int64
	GenreGroup       string
	ListStatus       string
	FetchStatus      string
	CollectionSongs  int64
	TotalSongs       int64
	LastUpdated      time.Time
	CreatedAt        time.Time
}

// NewArtist creates an Artist and records the ArtistCreated event.
func NewArtist(id, name, spotifyID, genreGroup, listStatus string) (*Artist, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ErrEmptyArtistName
	}
	if genreGroup != "rock_metal" && genreGroup != "everything_else" {
		genreGroup = "everything_else"
	}
	if listStatus == "" {
		listStatus = "waiting"
	} else {
		switch listStatus {
		case "included", "recently_added", "not_added", "waiting":
		default:
			return nil, ErrInvalidListStatus
		}
	}

	agg := &Artist{
		BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeArtist),
	}

	payload := CreatedPayload{
		ID:         id,
		Name:       name,
		SpotifyID:  strings.TrimSpace(spotifyID),
		GenreGroup: genreGroup,
		ListStatus: listStatus,
	}

	evt, err := agg.RecordThat(EventTypeArtistCreated, payload, nil)
	if err != nil {
		return nil, err
	}

	if err := agg.apply(evt); err != nil {
		return nil, err
	}

	return agg, nil
}

// RecordMonthlyListeners updates the artist's listener count from a scrape provider.
// Optional corr carries the saga request_id for causation tracing.
func (a *Artist) RecordMonthlyListeners(listeners int64, provider string, durationMs int64, corr ...eventsourcing.Correlation) error {
	delta := listeners - a.MonthlyListeners
	now := time.Now().UTC()

	payload := ListenersScrapedPayload{
		ArtistID:          a.AggregateID(),
		MonthlyListeners:  listeners,
		PreviousListeners: a.MonthlyListeners,
		Delta:             delta,
		Provider:          provider,
		DurationMs:        durationMs,
		ScrapedAt:         now,
	}

	evt, err := a.RecordThat(EventTypeArtistMonthlyListenersScraped, payload,
		eventsourcing.FirstCorrelation(corr).Metadata(map[string]any{
			"provider": provider,
			"delta":    delta,
		}))
	if err != nil {
		return err
	}

	return a.apply(evt)
}

// ChangeListStatus updates the list_status (included, recently_added, not_added, waiting).
func (a *Artist) ChangeListStatus(newStatus, reason string, corr ...eventsourcing.Correlation) error {
	switch newStatus {
	case "included", "recently_added", "not_added", "waiting":
	default:
		return ErrInvalidListStatus
	}
	if a.ListStatus == newStatus {
		return nil
	}

	payload := StatusChangedPayload{
		ArtistID:  a.AggregateID(),
		OldStatus: a.ListStatus,
		NewStatus: newStatus,
		Reason:    reason,
	}

	evt, err := a.RecordThat(EventTypeArtistStatusChanged, payload,
		eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return err
	}

	return a.apply(evt)
}

// SetFetchStatus transitions fetch_status between idle, pending, and failed.
func (a *Artist) SetFetchStatus(status, errMsg string, corr ...eventsourcing.Correlation) error {
	switch status {
	case "idle", "pending", "failed":
	default:
		return ErrInvalidFetchStatus
	}
	if a.FetchStatus == status {
		return nil
	}

	payload := FetchStatusChangedPayload{
		ArtistID:    a.AggregateID(),
		FetchStatus: status,
		Error:       errMsg,
	}

	evt, err := a.RecordThat(EventTypeArtistFetchStatusChanged, payload,
		eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return err
	}

	return a.apply(evt)
}

// AdjustCollectionSongs updates collection_songs and total_songs counts.
func (a *Artist) AdjustCollectionSongs(collectionSongs, totalSongs int64, corr ...eventsourcing.Correlation) error {
	if a.CollectionSongs == collectionSongs && a.TotalSongs == totalSongs {
		return nil
	}

	payload := CollectionSongsAdjustedPayload{
		ArtistID:        a.AggregateID(),
		CollectionSongs: collectionSongs,
		TotalSongs:      totalSongs,
	}

	evt, err := a.RecordThat(EventTypeArtistCollectionSongsAdjusted, payload,
		eventsourcing.FirstCorrelation(corr).Metadata(nil))
	if err != nil {
		return err
	}

	return a.apply(evt)
}

// Apply handles applying an event to aggregate state.
func (a *Artist) Apply(evt eventsourcing.Event) error {
	return a.apply(evt)
}

func (a *Artist) apply(evt eventsourcing.Event) error {
	a.SetVersion(evt.Version)

	switch evt.EventType {
	case EventTypeArtistCreated:
		var pl CreatedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.Name = pl.Name
		a.SpotifyID = pl.SpotifyID
		a.GenreGroup = pl.GenreGroup
		a.ListStatus = pl.ListStatus
		a.FetchStatus = "idle"
		a.CreatedAt = evt.CreatedAt
		a.LastUpdated = evt.CreatedAt

	case EventTypeArtistMonthlyListenersScraped:
		var pl ListenersScrapedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.MonthlyListeners = pl.MonthlyListeners
		a.FetchStatus = "idle"
		a.LastUpdated = pl.ScrapedAt

	case EventTypeArtistStatusChanged:
		var pl StatusChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.ListStatus = pl.NewStatus

	case EventTypeArtistFetchStatusChanged:
		var pl FetchStatusChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.FetchStatus = pl.FetchStatus

	case EventTypeArtistCollectionSongsAdjusted:
		var pl CollectionSongsAdjustedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return fmt.Errorf("unmarshaling %s: %w", evt.EventType, err)
		}
		a.CollectionSongs = pl.CollectionSongs
		a.TotalSongs = pl.TotalSongs
	}

	return nil
}

// Replay reconstructs the Artist from historical events.
func Replay(id string, events []eventsourcing.Event) (*Artist, error) {
	if len(events) == 0 {
		return nil, eventsourcing.ErrStreamNotFound
	}

	agg := &Artist{
		BaseAggregate: eventsourcing.NewBaseAggregate(id, StreamTypeArtist),
	}

	for _, evt := range events {
		if err := agg.apply(evt); err != nil {
			return nil, fmt.Errorf("replaying artist event %s v%d: %w", evt.EventType, evt.Version, err)
		}
	}

	return agg, nil
}
