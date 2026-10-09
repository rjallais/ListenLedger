package artist

import "time"

const (
	EventTypeArtistCreated                  = "ArtistCreated"
	EventTypeArtistMonthlyListenersScraped  = "ArtistMonthlyListenersScraped"
	EventTypeArtistStatusChanged            = "ArtistStatusChanged"
	EventTypeArtistFetchStatusChanged       = "ArtistFetchStatusChanged"
	EventTypeArtistCollectionSongsAdjusted  = "ArtistCollectionSongsAdjusted"
)

// CreatedPayload contains state for initial artist registration.
type CreatedPayload struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	SpotifyID  string `json:"spotify_id"`
	GenreGroup string `json:"genre_group"`
	ListStatus string `json:"list_status"`
}

// ListenersScrapedPayload contains temporal monthly listener results from a scraper provider.
type ListenersScrapedPayload struct {
	ArtistID          string    `json:"artist_id"`
	MonthlyListeners  int64     `json:"monthly_listeners"`
	PreviousListeners int64     `json:"previous_listeners"`
	Delta             int64     `json:"delta"`
	Provider          string    `json:"provider"`
	DurationMs        int64     `json:"duration_ms"`
	ScrapedAt         time.Time `json:"scraped_at"`
}

// StatusChangedPayload records user or workflow list status transitions.
type StatusChangedPayload struct {
	ArtistID  string `json:"artist_id"`
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
	Reason    string `json:"reason,omitempty"`
}

// FetchStatusChangedPayload tracks worker queue state transitions.
type FetchStatusChangedPayload struct {
	ArtistID    string `json:"artist_id"`
	FetchStatus string `json:"fetch_status"`
	Error       string `json:"error,omitempty"`
}

// CollectionSongsAdjustedPayload tracks collection vs total album songs count.
type CollectionSongsAdjustedPayload struct {
	ArtistID        string `json:"artist_id"`
	CollectionSongs int64  `json:"collection_songs"`
	TotalSongs      int64  `json:"total_songs"`
}
