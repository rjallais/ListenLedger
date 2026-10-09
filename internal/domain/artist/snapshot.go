package artist

import (
	"encoding/json/v2"
	"fmt"
	"time"
)

// artistSnapshot is the serializable freeze-frame of an Artist aggregate.
// Snapshots are a load optimization only: the event log stays the source of
// truth and loaders replay every event after Snapshot.Version.
type artistSnapshot struct {
	SchemaVersion    int       `json:"schema_version"`
	Name             string    `json:"name"`
	SpotifyID        string    `json:"spotify_id"`
	MonthlyListeners int64     `json:"monthly_listeners"`
	GenreGroup       string    `json:"genre_group"`
	ListStatus       string    `json:"list_status"`
	FetchStatus      string    `json:"fetch_status"`
	CollectionSongs  int64     `json:"collection_songs"`
	TotalSongs       int64     `json:"total_songs"`
	LastUpdated      time.Time `json:"last_updated"`
	CreatedAt        time.Time `json:"created_at"`
}

// snapshotSchemaVersion is the writer version for artist snapshots.
const snapshotSchemaVersion = 1

// marshalSnapshot encodes current aggregate state for SaveSnapshot.
func (a *Artist) marshalSnapshot() ([]byte, error) {
	payload, err := json.Marshal(artistSnapshot{
		SchemaVersion:    snapshotSchemaVersion,
		Name:             a.Name,
		SpotifyID:        a.SpotifyID,
		MonthlyListeners: a.MonthlyListeners,
		GenreGroup:       a.GenreGroup,
		ListStatus:       a.ListStatus,
		FetchStatus:      a.FetchStatus,
		CollectionSongs:  a.CollectionSongs,
		TotalSongs:       a.TotalSongs,
		LastUpdated:      a.LastUpdated,
		CreatedAt:        a.CreatedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling artist snapshot: %w", err)
	}
	return payload, nil
}

// restoreFromSnapshot rebuilds the aggregate base from a snapshot payload at
// the snapshot version; callers then apply every event after that version.
func (a *Artist) restoreFromSnapshot(version int64, payload []byte) error {
	var snap artistSnapshot
	if err := json.Unmarshal(payload, &snap); err != nil {
		return fmt.Errorf("unmarshaling artist snapshot: %w", err)
	}
	if snap.SchemaVersion != snapshotSchemaVersion {
		return fmt.Errorf("unsupported artist snapshot version %d", snap.SchemaVersion)
	}
	a.Name = snap.Name
	a.SpotifyID = snap.SpotifyID
	a.MonthlyListeners = snap.MonthlyListeners
	a.GenreGroup = snap.GenreGroup
	a.ListStatus = snap.ListStatus
	a.FetchStatus = snap.FetchStatus
	a.CollectionSongs = snap.CollectionSongs
	a.TotalSongs = snap.TotalSongs
	a.LastUpdated = snap.LastUpdated
	a.CreatedAt = snap.CreatedAt
	a.SetVersion(version)
	return nil
}
