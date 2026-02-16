//go:build goexperiment.jsonv2

// Package messaging defines event contracts and serialization for NATS subjects.
package messaging

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	// SchemaVersionV1 identifies the current payload schema.
	SchemaVersionV1 = "v1"

	// SubjectScrapeRequest is the work queue subject for scrape jobs.
	SubjectScrapeRequest = "scrape.request"
	// SubjectScrapeDLQ is the subject for dead-lettered scrape jobs.
	SubjectScrapeDLQ = "scrape.dlq"
	// SubjectArtistUpdated is the fanout subject for artist update notifications.
	SubjectArtistUpdated = "artist.updated"
)

// ScrapeRequested is the durable queue payload for a listener refresh job.
type ScrapeRequested struct {
	Version    string `json:"version"`
	RequestID  string `json:"request_id,omitempty"`
	ArtistID   string `json:"artist_id"`
	SpotifyID  string `json:"spotify_id"`
	ArtistName string `json:"artist_name"`
	QueuedAt   string `json:"queued_at,omitempty"`
}

// ArtistUpdated is the event payload emitted after an artist update attempt.
type ArtistUpdated struct {
	Version          string `json:"version"`
	RequestID        string `json:"request_id,omitempty"`
	ArtistID         string `json:"artist_id"`
	Name             string `json:"name"`
	MonthlyListeners int    `json:"monthly_listeners"`
	FetchStatus      string `json:"fetch_status"`
	UpdatedAt        string `json:"updated_at"`
}

// NewScrapeRequested constructs a versioned scrape request payload.
func NewScrapeRequested(artistID, spotifyID, artistName, requestID string) ScrapeRequested {
	return ScrapeRequested{
		Version:    SchemaVersionV1,
		RequestID:  requestID,
		ArtistID:   artistID,
		SpotifyID:  spotifyID,
		ArtistName: artistName,
		QueuedAt:   time.Now().Format(time.RFC3339),
	}
}

// MarshalScrapeRequested serializes a scrape request payload.
func MarshalScrapeRequested(req ScrapeRequested) ([]byte, error) {
	if req.Version == "" {
		req.Version = SchemaVersionV1
	}
	return json.Marshal(req)
}

// UnmarshalScrapeRequested decodes and validates a scrape request payload.
func UnmarshalScrapeRequested(data []byte) (ScrapeRequested, error) {
	var req ScrapeRequested
	if err := json.Unmarshal(data, &req); err != nil {
		return ScrapeRequested{}, err
	}
	if req.Version == "" {
		req.Version = SchemaVersionV1
	}
	if req.ArtistID == "" {
		return ScrapeRequested{}, fmt.Errorf("missing artist_id")
	}
	if req.SpotifyID == "" {
		return ScrapeRequested{}, fmt.Errorf("missing spotify_id")
	}
	return req, nil
}

// NewArtistUpdated constructs a versioned artist update payload.
func NewArtistUpdated(artistID, name string, listeners int, fetchStatus, requestID string) ArtistUpdated {
	return ArtistUpdated{
		Version:          SchemaVersionV1,
		RequestID:        requestID,
		ArtistID:         artistID,
		Name:             name,
		MonthlyListeners: listeners,
		FetchStatus:      fetchStatus,
		UpdatedAt:        time.Now().Format(time.RFC3339),
	}
}

// MarshalArtistUpdated serializes an artist update payload.
func MarshalArtistUpdated(update ArtistUpdated) ([]byte, error) {
	if update.Version == "" {
		update.Version = SchemaVersionV1
	}
	return json.Marshal(update)
}

// UnmarshalArtistUpdated decodes an artist update payload.
func UnmarshalArtistUpdated(data []byte) (ArtistUpdated, error) {
	var update ArtistUpdated
	if err := json.Unmarshal(data, &update); err != nil {
		return ArtistUpdated{}, err
	}
	if update.Version == "" {
		update.Version = SchemaVersionV1
	}
	if update.FetchStatus == "" {
		update.FetchStatus = "idle"
	}
	return update, nil
}
