// Package messaging defines event contracts and serialization for NATS subjects.
package messaging

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	// SchemaVersionV1 identifies the current payload schema.
	SchemaVersionV1 = "v1"

	// SubjectScrapeRequest is the work queue subject for scrape jobs.
	SubjectScrapeRequest = "scrape.request"
	// SubjectScrapeRequestWildcard matches provider-routed scrape jobs.
	SubjectScrapeRequestWildcard = "scrape.request.*"
	// SubjectScrapeDLQ is the subject for dead-lettered scrape jobs.
	SubjectScrapeDLQ = "scrape.dlq"
	// SubjectArtistUpdated is the fanout subject for artist update notifications.
	SubjectArtistUpdated = "artist.updated"
)

const (
	// ScrapeProviderAny routes to the legacy/fallback worker.
	ScrapeProviderAny = "any"
	// ScrapeProviderLocal targets local headless scraping.
	ScrapeProviderLocal = "local"
	// ScrapeProviderBrowserless targets Browserless scraping.
	ScrapeProviderBrowserless = "browserless"
	// ScrapeProviderScrapingAnt targets ScrapingAnt scraping.
	ScrapeProviderScrapingAnt = "scrapingant"
	// ScrapeProviderScraperAPI targets ScraperAPI scraping.
	ScrapeProviderScraperAPI = "scraperapi"
	// ScrapeProviderApify targets Apify scraping.
	ScrapeProviderApify = "apify"
	// ScrapeProviderLocalBrowserless targets self-hosted Browserless scraping.
	ScrapeProviderLocalBrowserless = "local-browserless"
)

// ScrapeRequested is the durable queue payload for a listener refresh job.
type ScrapeRequested struct {
	Version    string `json:"version"`
	RequestID  string `json:"request_id,omitzero"`
	ArtistID   string `json:"artist_id"`
	SpotifyID  string `json:"spotify_id"`
	ArtistName string `json:"artist_name"`
	QueuedAt   string `json:"queued_at,omitzero"`
}

// ArtistUpdated is the event payload emitted after an artist update attempt.
type ArtistUpdated struct {
	Version     string `json:"version"`
	RequestID   string `json:"request_id,omitzero"`
	ArtistID    string `json:"artist_id"`
	Name        string `json:"name"`
	FetchStatus string `json:"fetch_status"`
	UpdatedAt   string `json:"updated_at"`

	MonthlyListeners int `json:"monthly_listeners"`
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

// NormalizeScrapeProvider sanitizes a provider key for subject routing.
func NormalizeScrapeProvider(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	switch provider {
	case ScrapeProviderLocal,
		ScrapeProviderBrowserless,
		ScrapeProviderLocalBrowserless,
		ScrapeProviderScrapingAnt,
		ScrapeProviderScraperAPI,
		ScrapeProviderApify:
		return provider
	default:
		return ScrapeProviderAny
	}
}

// SubjectScrapeRequestForProvider returns the routed scrape subject for a provider.
func SubjectScrapeRequestForProvider(provider string) string {
	normalized := NormalizeScrapeProvider(provider)
	if normalized == ScrapeProviderAny {
		return SubjectScrapeRequest
	}
	return SubjectScrapeRequest + "." + normalized
}

// ScrapeProviderFromSubject decodes a provider key from a scrape subject.
func ScrapeProviderFromSubject(subject string) string {
	if subject == SubjectScrapeRequest {
		return ScrapeProviderAny
	}

	prefix := SubjectScrapeRequest + "."
	if !strings.HasPrefix(subject, prefix) {
		return ScrapeProviderAny
	}

	return NormalizeScrapeProvider(strings.TrimPrefix(subject, prefix))
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
