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
	// SubjectQueueUpdated is the fanout subject for queue and job stats notifications.
	SubjectQueueUpdated = "queue.updated"
	// SubjectRanksUpdated is the fanout subject prefix for total_songs rank
	// recalculation completions. Published per genre as
	// "ranks.updated.<genre_group>" (ephemeral core NATS, like queue.updated:
	// a UI hint, fully derivable from the DB). Lets each client refresh its
	// visible page slice exactly once per recalc instead of fanning out one
	// artist.updated event per rewritten row.
	SubjectRanksUpdated = "ranks.updated"
	// SubjectRanksUpdatedWildcard matches per-genre rank notifications.
	SubjectRanksUpdatedWildcard = "ranks.updated.*"

	// SubjectDomainEventsPrefix is the durable JetStream prefix for domain events.
	// Per the Datastar YouTube canon (Beta 3 CQRS, Immutability & Event Sourcing):
	// commands are short POSTs returning 204, the append-only log is the source
	// of truth, and SQLite projections are rebuildable perfect indexes.
	// Ephemeral UI hints (artist.updated, queue.updated, ranks.updated.*) stay
	// on core NATS; everything under domain.events.> is durable in JetStream.
	SubjectDomainEventsPrefix = "domain.events"
	// SubjectDomainEventsWildcard matches all durable domain events.
	SubjectDomainEventsWildcard = "domain.events.>"
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
	// ScrapeProviderBrowserbase targets Browserbase cloud browser scraping.
	ScrapeProviderBrowserbase = "browserbase"
	// ScrapeProviderMobileSSR targets Spotify's mobile server-side rendered pages
	// directly (no JS rendering, no paid API). Uses an iOS Safari user-agent to
	// get the monthly listeners count from the initial HTML response.
	ScrapeProviderMobileSSR = "mobile-ssr"
)

// SupportedMessageVersions is the set of NATS payload schemas this binary
// reads. Unknown versions fail fast (never silently misread): ship the
// reader before the writer when introducing v2.
func SupportedMessageVersions() map[string]struct{} {
	return map[string]struct{}{SchemaVersionV1: {}}
}

// checkMessageVersion defaults empty to v1 (pre-versioning rows) and rejects
// unknown versions.
func checkMessageVersion(version, payload string) (string, error) {
	if version == "" {
		return SchemaVersionV1, nil
	}
	if _, ok := SupportedMessageVersions()[version]; !ok {
		return "", fmt.Errorf("unsupported %s version %q", payload, version)
	}
	return version, nil
}

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
		ScrapeProviderApify,
		ScrapeProviderBrowserbase,
		ScrapeProviderMobileSSR:
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

// SubjectRanksUpdatedForGenre returns the rank notification subject for a
// genre group (e.g. "ranks.updated.rock_metal"). An empty genre falls back
// to the unqualified subject, which matches no wildcard subscriber
// (ranks.updated.* requires a genre token) — callers must not publish blank
// genres unless the drop is intended.
func SubjectRanksUpdatedForGenre(genre string) string {
	genre = strings.TrimSpace(genre)
	if genre == "" {
		return SubjectRanksUpdated
	}
	return SubjectRanksUpdated + "." + genre
}

// SubjectDomainEvent returns the durable domain-event subject for an
// aggregate event, e.g. "domain.events.artist.<artistID>.ArtistCreated".
// sanitizeSubjectToken maps one subject token to its publish-safe form:
// trimmed, with NATS-reserved and whitespace runes replaced by underscores.
// Empty input becomes "-". Mirrors the per-token mapping in
// SubjectDomainEvent; keep the two in sync.
func sanitizeSubjectToken(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '>', ' ', '\t', '\n', '\r':
			return '_'
		}
		return r
	}, s)
	if s == "" {
		return "-"
	}
	return s
}

// Dots in IDs are sanitized since NATS treats dots as token separators.
func SubjectDomainEvent(streamType, streamID, eventType string) string {
	return SubjectDomainEventsPrefix + "." + sanitizeSubjectToken(streamType) + "." + sanitizeSubjectToken(streamID) + "." + sanitizeSubjectToken(eventType)
}

// SubjectDomainEventFilter returns the JetStream filter subject matching one
// aggregate's events across all types: domain.events.*.<id>.>. The wildcard
// covers the stream-type token; the ID is sanitized exactly as at publish
// time so the filter aligns with stored subjects.
func SubjectDomainEventFilter(streamID string) string {
	return SubjectDomainEventsPrefix + ".*." + sanitizeSubjectToken(streamID) + ".>"
}

// RanksGenreFromSubject decodes the genre group from a ranks.updated subject,
// returning "" for the unqualified subject or unrelated subjects.
func RanksGenreFromSubject(subject string) string {
	prefix := SubjectRanksUpdated + "."
	if !strings.HasPrefix(subject, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(subject, prefix))
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
	version, err := checkMessageVersion(req.Version, "scrape.request")
	if err != nil {
		return ScrapeRequested{}, err
	}
	req.Version = version
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
	version, err := checkMessageVersion(update.Version, "artist.updated")
	if err != nil {
		return ArtistUpdated{}, err
	}
	update.Version = version
	if update.FetchStatus == "" {
		update.FetchStatus = "idle"
	}
	return update, nil
}
