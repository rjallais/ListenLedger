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

// sanitizeSubjectToken maps one subject token to its publish-safe form; the
// mapping is injective (distinct inputs yield distinct tokens), so OCC scopes
// and filters never conflate two aggregates. Whitespace (including leading
// and trailing) is escaped, never trimmed; only the empty input maps to "-".
func sanitizeSubjectToken(s string) string {
	if s == "" {
		// "-" is reachable only here: a literal "-" encodes to "--".
		return "-"
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for _, r := range s {
		switch r {
		case '-':
			b.WriteString("--")
		case '.':
			b.WriteString("-d-")
		case '*':
			b.WriteString("-s-")
		case '>':
			b.WriteString("-g-")
		case ' ':
			b.WriteString("-w-")
		case '\t':
			b.WriteString("-t-")
		case '\n':
			b.WriteString("-n-")
		case '\r':
			b.WriteString("-r-")
		case '\f':
			b.WriteString("-f-")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// SubjectDomainEvent returns the durable domain-event subject for an
// aggregate event, e.g. "domain.events.artist.ar_123.ArtistCreated".
// Dots in IDs are escaped since NATS treats dots as token separators; the
// escape keeps distinct IDs distinct ("a.b" vs "a_b" encode differently).
func SubjectDomainEvent(streamType, streamID, eventType string) string {
	return SubjectDomainEventsPrefix + "." + sanitizeSubjectToken(streamType) + "." + sanitizeSubjectToken(streamID) + "." + sanitizeSubjectToken(eventType)
}

// ParseSubjectDomainEvent inverts SubjectDomainEvent, decoding each token's
// dash escapes back to the raw stream type, ID, and event type. Encoded
// tokens never contain literal dots, so a plain dot-split is safe. It
// rejects wrong prefixes, wrong token counts, and malformed escapes —
// fail-closed, since replay and tooling must never read a stranger's stream.
// The single token "-" decodes to "" (the empty-input marker).
func ParseSubjectDomainEvent(subject string) (streamType, streamID, eventType string, err error) {
	tokens := strings.Split(subject, ".")
	if len(tokens) != 5 || tokens[0] != "domain" || tokens[1] != "events" {
		return "", "", "", fmt.Errorf("not a domain event subject: %q", subject)
	}
	if streamType, err = decodeSubjectToken(tokens[2]); err != nil {
		return "", "", "", fmt.Errorf("decoding stream type in %q: %w", subject, err)
	}
	if streamID, err = decodeSubjectToken(tokens[3]); err != nil {
		return "", "", "", fmt.Errorf("decoding stream ID in %q: %w", subject, err)
	}
	if eventType, err = decodeSubjectToken(tokens[4]); err != nil {
		return "", "", "", fmt.Errorf("decoding event type in %q: %w", subject, err)
	}
	return streamType, streamID, eventType, nil
}

// decodeSubjectToken inverts sanitizeSubjectToken's dash escapes.
func decodeSubjectToken(token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("empty subject token")
	}
	if token == "-" {
		return "", nil // empty-input marker
	}
	var b strings.Builder
	b.Grow(len(token))
	for i := 0; i < len(token); {
		if token[i] != '-' {
			b.WriteByte(token[i])
			i++
			continue
		}
		// Dash escape: "--" is a literal dash, "-x-" maps one reserved rune.
		if i+1 < len(token) && token[i+1] == '-' {
			b.WriteByte('-')
			i += 2
			continue
		}
		if i+2 >= len(token) || token[i+2] != '-' {
			return "", fmt.Errorf("malformed escape in %q", token)
		}
		switch token[i+1] {
		case 'd':
			b.WriteByte('.')
		case 's':
			b.WriteByte('*')
		case 'g':
			b.WriteByte('>')
		case 'w':
			b.WriteByte(' ')
		case 't':
			b.WriteByte('\t')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 'f':
			b.WriteByte('\f')
		default:
			return "", fmt.Errorf("malformed escape in %q", token)
		}
		i += 3
	}
	return b.String(), nil
}

// SubjectDomainEventScope returns the wildcard subject covering one
// aggregate's events across all stream types: domain.events.*.<id>.*. Used
// for per-aggregate sequence checks (GetLastMsgForSubject, expected-subject
// publish anchors); the server resolves the wildcard to the latest matching
// message. The scope deliberately omits the stream type so it matches the
// replay scope in SubjectDomainEventFilter: SQLite keys history by stream ID
// alone (with a type-consistency guard), and the JetStream OCC must too —
// otherwise two types sharing one ID could both mint version 1. The ID is
// encoded exactly as at publish time.
func SubjectDomainEventScope(streamID string) string {
	return SubjectDomainEventsPrefix + ".*." + sanitizeSubjectToken(streamID) + ".*"
}

// SubjectDomainEventFilter returns the JetStream filter subject matching one
// aggregate's events across all types: domain.events.*.<id>.>. The wildcard
// covers the stream-type token; the ID is encoded exactly as at publish time
// (injective escape) so the filter aligns with stored subjects without
// conflating distinct IDs.
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
