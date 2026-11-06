package songbackfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	defaultTidalSearchLimit = 5

	// TIDAL throttles by quota window rather than per-second burst, so the
	// default pacing is light. A silent 429 is a hard quota wall: the run is
	// resumable, so retrying it just sleeps against a window that will not
	// reopen mid-run. Retries therefore cover transport errors, 5xx, and 429s
	// that carry a bounded Retry-After.
	defaultTidalMinRequestInterval = 250 * time.Millisecond
	defaultTidalMaxRetries         = 4
	defaultTidalRetryBaseDelay     = 2 * time.Second
	maxTidalRetryAfterWait         = 60 * time.Second
)

type TidalLookup struct {
	BaseURL     string
	HTTPClient  *http.Client
	UserAgent   string
	CountryCode string
	SearchLimit int
	AuthToken   string

	MinRequestInterval time.Duration
	MaxRetries         int
	RetryBaseDelay     time.Duration

	mu            sync.Mutex
	nextRequestAt time.Time
}

func (l *TidalLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if strings.TrimSpace(song.Title) == "" {
		return nil, nil
	}
	if strings.TrimSpace(l.AuthToken) == "" {
		return nil, nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(l.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://openapi.tidal.com/v2"
	}

	countryCode := strings.TrimSpace(l.CountryCode)
	if countryCode == "" {
		countryCode = "US"
	}

	// TIDAL's v2 OpenAPI takes the search term as the required filter[query]
	// parameter on GET /searchResults (a path segment is rejected as a
	// resource ID). The dotted includes embed each track's artist and album
	// refs; a flat include=tracks,artists,albums returns three unlinked
	// collections instead.
	params := url.Values{}
	params.Set("filter[query]", strings.TrimSpace(song.Title))
	params.Set("explicitFilter", "INCLUDE")
	params.Set("countryCode", countryCode)
	params.Set("include", "tracks.artists,tracks.albums")
	endpoint := baseURL + "/searchResults?" + params.Encode()

	client := l.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	userAgent := strings.TrimSpace(l.UserAgent)
	if userAgent == "" {
		userAgent = "ListenLedger/1.0 (song artist backfill)"
	}

	resp, err := l.doRequest(ctx, client, endpoint, userAgent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tidal search returned %s", resp.Status)
	}

	var payload tidalSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode tidal search response: %w", err)
	}

	trackItems := tidalIncludedTrackItems(payload)
	limit := l.SearchLimit
	if limit <= 0 {
		limit = defaultTidalSearchLimit
	}
	if len(trackItems) > limit {
		trackItems = trackItems[:limit]
	}
	candidates := make([]TrackCandidate, 0, len(trackItems))
	for _, item := range trackItems {
		artistNames := tidalTrackArtistNames(item, payload.Included)
		if len(artistNames) == 0 {
			continue
		}

		confidence := tidalConfidence(song, primaryArtistPrefix, item, artistNames)
		if confidence <= 0 {
			continue
		}

		albumTitle, albumReleaseDate, albumType := tidalTrackAlbum(item, payload.Included)
		albumName, releaseType := splitReleaseTypeSuffix(albumTitle)
		if releaseType == "" && albumType != "" {
			// Most album titles carry no " - Single" suffix; the album's
			// own type attribute is the fallback carrier.
			releaseType = strings.ToLower(albumType)
		}
		releaseDate := firstNonEmpty(albumReleaseDate, strings.TrimSpace(item.Attributes.ReleaseDate))
		candidates = append(candidates, TrackCandidate{
			Source:      "tidal_track",
			Title:       strings.TrimSpace(item.Attributes.Title),
			ArtistNames: artistNames,
			AlbumName:   albumName,
			ReleaseDate: releaseDate,
			ReleaseType: releaseType,
			// Tracks carry no releaseDate in the searchResults payload —
			// the album does, so the year follows the same precedence.
			ReleaseYear: parseReleaseYear(releaseDate),
			Confidence:  confidence,
		})
	}

	return dedupeTrackCandidates(candidates), nil
}

func (l *TidalLookup) doRequest(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	maxRetries := l.maxRetries()
	for attempt := 0; ; attempt++ {
		if err := l.waitTurn(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build tidal request: %w", err)
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/vnd.api+json")
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(l.AuthToken))

		resp, err := client.Do(req)
		if err != nil {
			if attempt >= maxRetries || ctx.Err() != nil {
				return nil, fmt.Errorf("perform tidal request: %w", err)
			}
			if sleepErr := sleepWithContext(ctx, cappedExponentialDelay(l.retryBaseDelay(), attempt, 30*time.Second)); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		if retryErr := l.handleHTTPRetry(ctx, resp, attempt, maxRetries); retryErr != nil {
			return nil, retryErr
		}
	}
}

func (l *TidalLookup) handleHTTPRetry(ctx context.Context, resp *http.Response, attempt, maxRetries int) error {
	delay, retryable := l.retryPlan(resp, attempt)
	if !retryable || attempt >= maxRetries {
		status := resp.Status
		_ = resp.Body.Close()
		if attempt > 0 {
			return fmt.Errorf("tidal search returned %s after %d retries", status, attempt)
		}
		return fmt.Errorf("tidal search returned %s", status)
	}
	_ = resp.Body.Close()
	if sleepErr := sleepWithContext(ctx, delay); sleepErr != nil {
		return sleepErr
	}
	return nil
}

// retryPlan reports how long to wait before retrying resp, and whether the
// response is retryable at all. Transport failures and 5xx back off
// exponentially; a 429 only retries when the server sends a bounded
// Retry-After, since a silent quota 429 will not clear within the run.
func (l *TidalLookup) retryPlan(resp *http.Response, attempt int) (time.Duration, bool) {
	if resp.StatusCode == http.StatusTooManyRequests {
		if retryAfter, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok && retryAfter <= maxTidalRetryAfterWait {
			return retryAfter, true
		}
		return 0, false
	}
	if !tidalRetryableStatus(resp.StatusCode) {
		return 0, false
	}
	return cappedExponentialDelay(l.retryBaseDelay(), attempt, 30*time.Second), true
}

func (l *TidalLookup) waitTurn(ctx context.Context) error {
	interval := l.minRequestInterval()
	if interval <= 0 {
		return nil
	}

	l.mu.Lock()
	now := time.Now()
	wait := time.Duration(0)
	if l.nextRequestAt.After(now) {
		wait = time.Until(l.nextRequestAt)
		l.nextRequestAt = l.nextRequestAt.Add(interval)
	} else {
		l.nextRequestAt = now.Add(interval)
	}
	l.mu.Unlock()

	if wait <= 0 {
		return nil
	}

	return sleepWithContext(ctx, wait)
}

func (l *TidalLookup) minRequestInterval() time.Duration {
	if l.MinRequestInterval < 0 {
		return 0
	}
	if l.MinRequestInterval == 0 {
		return defaultTidalMinRequestInterval
	}
	return l.MinRequestInterval
}

func (l *TidalLookup) maxRetries() int {
	if l.MaxRetries < 0 {
		return 0
	}
	if l.MaxRetries == 0 {
		return defaultTidalMaxRetries
	}
	return l.MaxRetries
}

func (l *TidalLookup) retryBaseDelay() time.Duration {
	if l.RetryBaseDelay <= 0 {
		return defaultTidalRetryBaseDelay
	}
	return l.RetryBaseDelay
}

func tidalRetryableStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

type tidalSearchResponse struct {
	// Data is an array: the API wraps every search result set in a list of
	// searchResults resource objects (normally one).
	Data     []tidalSearchData `json:"data"`
	Included []tidalSearchItem `json:"included"`
}

type tidalSearchData struct {
	ID            string                   `json:"id"`
	Type          string                   `json:"type"`
	Attributes    map[string]any           `json:"attributes"`
	Relationships tidalSearchRelationships `json:"relationships"`
}

type tidalSearchRelationships struct {
	Tracks struct {
		Data []tidalRelationshipRef `json:"data"`
	} `json:"tracks"`
}

type tidalRelationshipRef struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type tidalSearchItem struct {
	Type          string              `json:"type"`
	ID            string              `json:"id"`
	Attributes    tidalItemAttributes `json:"attributes"`
	Relationships struct {
		Artists struct {
			Data []tidalRelationshipRef `json:"data"`
		} `json:"artists"`
		Albums struct {
			Data []tidalRelationshipRef `json:"data"`
		} `json:"albums"`
	} `json:"relationships"`
}

type tidalItemAttributes struct {
	Title       string `json:"title"`
	ReleaseDate string `json:"releaseDate"`
	Name        string `json:"name"`
	// Album type ("ALBUM", "SINGLE", "EP", ...): TIDAL has no release-type
	// field on tracks, and most album titles carry no " - Single" suffix,
	// so this attribute is the only carrier for most releases.
	Type string `json:"type"`
}

func tidalIncludedTrackItems(payload tidalSearchResponse) []tidalSearchItem {
	includedMap := make(map[string]tidalSearchItem, len(payload.Included))
	for _, item := range payload.Included {
		t := strings.ToUpper(strings.TrimSpace(item.Type))
		if t == "TRACKS" || t == "TRACK" {
			includedMap[item.ID] = item
		}
	}

	items := make([]tidalSearchItem, 0, len(payload.Included))
	for _, data := range payload.Data {
		for _, ref := range data.Relationships.Tracks.Data {
			if strings.ToUpper(strings.TrimSpace(ref.Type)) != "TRACKS" && strings.ToUpper(strings.TrimSpace(ref.Type)) != "TRACK" {
				continue
			}
			item, ok := includedMap[ref.ID]
			if !ok {
				continue
			}
			if strings.ToUpper(strings.TrimSpace(item.Type)) != "TRACKS" && strings.ToUpper(strings.TrimSpace(item.Type)) != "TRACK" {
				continue
			}
			items = append(items, item)
		}
	}
	return items
}

func tidalTrackArtistNames(track tidalSearchItem, included []tidalSearchItem) []string {
	includedArtists := make(map[string]tidalSearchItem, len(included))
	for _, item := range included {
		if strings.ToUpper(strings.TrimSpace(item.Type)) != "ARTISTS" && strings.ToUpper(strings.TrimSpace(item.Type)) != "ARTIST" {
			continue
		}
		includedArtists[item.ID] = item
	}

	names := make([]string, 0, len(track.Relationships.Artists.Data))
	seen := map[string]bool{}
	for _, ref := range track.Relationships.Artists.Data {
		item, ok := includedArtists[ref.ID]
		if !ok {
			continue
		}
		name := strings.TrimSpace(tidalIncludedArtistName(item.Attributes))
		key := normalizeKey(name)
		if name == "" || key == "" || seen[key] {
			continue
		}
		seen[key] = true
		names = append(names, name)
	}
	return names
}

func tidalIncludedArtistName(attributes tidalItemAttributes) string {
	return attributes.Name
}

// tidalTrackAlbum resolves a track's first included album to (title,
// releaseDate, type). TIDAL albums answer with all three attributes, so a
// track carries its album's title and release date for enrichment.
func tidalTrackAlbum(track tidalSearchItem, included []tidalSearchItem) (string, string, string) {
	includedAlbums := make(map[string]tidalSearchItem, len(included))
	for _, item := range included {
		if strings.ToUpper(strings.TrimSpace(item.Type)) != "ALBUMS" && strings.ToUpper(strings.TrimSpace(item.Type)) != "ALBUM" {
			continue
		}
		includedAlbums[item.ID] = item
	}

	for _, ref := range track.Relationships.Albums.Data {
		item, ok := includedAlbums[ref.ID]
		if !ok {
			continue
		}
		return strings.TrimSpace(item.Attributes.Title), strings.TrimSpace(item.Attributes.ReleaseDate), strings.TrimSpace(item.Attributes.Type)
	}
	return "", "", ""
}

// SplitReleaseTypeSuffix splits a distributor title suffix ("X - Single",
// "X - EP") into the clean album title and the release type. Tracks have no
// release-type attribute, so the title suffix is one carrier; the album's
// own type attribute is the other (see tidalTrackAlbum).
var releaseTypeSuffix = regexp.MustCompile(`(?i) - (single|ep)$`)

func SplitReleaseTypeSuffix(title string) (name, releaseType string) {
	title = strings.TrimSpace(title)
	m := releaseTypeSuffix.FindStringSubmatchIndex(title)
	if m == nil {
		return title, ""
	}
	return strings.TrimSpace(title[:m[0]]), strings.ToLower(title[m[2]:m[3]])
}

func splitReleaseTypeSuffix(title string) (name, releaseType string) {
	return SplitReleaseTypeSuffix(title)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func tidalConfidence(song SongInput, primaryArtistPrefix string, item tidalSearchItem, artistNames []string) float64 {
	titleBonus, ok := titleMatchBonus(song.Title, item.Attributes.Title)
	if !ok {
		return 0
	}

	matchedPrimary, firstMatched := primaryArtistMatch(primaryArtistPrefix, artistNames)
	if primaryArtistPrefix != "" && !matchedPrimary {
		return 0
	}

	confidence := 0.86 + titleBonus
	if firstMatched {
		confidence += 0.04
	} else if matchedPrimary {
		confidence += 0.02
	}

	if distinctArtistCount(artistNames) > 1 {
		confidence += 0.03
	}

	recordingYear := parseReleaseYear(item.Attributes.ReleaseDate)
	songYear := parseReleaseYear(song.ReleaseDate)
	confidence += yearMatchAdjustment(recordingYear, songYear, 0.03, 0.04)

	if strings.Contains(normalizeEllipsis(strings.TrimSpace(song.ArtistName)), "...") && distinctArtistCount(artistNames) > 1 {
		confidence += 0.02
	}

	if confidence > 0.99 {
		confidence = 0.99
	}
	if confidence < 0 {
		return 0
	}
	return confidence
}
