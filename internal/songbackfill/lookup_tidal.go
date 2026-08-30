package songbackfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultTidalSearchLimit = 5

type TidalLookup struct {
	BaseURL     string
	HTTPClient  *http.Client
	UserAgent   string
	CountryCode string
	SearchLimit int
	AuthToken   string
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

	endpoint := fmt.Sprintf(
		"%s/searchResults/%s?explicitFilter=INCLUDE&countryCode=%s&include=tracks&include=artists",
		baseURL,
		url.PathEscape(strings.TrimSpace(song.Title)),
		url.QueryEscape(countryCode),
	)

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

		candidates = append(candidates, TrackCandidate{
			Source:      "tidal_track",
			Title:       strings.TrimSpace(item.Attributes.Title),
			ArtistNames: artistNames,
			ReleaseYear: parseReleaseYear(item.Attributes.ReleaseDate),
			Confidence:  confidence,
		})
	}

	return dedupeTrackCandidates(candidates), nil
}

func (l *TidalLookup) doRequest(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build tidal request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/vnd.api+json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(l.AuthToken))
	return client.Do(req)
}

type tidalSearchResponse struct {
	Data     tidalSearchData   `json:"data"`
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
	} `json:"relationships"`
}

type tidalItemAttributes struct {
	Title       string `json:"title"`
	ReleaseDate string `json:"releaseDate"`
	Name        string `json:"name"`
}

func tidalIncludedTrackItems(payload tidalSearchResponse) []tidalSearchItem {
	includedMap := make(map[string]tidalSearchItem, len(payload.Included))
	for _, item := range payload.Included {
		includedMap[item.ID] = item
	}

	items := make([]tidalSearchItem, 0, len(payload.Data.Relationships.Tracks.Data))
	for _, ref := range payload.Data.Relationships.Tracks.Data {
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
