//go:build goexperiment.jsonv2

package songbackfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const defaultDeezerSearchLimit = 5

type DeezerLookup struct {
	BaseURL    string
	HTTPClient *http.Client
	UserAgent  string
	SearchLimit int
}

func (l *DeezerLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if strings.TrimSpace(song.Title) == "" || strings.TrimSpace(primaryArtistPrefix) == "" {
		return nil, nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(l.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://api.deezer.com"
	}

	searchURL := fmt.Sprintf(
		"%s/search/track?q=%s",
		baseURL,
		url.QueryEscape(fmt.Sprintf(`artist:"%s" track:"%s"`, primaryArtistPrefix, song.Title)),
	)

	client := l.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	userAgent := strings.TrimSpace(l.UserAgent)
	if userAgent == "" {
		userAgent = "ListenLedger/1.0 (song artist backfill)"
	}

	resp, err := deezerGet(ctx, client, searchURL, userAgent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("deezer search returned %s", resp.Status)
	}

	var payload deezerSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode deezer search response: %w", err)
	}

	limit := l.SearchLimit
	if limit <= 0 {
		limit = defaultDeezerSearchLimit
	}
	if limit > len(payload.Data) {
		limit = len(payload.Data)
	}

	candidates := make([]TrackCandidate, 0, limit)
	for _, item := range payload.Data[:limit] {
		detailURL := fmt.Sprintf("%s/track/%d", baseURL, item.ID)
		detailResp, err := deezerGet(ctx, client, detailURL, userAgent)
		if err != nil {
			continue
		}

		var detail deezerTrackDetail
		decodeErr := json.NewDecoder(detailResp.Body).Decode(&detail)
		_ = detailResp.Body.Close()
		if decodeErr != nil {
			continue
		}

		artistNames := deezerContributorNames(detail)
		if len(artistNames) == 0 {
			continue
		}

		confidence := deezerConfidence(song, primaryArtistPrefix, detail, artistNames)
		if confidence <= 0 {
			continue
		}

		candidates = append(candidates, TrackCandidate{
			Source:      "deezer_track",
			Title:       strings.TrimSpace(detail.Title),
			ArtistNames: artistNames,
			ReleaseYear: parseReleaseYear(detail.ReleaseDate),
			Confidence:  confidence,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if len(candidates[i].ArtistNames) != len(candidates[j].ArtistNames) {
			return len(candidates[i].ArtistNames) > len(candidates[j].ArtistNames)
		}
		return candidates[i].Title < candidates[j].Title
	})

	return dedupeTrackCandidates(candidates), nil
}

func deezerGet(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	return client.Do(req)
}

type deezerSearchResponse struct {
	Data []deezerTrackSummary `json:"data"`
}

type deezerTrackSummary struct {
	ID int64 `json:"id"`
}

type deezerTrackDetail struct {
	Title       string                  `json:"title"`
	ReleaseDate string                  `json:"release_date"`
	Artist      deezerArtistContributor `json:"artist"`
	Contributors []deezerArtistContributor `json:"contributors"`
}

type deezerArtistContributor struct {
	Name string `json:"name"`
	Role string `json:"role"`
}

func deezerContributorNames(detail deezerTrackDetail) []string {
	names := make([]string, 0, len(detail.Contributors)+1)
	seen := map[string]bool{}

	appendName := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		key := normalizeKey(name)
		if seen[key] {
			return
		}
		seen[key] = true
		names = append(names, name)
	}

	for _, contributor := range detail.Contributors {
		appendName(contributor.Name)
	}
	if len(names) == 0 {
		appendName(detail.Artist.Name)
	}

	return names
}

func deezerConfidence(song SongInput, primaryArtistPrefix string, detail deezerTrackDetail, artistNames []string) float64 {
	titleBonus, ok := titleMatchBonus(song.Title, detail.Title)
	if !ok {
		return 0
	}

	matchedPrimary, firstMatched := primaryArtistMatch(primaryArtistPrefix, artistNames)
	if !matchedPrimary {
		return 0
	}

	confidence := 0.84 + titleBonus
	if firstMatched {
		confidence += 0.05
	} else {
		confidence += 0.03
	}

	if distinctArtistCount(artistNames) > 1 {
		confidence += 0.02
	}

	recordingYear := parseReleaseYear(detail.ReleaseDate)
	songYear := parseReleaseYear(song.ReleaseDate)
	confidence += yearMatchAdjustment(recordingYear, songYear, 0.03, 0.03)

	if confidence > 0.98 {
		confidence = 0.98
	}
	if confidence < 0 {
		return 0
	}

	return confidence
}
