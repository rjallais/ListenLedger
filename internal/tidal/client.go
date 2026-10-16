package tidal

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/unicode/norm"
)

const (
	DefaultBaseURL     = "https://openapi.tidal.com/v2"
	DefaultCountryCode = "BR"
)

// Client interacts with the TIDAL OpenAPI v2.
type Client struct {
	BaseURL        string
	HTTPClient     *http.Client
	CountryCode    string
	AccessToken    string
	TokenRefresher func(ctx context.Context) (string, error)
}

// NewClient initializes a TIDAL client with defaults.
func NewClient(accessToken string) *Client {
	return &Client{
		BaseURL:     DefaultBaseURL,
		CountryCode: DefaultCountryCode,
		AccessToken: accessToken,
		HTTPClient:  &http.Client{Timeout: 30 * time.Second},
	}
}

type jsonAPIResponse struct {
	Data     []jsonAPIDataItem     `json:"data"`
	Included []jsonAPIResourceItem `json:"included"`
	Links    struct {
		Next string `json:"next"`
	} `json:"links"`
}

type jsonAPIDataItem struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Meta struct {
		AddedAt string `json:"addedAt"`
	} `json:"meta"`
}

type jsonAPIResourceItem struct {
	ID            string                        `json:"id"`
	Type          string                        `json:"type"`
	Attributes    map[string]any                `json:"attributes"`
	Relationships map[string]jsonAPIRelationVal `json:"relationships"`
}

type jsonAPIRelationVal struct {
	Data any `json:"data"` // single object or array of {id, type}
}

func (c *Client) doRequest(ctx context.Context, endpoint string) (*http.Response, error) {
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	maxRetries := 5
	backoff := 1 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("building request: %w", err)
		}

		req.Header.Set("Accept", "application/vnd.api+json")
		req.Header.Set("User-Agent", "ListenLedger/1.0 (TIDAL sync)")
		if c.AccessToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.AccessToken)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		// Handle 401 Unauthorized with token refresh if refresher configured
		if resp.StatusCode == http.StatusUnauthorized && c.TokenRefresher != nil {
			_ = resp.Body.Close()
			newToken, refreshErr := c.TokenRefresher(ctx)
			if refreshErr != nil {
				return nil, fmt.Errorf("token expired and refresh failed: %w", refreshErr)
			}
			c.AccessToken = newToken
			continue
		}

		// Handle 429 Too Many Requests with backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			waitDuration := backoff
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
					waitDuration = time.Duration(seconds) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitDuration):
			}
			backoff *= 2
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("exceeded max retries for %s", endpoint)
}

// FetchFavoriteTracks retrieves the user's favorite/saved tracks using pagination.
func (c *Client) FetchFavoriteTracks(ctx context.Context, userID string, limit int) ([]ObservedTrack, error) {
	countryCode := cmp.Or(strings.TrimSpace(c.CountryCode), DefaultCountryCode)
	baseURL := cmp.Or(strings.TrimRight(strings.TrimSpace(c.BaseURL), "/"), DefaultBaseURL)

	nextURL := fmt.Sprintf(
		"%s/userCollectionTracks/me/relationships/items?countryCode=%s&include=items,items.artists,items.albums",
		baseURL,
		url.QueryEscape(countryCode),
	)

	var allTracks []ObservedTrack

	for nextURL != "" {
		resp, err := c.doRequest(ctx, nextURL)
		if err != nil {
			return nil, fmt.Errorf("requesting favorite tracks: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("favorite tracks endpoint returned status %s", resp.Status)
		}

		var payload jsonAPIResponse
		if err := json.UnmarshalRead(resp.Body, &payload); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decoding favorite tracks response: %w", err)
		}
		_ = resp.Body.Close()

		// Index included resources by (type, id)
		resourceMap := make(map[string]jsonAPIResourceItem, len(payload.Included))
		for _, inc := range payload.Included {
			key := fmt.Sprintf("%s:%s", strings.ToLower(inc.Type), inc.ID)
			resourceMap[key] = inc
		}

		for _, item := range payload.Data {
			trackKey := fmt.Sprintf("tracks:%s", item.ID)
			trackRes, ok := resourceMap[trackKey]
			if !ok {
				// Fallback if data itself is the track resource
				trackKey = fmt.Sprintf("%s:%s", strings.ToLower(item.Type), item.ID)
				trackRes = resourceMap[trackKey]
			}

			track := parseObservedTrack(item, trackRes, resourceMap)
			allTracks = append(allTracks, track)

			if limit > 0 && len(allTracks) >= limit {
				return allTracks[:limit], nil
			}
		}

		nextURL = resolveNextURL(baseURL, payload.Links.Next)
		if nextURL != "" {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	return allTracks, nil
}

func parseObservedTrack(dataItem jsonAPIDataItem, trackRes jsonAPIResourceItem, resourceMap map[string]jsonAPIResourceItem) ObservedTrack {
	track := ObservedTrack{
		ID: dataItem.ID,
	}

	if trackRes.ID != "" {
		if val, ok := trackRes.Attributes["title"].(string); ok {
			track.Title = norm.NFC.String(strings.TrimSpace(val))
		}
		if val, ok := trackRes.Attributes["isrc"].(string); ok {
			track.ISRC = strings.TrimSpace(val)
		}
		if val, ok := trackRes.Attributes["releaseDate"].(string); ok {
			track.ReleaseDate = strings.TrimSpace(val)
		}
		if val, ok := trackRes.Attributes["duration"]; ok {
			track.Duration = parseDurationSeconds(val)
		}

		// Artist relationships
		if rel, ok := trackRes.Relationships["artists"]; ok {
			track.ArtistNames = extractArtistNames(rel, resourceMap)
		}

		// Album relationship
		if rel, ok := trackRes.Relationships["albums"]; ok {
			track.AlbumTitle, track.AlbumReleaseDate = extractAlbumDetails(rel, resourceMap)
		}
	}

	if track.ReleaseDate == "" && track.AlbumReleaseDate != "" {
		track.ReleaseDate = track.AlbumReleaseDate
	}

	if dataItem.Meta.AddedAt != "" {
		if t, err := time.Parse(time.RFC3339, dataItem.Meta.AddedAt); err == nil {
			track.AddedAt = t
		}
	}

	return track
}

func resolveNextURL(baseURL, nextRaw string) string {
	nextRaw = strings.TrimSpace(nextRaw)
	if nextRaw == "" {
		return ""
	}
	if strings.HasPrefix(nextRaw, "http://") || strings.HasPrefix(nextRaw, "https://") {
		return nextRaw
	}
	if strings.HasPrefix(nextRaw, "/") {
		return baseURL + nextRaw
	}
	return baseURL + "/" + nextRaw
}

func parseDurationSeconds(val any) int {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		v = strings.TrimSpace(v)
		if v == "" {
			return 0
		}
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		if strings.HasPrefix(v, "PT") || strings.HasPrefix(v, "P") {
			return parseISODuration(v)
		}
	}
	return 0
}

func parseISODuration(s string) int {
	s = strings.TrimPrefix(s, "P")
	s = strings.TrimPrefix(s, "T")
	total := 0
	cur := 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			cur = cur*10 + int(r-'0')
		} else {
			switch r {
			case 'H':
				total += cur * 3600
			case 'M':
				total += cur * 60
			case 'S':
				total += cur
			}
			cur = 0
		}
	}
	return total
}

func extractArtistNames(rel jsonAPIRelationVal, resourceMap map[string]jsonAPIResourceItem) []string {
	var names []string
	items := parseRelItems(rel.Data)
	for _, it := range items {
		key := fmt.Sprintf("artists:%s", it.ID)
		if artistRes, ok := resourceMap[key]; ok {
			if name, ok := artistRes.Attributes["name"].(string); ok && strings.TrimSpace(name) != "" {
				names = append(names, norm.NFC.String(strings.TrimSpace(name)))
			}
		}
	}
	return names
}

func extractAlbumDetails(rel jsonAPIRelationVal, resourceMap map[string]jsonAPIResourceItem) (string, string) {
	items := parseRelItems(rel.Data)
	if len(items) == 0 {
		return "", ""
	}
	key := fmt.Sprintf("albums:%s", items[0].ID)
	if albumRes, ok := resourceMap[key]; ok {
		var title, relDate string
		if t, ok := albumRes.Attributes["title"].(string); ok {
			title = norm.NFC.String(strings.TrimSpace(t))
		}
		if d, ok := albumRes.Attributes["releaseDate"].(string); ok {
			relDate = strings.TrimSpace(d)
		}
		return title, relDate
	}
	return "", ""
}

type relRef struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

func parseRelItems(data any) []relRef {
	if data == nil {
		return nil
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	var arr []relRef
	if err := json.Unmarshal(b, &arr); err == nil {
		return arr
	}
	var single relRef
	if err := json.Unmarshal(b, &single); err == nil && single.ID != "" {
		return []relRef{single}
	}
	return nil
}

// FetchFavoriteAlbums retrieves favorite albums.
func (c *Client) FetchFavoriteAlbums(ctx context.Context, userID string, limit int) ([]ObservedAlbum, error) {
	countryCode := cmp.Or(strings.TrimSpace(c.CountryCode), DefaultCountryCode)
	baseURL := cmp.Or(strings.TrimRight(strings.TrimSpace(c.BaseURL), "/"), DefaultBaseURL)

	nextURL := fmt.Sprintf(
		"%s/userCollectionAlbums/me/relationships/items?countryCode=%s&include=items,items.artists",
		baseURL,
		url.QueryEscape(countryCode),
	)

	var allAlbums []ObservedAlbum

	for nextURL != "" {
		resp, err := c.doRequest(ctx, nextURL)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("favorite albums endpoint returned status %s", resp.Status)
		}

		var payload jsonAPIResponse
		if err := json.UnmarshalRead(resp.Body, &payload); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()

		resourceMap := make(map[string]jsonAPIResourceItem, len(payload.Included))
		for _, inc := range payload.Included {
			resourceMap[fmt.Sprintf("%s:%s", strings.ToLower(inc.Type), inc.ID)] = inc
		}

		for _, item := range payload.Data {
			albumKey := fmt.Sprintf("albums:%s", item.ID)
			albumRes := resourceMap[albumKey]

			album := ObservedAlbum{ID: item.ID}
			if albumRes.ID != "" {
				if t, ok := albumRes.Attributes["title"].(string); ok {
					album.Title = strings.TrimSpace(t)
				}
				if d, ok := albumRes.Attributes["releaseDate"].(string); ok {
					album.ReleaseDate = strings.TrimSpace(d)
				}
				if rt, ok := albumRes.Attributes["type"].(string); ok {
					album.ReleaseType = strings.TrimSpace(rt)
				}
				if rel, ok := albumRes.Relationships["artists"]; ok {
					album.ArtistNames = extractArtistNames(rel, resourceMap)
				}
			}
			if item.Meta.AddedAt != "" {
				if t, err := time.Parse(time.RFC3339, item.Meta.AddedAt); err == nil {
					album.AddedAt = t
				}
			}

			allAlbums = append(allAlbums, album)
			if limit > 0 && len(allAlbums) >= limit {
				return allAlbums[:limit], nil
			}
		}

		nextURL = resolveNextURL(baseURL, payload.Links.Next)
	}

	return allAlbums, nil
}

// FetchFavoriteArtists retrieves followed/favorite artists.
func (c *Client) FetchFavoriteArtists(ctx context.Context, userID string, limit int) ([]ObservedArtist, error) {
	countryCode := cmp.Or(strings.TrimSpace(c.CountryCode), DefaultCountryCode)
	baseURL := cmp.Or(strings.TrimRight(strings.TrimSpace(c.BaseURL), "/"), DefaultBaseURL)

	nextURL := fmt.Sprintf(
		"%s/userCollectionArtists/me/relationships/items?countryCode=%s&include=items",
		baseURL,
		url.QueryEscape(countryCode),
	)

	var allArtists []ObservedArtist

	for nextURL != "" {
		resp, err := c.doRequest(ctx, nextURL)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("favorite artists endpoint returned status %s", resp.Status)
		}

		var payload jsonAPIResponse
		if err := json.UnmarshalRead(resp.Body, &payload); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()

		resourceMap := make(map[string]jsonAPIResourceItem, len(payload.Included))
		for _, inc := range payload.Included {
			resourceMap[fmt.Sprintf("%s:%s", strings.ToLower(inc.Type), inc.ID)] = inc
		}

		for _, item := range payload.Data {
			artistKey := fmt.Sprintf("artists:%s", item.ID)
			artistRes := resourceMap[artistKey]

			artist := ObservedArtist{ID: item.ID}
			if artistRes.ID != "" {
				if name, ok := artistRes.Attributes["name"].(string); ok {
					artist.Name = strings.TrimSpace(name)
				}
			}
			if item.Meta.AddedAt != "" {
				if t, err := time.Parse(time.RFC3339, item.Meta.AddedAt); err == nil {
					artist.AddedAt = t
				}
			}

			allArtists = append(allArtists, artist)
			if limit > 0 && len(allArtists) >= limit {
				return allArtists[:limit], nil
			}
		}

		nextURL = resolveNextURL(baseURL, payload.Links.Next)
	}

	return allArtists, nil
}
