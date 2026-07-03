//go:build goexperiment.jsonv2

package songbackfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultMusicBrainzMinRequestInterval = 1100 * time.Millisecond
	defaultMusicBrainzMaxRetries         = 4
	defaultMusicBrainzRetryBaseDelay     = 2 * time.Second
)

type MusicBrainzLookup struct {
	BaseURL            string
	HTTPClient         *http.Client
	UserAgent          string
	MinRequestInterval time.Duration
	MaxRetries         int
	RetryBaseDelay     time.Duration

	mu            sync.Mutex
	nextRequestAt time.Time
}

func (l *MusicBrainzLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if strings.TrimSpace(song.Title) == "" || strings.TrimSpace(primaryArtistPrefix) == "" {
		return nil, nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(l.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://musicbrainz.org"
	}

	query := fmt.Sprintf(
		`recording:"%s" AND artist:"%s"`,
		musicBrainzQueryTerm(song.Title),
		musicBrainzQueryTerm(primaryArtistPrefix),
	)
	endpoint := baseURL + "/ws/2/recording?fmt=json&limit=5&query=" + url.QueryEscape(query)

	userAgent := strings.TrimSpace(l.UserAgent)
	if userAgent == "" {
		userAgent = "ListenLedger/1.0 (song artist backfill)"
	}

	client := l.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := l.doRequest(ctx, client, endpoint, userAgent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var payload musicBrainzRecordingResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode musicbrainz response: %w", err)
	}

	candidates := make([]TrackCandidate, 0, len(payload.Recordings))
	for _, recording := range payload.Recordings {
		artistNames := musicBrainzArtistNames(recording.ArtistCredit)
		if len(artistNames) == 0 {
			continue
		}

		confidence := musicBrainzConfidence(song, primaryArtistPrefix, recording, artistNames)
		if confidence <= 0 {
			continue
		}

		candidates = append(candidates, TrackCandidate{
			Source:      "musicbrainz_recording",
			Title:       strings.TrimSpace(recording.Title),
			ArtistNames: artistNames,
			ReleaseYear: parseReleaseYear(recording.FirstReleaseDate),
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

	return candidates, nil
}

func (l *MusicBrainzLookup) doRequest(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	maxRetries := l.maxRetries()
	for attempt := 0; ; attempt++ {
		if err := l.waitTurn(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build musicbrainz request: %w", err)
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			if attempt >= maxRetries || ctx.Err() != nil {
				return nil, fmt.Errorf("perform musicbrainz request: %w", err)
			}
			if sleepErr := sleepWithContext(ctx, l.retryDelay(nil, attempt)); sleepErr != nil {
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

func (l *MusicBrainzLookup) handleHTTPRetry(ctx context.Context, resp *http.Response, attempt, maxRetries int) error {
	if !musicBrainzRetryableStatus(resp.StatusCode) || attempt >= maxRetries {
		status := resp.Status
		_ = resp.Body.Close()
		if attempt > 0 {
			return fmt.Errorf("musicbrainz returned %s after %d retries", status, attempt)
		}
		return fmt.Errorf("musicbrainz returned %s", status)
	}
	delay := l.retryDelay(resp, attempt)
	_ = resp.Body.Close()
	if sleepErr := sleepWithContext(ctx, delay); sleepErr != nil {
		return sleepErr
	}
	return nil
}

func (l *MusicBrainzLookup) waitTurn(ctx context.Context) error {
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

func (l *MusicBrainzLookup) minRequestInterval() time.Duration {
	if l.MinRequestInterval < 0 {
		return 0
	}
	if l.MinRequestInterval == 0 {
		return defaultMusicBrainzMinRequestInterval
	}
	return l.MinRequestInterval
}

func (l *MusicBrainzLookup) maxRetries() int {
	if l.MaxRetries < 0 {
		return 0
	}
	if l.MaxRetries == 0 {
		return defaultMusicBrainzMaxRetries
	}
	return l.MaxRetries
}

func (l *MusicBrainzLookup) retryBaseDelay() time.Duration {
	if l.RetryBaseDelay <= 0 {
		return defaultMusicBrainzRetryBaseDelay
	}
	return l.RetryBaseDelay
}

func (l *MusicBrainzLookup) retryDelay(resp *http.Response, attempt int) time.Duration {
	if resp != nil {
		if retryAfter, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return retryAfter
		}
	}
	return cappedExponentialDelay(l.retryBaseDelay(), attempt, 30*time.Second)
}

func cappedExponentialDelay(base time.Duration, attempt int, maxDelay time.Duration) time.Duration {
	delay := base
	for range attempt {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func musicBrainzRetryableStatus(statusCode int) bool {
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

func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := time.Until(when)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type musicBrainzRecordingResponse struct {
	Recordings []musicBrainzRecording `json:"recordings"`
}

type musicBrainzRecording struct {
	Title            string                    `json:"title"`
	FirstReleaseDate string                    `json:"first-release-date"`
	Score            int                       `json:"score"`
	ArtistCredit     []musicBrainzArtistCredit `json:"artist-credit"`
}

type musicBrainzArtistCredit struct {
	Name   string `json:"name"`
	Artist struct {
		Name string `json:"name"`
	} `json:"artist"`
}

func musicBrainzArtistNames(credits []musicBrainzArtistCredit) []string {
	names := make([]string, 0, len(credits))
	seen := map[string]bool{}
	for _, credit := range credits {
		name := strings.TrimSpace(credit.Name)
		if name == "" {
			name = strings.TrimSpace(credit.Artist.Name)
		}
		if name == "" {
			continue
		}
		key := normalizeKey(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		names = append(names, name)
	}
	return names
}

func musicBrainzQueryTerm(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func musicBrainzConfidence(song SongInput, primaryArtistPrefix string, recording musicBrainzRecording, artistNames []string) float64 {
	if normalizeLooseKey(recording.Title) != normalizeLooseKey(song.Title) {
		return 0
	}

	prefixKey := normalizeLooseKey(primaryArtistPrefix)
	if prefixKey == "" {
		return 0
	}

	matchedPrimary := false
	firstMatched := false
	for idx, artistName := range artistNames {
		artistKey := normalizeLooseKey(artistName)
		if artistKey == "" {
			continue
		}
		if strings.HasPrefix(artistKey, prefixKey) || strings.HasPrefix(prefixKey, artistKey) {
			matchedPrimary = true
			if idx == 0 {
				firstMatched = true
			}
			break
		}
	}
	if !matchedPrimary {
		return 0
	}

	confidence := 0.88
	if firstMatched {
		confidence += 0.04
	}

	recordingYear := parseReleaseYear(recording.FirstReleaseDate)
	songYear := parseReleaseYear(song.ReleaseDate)
	confidence += yearMatchAdjustment(recordingYear, songYear, 0.03, 0.05)

	if recording.Score >= 95 {
		confidence += 0.02
	} else if recording.Score < 80 {
		confidence -= 0.03
	}

	if confidence > 0.99 {
		confidence = 0.99
	}
	if confidence < 0 {
		return 0
	}

	return confidence
}
