// Package songbackfill provides utilities to parse and backfill song metadata with artist information.
package songbackfill

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMusicBrainzLookupParsesCandidates(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/2/recording" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("fmt"); got != "json" {
			t.Fatalf("fmt = %q, want json", got)
		}
		if got := r.URL.Query().Get("limit"); got != "5" {
			t.Fatalf("limit = %q, want 5", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"recordings": []map[string]any{
				{
					"title":              "Nights Like This",
					"first-release-date": "2019-01-10",
					"score":              100,
					"artist-credit": []map[string]any{
						{"name": "Kehlani"},
						{"name": "Ty Dolla $ign"},
					},
				},
			},
		})
	}))
	defer server.Close()

	lookup := &MusicBrainzLookup{
		BaseURL:    server.URL,
		HTTPClient: &http.Client{Timeout: 2 * time.Second},
	}

	candidates, err := lookup.Lookup(t.Context(), SongInput{
		Title:       "Nights Like This",
		ReleaseDate: "2019-01-10",
	}, "Kehlani")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if len(candidates[0].ArtistNames) < 2 {
		t.Fatalf("len(ArtistNames) = %d, want >= 2", len(candidates[0].ArtistNames))
	}
	if candidates[0].ArtistNames[0] != "Kehlani" || candidates[0].ArtistNames[1] != "Ty Dolla $ign" {
		t.Fatalf("ArtistNames = %#v", candidates[0].ArtistNames)
	}
	if candidates[0].Confidence < 0.90 {
		t.Fatalf("Confidence = %f, want >= 0.90", candidates[0].Confidence)
	}
}

func TestMusicBrainzLookupRetriesTransient503(t *testing.T) {
	t.Parallel()

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"recordings": []map[string]any{
				{
					"title":              "Nights Like This",
					"first-release-date": "2019-01-10",
					"score":              100,
					"artist-credit": []map[string]any{
						{"name": "Kehlani"},
						{"name": "Ty Dolla $ign"},
					},
				},
			},
		})
	}))
	defer server.Close()

	lookup := &MusicBrainzLookup{
		BaseURL:            server.URL,
		HTTPClient:         &http.Client{Timeout: 2 * time.Second},
		MinRequestInterval: -1,
		MaxRetries:         3,
		RetryBaseDelay:     time.Millisecond,
	}

	candidates, err := lookup.Lookup(t.Context(), SongInput{
		Title:       "Nights Like This",
		ReleaseDate: "2019-01-10",
	}, "Kehlani")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
}

func TestMusicBrainzLookupReturnsRetryCountOnPersistent503(t *testing.T) {
	t.Parallel()

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	lookup := &MusicBrainzLookup{
		BaseURL:            server.URL,
		HTTPClient:         &http.Client{Timeout: 2 * time.Second},
		MinRequestInterval: -1,
		MaxRetries:         2,
		RetryBaseDelay:     time.Millisecond,
	}

	_, err := lookup.Lookup(t.Context(), SongInput{
		Title:       "Nights Like This",
		ReleaseDate: "2019-01-10",
	}, "Kehlani")
	if err == nil {
		t.Fatalf("Lookup() error = nil, want failure")
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if !strings.Contains(err.Error(), "after 2 retries") {
		t.Fatalf("error = %q, want retry count", err.Error())
	}
}

func TestChainTrackLookupFallsBackToDeezer(t *testing.T) {
	t.Parallel()

	lookup := ChainTrackLookup{Lookups: []TrackMetadataLookup{
		fakeLookup{},
		fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "deezer_track",
				Title:       "Bang My Head (feat. Sia & Fetty Wap)",
				ArtistNames: []string{"David Guetta", "Fetty Wap", "Sia"},
				Confidence:  0.94,
			},
		}},
	}}

	candidates, err := lookup.Lookup(t.Context(), SongInput{
		Title: "Bang My Head",
	}, "David Guetta")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if candidates[0].Source != "deezer_track" {
		t.Fatalf("Source = %q, want deezer_track", candidates[0].Source)
	}
}

func TestTidalLookupParsesTrackArtists(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/searchResults/Nights Like This" {
			t.Fatalf("Path = %q, want /searchResults/Nights Like This", got)
		}
		if got := r.URL.Query().Get("explicitFilter"); got != "INCLUDE" {
			t.Fatalf("explicitFilter = %q, want INCLUDE", got)
		}
		if got := r.URL.Query().Get("countryCode"); got != "US" {
			t.Fatalf("countryCode = %q, want US", got)
		}
		includes := r.URL.Query()["include"]
		if len(includes) != 2 || includes[0] != "tracks" || includes[1] != "artists" {
			t.Fatalf("include = %v, want [tracks artists]", includes)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-123" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.api+json" {
			t.Fatalf("Accept = %q, want application/vnd.api+json", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"id":   "Nights Like This",
				"type": "searchResults",
				"relationships": map[string]any{
					"tracks": map[string]any{
						"data": []map[string]any{{"id": "251380837", "type": "tracks"}},
					},
				},
			},
			"included": []map[string]any{
				{
					"type": "TRACK",
					"id":   "251380837",
					"attributes": map[string]any{
						"title":       "Nights Like This",
						"releaseDate": "2019-01-10",
					},
					"relationships": map[string]any{
						"artists": map[string]any{
							"data": []map[string]any{{"id": "1", "type": "artists"}, {"id": "2", "type": "artists"}},
						},
					},
				},
				{
					"type": "artists",
					"id":   "1",
					"attributes": map[string]any{
						"name": "Kehlani",
					},
				},
				{
					"type": "artists",
					"id":   "2",
					"attributes": map[string]any{
						"name": "Ty Dolla $ign",
					},
				},
			},
		})
	}))
	defer server.Close()

	lookup := &TidalLookup{
		BaseURL:     server.URL,
		HTTPClient:  &http.Client{Timeout: 2 * time.Second},
		CountryCode: "US",
		AuthToken:   "token-123",
	}

	candidates, err := lookup.Lookup(t.Context(), SongInput{Title: "Nights Like This", ReleaseDate: "2019-01-10", ArtistName: "Kehlani, ..."}, "Kehlani")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if got := strings.Join(candidates[0].ArtistNames, ", "); got != "Kehlani, Ty Dolla $ign" {
		t.Fatalf("ArtistNames = %q", got)
	}
	if candidates[0].Source != "tidal_track" {
		t.Fatalf("Source = %q, want tidal_track", candidates[0].Source)
	}
}

func TestDeezerLookupParsesContributors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search/track":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": 110663020},
				},
			})
		case "/track/110663020":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"title":        "Bang My Head (feat. Sia & Fetty Wap)",
				"release_date": "2015-11-06",
				"artist": map[string]any{
					"name": "David Guetta",
				},
				"contributors": []map[string]any{
					{"name": "David Guetta", "role": "Main"},
					{"name": "Fetty Wap", "role": "Featured"},
					{"name": "Sia", "role": "Featured"},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	lookup := &DeezerLookup{
		BaseURL:    server.URL,
		HTTPClient: &http.Client{Timeout: 2 * time.Second},
	}

	candidates, err := lookup.Lookup(t.Context(), SongInput{
		Title:       "Bang My Head",
		ReleaseDate: "2015-11-06",
	}, "David Guetta")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if got := strings.Join(candidates[0].ArtistNames, ", "); got != "David Guetta, Fetty Wap, Sia" {
		t.Fatalf("ArtistNames = %q", got)
	}
	if candidates[0].Confidence < 0.90 {
		t.Fatalf("Confidence = %f, want >= 0.90", candidates[0].Confidence)
	}
}
