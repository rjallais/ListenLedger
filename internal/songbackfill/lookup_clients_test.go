// Package songbackfill provides utilities to parse and backfill song metadata with artist information.
package songbackfill

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if attempts.Load() < 3 {
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
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
}

func TestMusicBrainzLookupReturnsRetryCountOnPersistent503(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
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
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
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
		if got := r.URL.Path; got != "/searchResults" {
			t.Fatalf("Path = %q, want /searchResults", got)
		}
		if got := r.URL.Query().Get("filter[query]"); got != "Nights Like This" {
			t.Fatalf("filter[query] = %q, want Nights Like This", got)
		}
		if got := r.URL.Query().Get("explicitFilter"); got != "INCLUDE" {
			t.Fatalf("explicitFilter = %q, want INCLUDE", got)
		}
		if got := r.URL.Query().Get("countryCode"); got != "US" {
			t.Fatalf("countryCode = %q, want US", got)
		}
		// Dotted includes embed each track's artist/album refs; a flat
		// include returns three unlinked collections.
		if includes := r.URL.Query()["include"]; len(includes) != 1 || includes[0] != "tracks.artists,tracks.albums" {
			t.Fatalf("include = %v, want [tracks.artists,tracks.albums]", includes)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-123" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.api+json" {
			t.Fatalf("Accept = %q, want application/vnd.api+json", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"id":   "Nights Like This",
					"type": "searchResults",
					"relationships": map[string]any{
						"tracks": map[string]any{
							"data": []map[string]any{{"id": "251380837", "type": "tracks"}},
						},
					},
				},
			},
			"included": []map[string]any{
				{
					"type": "tracks",
					"id":   "251380837",
					"attributes": map[string]any{
						"title": "Nights Like This",
					},
					"relationships": map[string]any{
						"artists": map[string]any{
							"data": []map[string]any{{"id": "1", "type": "artists"}, {"id": "2", "type": "artists"}},
						},
						"albums": map[string]any{
							"data": []map[string]any{{"id": "77148219", "type": "albums"}},
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
				{
					"type": "albums",
					"id":   "77148219",
					"attributes": map[string]any{
						"title":       "While We Wait - EP",
						"releaseDate": "2019-02-22",
						// The suffix wins over the attribute when both carry it.
						"type": "EP",
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
	if candidates[0].AlbumName != "While We Wait" {
		t.Fatalf("AlbumName = %q, want While We Wait", candidates[0].AlbumName)
	}
	// The distributor suffix becomes the release type, not part of the name.
	if candidates[0].ReleaseType != "ep" {
		t.Fatalf("ReleaseType = %q, want ep", candidates[0].ReleaseType)
	}
	// Album release date wins over the track's.
	if candidates[0].ReleaseDate != "2019-02-22" {
		t.Fatalf("ReleaseDate = %q, want 2019-02-22", candidates[0].ReleaseDate)
	}
	if candidates[0].ReleaseYear != 2019 {
		t.Fatalf("ReleaseYear = %d, want 2019", candidates[0].ReleaseYear)
	}
}

// A silent 429 (no Retry-After) is a hard quota wall: it must fail fast so
// the resumable run can finish, not sleep against a window that will not
// reopen mid-run.
func TestTidalLookupTreatsSilent429AsTerminal(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	lookup := &TidalLookup{
		BaseURL:            server.URL,
		HTTPClient:         &http.Client{Timeout: 2 * time.Second},
		CountryCode:        "US",
		AuthToken:          "token-123",
		MinRequestInterval: -1,
		MaxRetries:         2,
		RetryBaseDelay:     time.Millisecond,
	}

	_, err := lookup.Lookup(t.Context(), SongInput{Title: "Nights Like This"}, "Kehlani")
	if err == nil {
		t.Fatalf("Lookup() error = nil, want failure")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (no retries against a silent quota wall)", got)
	}
	if !strings.Contains(err.Error(), "429 Too Many Requests") {
		t.Fatalf("error = %q, want the 429 status", err.Error())
	}
}

// A 429 carrying a bounded Retry-After is a transient window: wait it out.
func TestTidalLookupRetriesRetryAfter429(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if attempts.Load() < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "included": []map[string]any{}})
	}))
	defer server.Close()

	lookup := &TidalLookup{
		BaseURL:            server.URL,
		HTTPClient:         &http.Client{Timeout: 2 * time.Second},
		CountryCode:        "US",
		AuthToken:          "token-123",
		MinRequestInterval: -1,
		MaxRetries:         3,
		RetryBaseDelay:     time.Millisecond,
	}

	if _, err := lookup.Lookup(t.Context(), SongInput{Title: "Nights Like This"}, "Kehlani"); err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

// Transient 5xx backs off and retries without needing a Retry-After header.
func TestTidalLookupRetriesTransient503(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if attempts.Load() < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "included": []map[string]any{}})
	}))
	defer server.Close()

	lookup := &TidalLookup{
		BaseURL:            server.URL,
		HTTPClient:         &http.Client{Timeout: 2 * time.Second},
		CountryCode:        "US",
		AuthToken:          "token-123",
		MinRequestInterval: -1,
		MaxRetries:         3,
		RetryBaseDelay:     time.Millisecond,
	}

	if _, err := lookup.Lookup(t.Context(), SongInput{Title: "Nights Like This"}, "Kehlani"); err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

// An empty search (200, no data) is not an error: callers fall through to
// unresolved or the PocketBase-label path.
func TestTidalLookupEmptyResults(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "included": []map[string]any{}})
	}))
	defer server.Close()

	lookup := &TidalLookup{
		BaseURL:     server.URL,
		HTTPClient:  &http.Client{Timeout: 2 * time.Second},
		CountryCode: "US",
		AuthToken:   "token-123",
	}

	candidates, err := lookup.Lookup(t.Context(), SongInput{Title: "Nonexistent Song Xyzzy"}, "Nobody")
	if err != nil {
		t.Fatalf("Lookup() error = %v, want nil", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("len(candidates) = %d, want 0", len(candidates))
	}
}

// Most album titles carry no " - Single" suffix, so the album's own type
// attribute ("ALBUM", "SINGLE", ...) is the fallback release-type carrier.
func TestTidalLookupFallsBackToAlbumTypeAttribute(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"id":   "Dirt",
					"type": "searchResults",
					"relationships": map[string]any{
						"tracks": map[string]any{
							"data": []map[string]any{{"id": "9", "type": "tracks"}},
						},
					},
				},
			},
			"included": []map[string]any{
				{
					"type": "tracks",
					"id":   "9",
					"attributes": map[string]any{
						"title": "Dirt",
					},
					"relationships": map[string]any{
						"artists": map[string]any{
							"data": []map[string]any{{"id": "1", "type": "artists"}},
						},
						"albums": map[string]any{
							"data": []map[string]any{{"id": "10", "type": "albums"}},
						},
					},
				},
				{
					"type":       "artists",
					"id":         "1",
					"attributes": map[string]any{"name": "Alice in Chains"},
				},
				{
					"type": "albums",
					"id":   "10",
					"attributes": map[string]any{
						"title":       "Dirt",
						"releaseDate": "1992-09-29",
						"type":        "ALBUM",
					},
				},
			},
		})
	}))
	defer server.Close()

	lookup := &TidalLookup{
		BaseURL:            server.URL,
		HTTPClient:         &http.Client{Timeout: 2 * time.Second},
		CountryCode:        "US",
		AuthToken:          "token-123",
		MinRequestInterval: -1,
	}

	candidates, err := lookup.Lookup(t.Context(), SongInput{Title: "Dirt", ArtistName: "Alice in Chains"}, "Alice in Chains")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if candidates[0].ReleaseType != "album" {
		t.Fatalf("ReleaseType = %q, want album (from the album type attribute)", candidates[0].ReleaseType)
	}
	if candidates[0].AlbumName != "Dirt" {
		t.Fatalf("AlbumName = %q, want Dirt", candidates[0].AlbumName)
	}
	if candidates[0].ReleaseYear != 1992 {
		t.Fatalf("ReleaseYear = %d, want 1992", candidates[0].ReleaseYear)
	}
}

// TIDAL IDs are numeric per type, so an album can share a track's ID: the
// track lookup must not let a later same-ID album overwrite the track entry.
func TestTidalIncludedTrackItemsSkipsSameIDNonTracks(t *testing.T) {
	t.Parallel()

	rels := tidalSearchRelationships{}
	rels.Tracks.Data = []tidalRelationshipRef{{ID: "42", Type: "tracks"}}
	payload := tidalSearchResponse{
		Data: []tidalSearchData{{Relationships: rels}},
		Included: []tidalSearchItem{
			{Type: "tracks", ID: "42"},
			{Type: "albums", ID: "42"},
		},
	}

	items := tidalIncludedTrackItems(payload)
	if len(items) != 1 || items[0].Type != "tracks" {
		t.Fatalf("tidalIncludedTrackItems = %+v, want the track despite the same-ID album", items)
	}
}

func TestSplitReleaseTypeSuffix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		title    string
		wantName string
		wantType string
	}{
		{title: "Ylang Ylang - EP", wantName: "Ylang Ylang", wantType: "ep"},
		{title: "them bones - single", wantName: "them bones", wantType: "single"},
		{title: "Ylang Ylang EP", wantName: "Ylang Ylang EP", wantType: ""},
		{title: "Dirt", wantName: "Dirt", wantType: ""},
		// A bare suffix is no album title: passthrough, no type.
		{title: " - Single", wantName: "- Single", wantType: ""},
	} {
		name, releaseType := splitReleaseTypeSuffix(tc.title)
		if name != tc.wantName || releaseType != tc.wantType {
			t.Fatalf("splitReleaseTypeSuffix(%q) = (%q, %q), want (%q, %q)", tc.title, name, releaseType, tc.wantName, tc.wantType)
		}
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
