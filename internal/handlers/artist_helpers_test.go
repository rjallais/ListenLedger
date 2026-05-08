//go:build goexperiment.jsonv2

package handlers

import (
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseArtistCreateInputDefaults(t *testing.T) {
	form := url.Values{
		"name":       {"Black Sabbath"},
		"spotify_id": {"artist-1"},
	}

	req := httptest.NewRequest("POST", "/artists", nil)
	req.Form = form
	req.PostForm = form

	input, err := parseArtistCreateInput(req)
	if err != nil {
		t.Fatalf("parseArtistCreateInput() error = %v", err)
	}

	if input.name != "Black Sabbath" {
		t.Fatalf("name = %q, want %q", input.name, "Black Sabbath")
	}
	if input.spotifyID != "artist-1" {
		t.Fatalf("spotifyID = %q, want %q", input.spotifyID, "artist-1")
	}
	if input.genreGroup != defaultArtistGenreGroup {
		t.Fatalf("genreGroup = %q, want %q", input.genreGroup, defaultArtistGenreGroup)
	}
	if input.listStatus != defaultArtistListStatus {
		t.Fatalf("listStatus = %q, want %q", input.listStatus, defaultArtistListStatus)
	}
	if input.monthlyListeners != 0 {
		t.Fatalf("monthlyListeners = %d, want 0", input.monthlyListeners)
	}
	if input.collectionSongs != 0 {
		t.Fatalf("collectionSongs = %d, want 0", input.collectionSongs)
	}
}

func TestParseArtistCreateInputRejectsInvalidGenre(t *testing.T) {
	form := url.Values{
		"name":        {"Black Sabbath"},
		"spotify_id":  {"artist-1"},
		"genre_group": {"pop"},
	}

	req := httptest.NewRequest("POST", "/artists", nil)
	req.Form = form
	req.PostForm = form

	_, err := parseArtistCreateInput(req)
	if err == nil || err.Error() != "genre_group must be rock_metal or everything_else" {
		t.Fatalf("parseArtistCreateInput() error = %v, want invalid genre error", err)
	}
}

func TestParseArtistListParamsAppliesDefaultsAndBounds(t *testing.T) {
	req := httptest.NewRequest("GET", "/artists?page=0&limit=999&genre=unknown", nil)

	params := parseArtistListParams(req)
	if params.page != defaultArtistPage {
		t.Fatalf("page = %d, want %d", params.page, defaultArtistPage)
	}
	// limit=999 exceeds maxArtistPageSize; parseBoundedPositiveInt clamps to max.
	if params.limit != maxArtistPageSize {
		t.Fatalf("limit = %d, want %d (clamped to max)", params.limit, maxArtistPageSize)
	}
	if params.genre != defaultArtistGenreGroup {
		t.Fatalf("genre = %q, want %q", params.genre, defaultArtistGenreGroup)
	}
}

func TestRankedArtistTotalSongsUsesReverseGlobalIndex(t *testing.T) {
	tests := []struct {
		name       string
		totalCount int
		offset     int
		index      int
		want       int
	}{
		{
			name:       "first artist has highest max",
			totalCount: 100,
			offset:     0,
			index:      0,
			want:       100,
		},
		{
			name:       "next page continues reverse index",
			totalCount: 100,
			offset:     50,
			index:      0,
			want:       50,
		},
		{
			name:       "last artist has max one",
			totalCount: 100,
			offset:     99,
			index:      0,
			want:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rankedArtistTotalSongs(tt.totalCount, tt.offset, tt.index)
			if got != tt.want {
				t.Fatalf("rankedArtistTotalSongs(%d, %d, %d) = %d, want %d", tt.totalCount, tt.offset, tt.index, got, tt.want)
			}
		})
	}
}

func TestParseWaitingArtistListParamsAppliesDefaultsAndBounds(t *testing.T) {
	req := httptest.NewRequest("GET", "/artists/waiting?offset=-3&limit=25", nil)

	params := parseWaitingArtistListParams(req)
	if params.offset != 0 {
		t.Fatalf("offset = %d, want 0", params.offset)
	}
	// limit=25 exceeds maxWaitingArtistPageSize; parseBoundedPositiveInt clamps to max.
	if params.limit != maxWaitingArtistPageSize {
		t.Fatalf("limit = %d, want %d (clamped to max)", params.limit, maxWaitingArtistPageSize)
	}
}

func TestParseBatchRefreshCountRequiresPositiveInteger(t *testing.T) {
	req := httptest.NewRequest("POST", "/artists/refresh/batch", nil)
	req.Form = url.Values{"count": {"0"}}
	req.PostForm = req.Form

	_, err := parseBatchRefreshCount(req)
	if err == nil || err.Error() != "count must be a positive integer" {
		t.Fatalf("parseBatchRefreshCount() error = %v, want positive integer error", err)
	}
}

func TestParseSongCountAction(t *testing.T) {
	inc, err := parseSongCountAction("inc")
	if err != nil || inc != 1 {
		t.Fatalf("parseSongCountAction(inc) = (%d, %v), want (1, nil)", inc, err)
	}

	dec, err := parseSongCountAction("dec")
	if err != nil || dec != -1 {
		t.Fatalf("parseSongCountAction(dec) = (%d, %v), want (-1, nil)", dec, err)
	}

	_, err = parseSongCountAction("noop")
	if err == nil || err.Error() != "action must be 'inc' or 'dec'" {
		t.Fatalf("parseSongCountAction(noop) error = %v, want invalid action error", err)
	}
}
