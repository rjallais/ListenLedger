package tidal

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchFavoriteTracks(t *testing.T) {
	ctx := t.Context()

	var pageCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		w.Header().Set("Content-Type", "application/vnd.api+json")

		if pageCount == 1 {
			resp := map[string]any{
				"data": []map[string]any{
					{
						"id":   "track-1",
						"type": "tracks",
						"meta": map[string]any{
							"addedAt": "2024-05-01T12:00:00Z",
						},
					},
				},
				"included": []map[string]any{
					{
						"id":   "track-1",
						"type": "tracks",
						"attributes": map[string]any{
							"title":       "Samba de Verão",
							"isrc":        "BR1234567890",
							"releaseDate": "1964-01-01",
							"duration":    180,
						},
						"relationships": map[string]any{
							"artists": map[string]any{
								"data": []map[string]any{
									{"id": "artist-1", "type": "artists"},
								},
							},
							"albums": map[string]any{
								"data": []map[string]any{
									{"id": "album-1", "type": "albums"},
								},
							},
						},
					},
					{
						"id":   "artist-1",
						"type": "artists",
						"attributes": map[string]any{
							"name": "Marcos Valle",
						},
					},
					{
						"id":   "album-1",
						"type": "albums",
						"attributes": map[string]any{
							"title":       "Samba '68",
							"releaseDate": "1968-01-01",
						},
					},
				},
				"links": map[string]any{
					"next": "",
				},
			}
			_ = json.MarshalWrite(w, resp)
			return
		}

		http.Error(w, "unexpected call", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient("test-access-token")
	client.BaseURL = server.URL
	client.HTTPClient = server.Client()

	tracks, err := client.FetchFavoriteTracks(ctx, "user-42", 10)
	if err != nil {
		t.Fatalf("FetchFavoriteTracks() error: %v", err)
	}

	if len(tracks) != 1 {
		t.Fatalf("expected 1 track, got %d", len(tracks))
	}

	tr := tracks[0]
	if tr.ID != "track-1" {
		t.Errorf("expected ID 'track-1', got %q", tr.ID)
	}
	if tr.Title != "Samba de Verão" {
		t.Errorf("expected title 'Samba de Verão', got %q", tr.Title)
	}
	if tr.ISRC != "BR1234567890" {
		t.Errorf("expected ISRC 'BR1234567890', got %q", tr.ISRC)
	}
	if len(tr.ArtistNames) != 1 || tr.ArtistNames[0] != "Marcos Valle" {
		t.Errorf("unexpected artists: %v", tr.ArtistNames)
	}
	if tr.AlbumTitle != "Samba '68" {
		t.Errorf("expected album 'Samba '68', got %q", tr.AlbumTitle)
	}
	if tr.AddedAt.IsZero() {
		t.Errorf("expected AddedAt parsed, got zero time")
	}
}
