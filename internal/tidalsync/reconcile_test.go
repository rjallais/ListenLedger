package tidalsync

import (
	"testing"

	"ListenLedger/internal/tidal"
)

func TestReconcileScenarios(t *testing.T) {
	catalog := []CatalogSong{
		{
			ID:          "song-fill",
			Title:       "Garota de Ipanema",
			ArtistName:  "Stan Getz, João Gilberto",
			Album:       "", // missing
			ReleaseDate: "", // missing
			ReleaseType: "", // missing
			SpotifyID:   "spot-1",
		},
		{
			ID:          "song-suffix",
			Title:       "Chega de Saudade",
			ArtistName:  "João Gilberto",
			Album:       "Chega de Saudade - Single",
			ReleaseDate: "1959-01-01",
			ReleaseType: "single",
			SpotifyID:   "spot-2",
		},
		{
			ID:          "song-ellipsis",
			Title:       "Corcovado",
			ArtistName:  "Stan Getz, ...",
			Album:       "Getz/Gilberto",
			ReleaseDate: "1964-03-01",
			ReleaseType: "album",
			SpotifyID:   "spot-3",
		},
		{
			ID:          "song-dup-1",
			Title:       "Desafinado",
			ArtistName:  "João Gilberto",
			Album:       "Chega de Saudade",
			ReleaseDate: "1959-01-01",
			ReleaseType: "album",
			SpotifyID:   "spot-4",
		},
		{
			ID:          "song-dup-2",
			Title:       "Desafinado",
			ArtistName:  "João Gilberto",
			Album:       "",
			ReleaseDate: "",
			SpotifyID:   "",
		},
	}

	observed := []tidal.ObservedTrack{
		{
			ID:          "tidal-1",
			Title:       "Garota de Ipanema",
			ArtistNames: []string{"Stan Getz", "João Gilberto", "Astrud Gilberto"},
			AlbumTitle:  "Getz/Gilberto",
			ReleaseDate: "1964-03-01",
		},
		{
			ID:          "tidal-2",
			Title:       "Chega de Saudade",
			ArtistNames: []string{"João Gilberto"},
			AlbumTitle:  "Chega de Saudade", // clean
			ReleaseDate: "1959-01-01",
		},
		{
			ID:          "tidal-3",
			Title:       "Corcovado",
			ArtistNames: []string{"Stan Getz", "João Gilberto", "Astrud Gilberto"},
			AlbumTitle:  "Getz/Gilberto",
			ReleaseDate: "1964-03-01",
		},
		{
			ID:          "tidal-4",
			Title:       "Desafinado",
			ArtistNames: []string{"João Gilberto"},
			AlbumTitle:  "Chega de Saudade",
			ReleaseDate: "1959-01-01",
		},
		{
			ID:          "tidal-new",
			Title:       "Wave",
			ArtistNames: []string{"Antônio Carlos Jobim"},
			AlbumTitle:  "Wave",
			ReleaseDate: "1967-10-01",
		},
	}

	plans := Reconcile(observed, catalog, DefaultReconcileOptions())

	kinds := make(map[string]int)
	for _, p := range plans {
		kinds[p.Kind]++
	}

	if kinds["fill"] == 0 {
		t.Errorf("expected 'fill' proposal, got none")
	}
	if kinds["album_name"] == 0 {
		t.Errorf("expected 'album_name' proposal, got none")
	}
	if kinds["credit"] == 0 {
		t.Errorf("expected 'credit' proposal, got none")
	}
	if kinds["addition"] == 0 {
		t.Errorf("expected 'addition' proposal, got none")
	}
	if kinds["duplicate"] == 0 {
		t.Errorf("expected 'duplicate' proposal, got none")
	}
	if kinds["link"] == 0 {
		t.Errorf("expected 'link' proposal, got none")
	}

	// Verify fill plan auto-approve is true
	for _, p := range plans {
		if p.Kind == "fill" && !p.AutoApprove {
			t.Errorf("expected fill proposal to have AutoApprove=true")
		}
		if p.Kind == "addition" && p.AutoApprove {
			t.Errorf("expected addition proposal to have AutoApprove=false")
		}
	}
}
