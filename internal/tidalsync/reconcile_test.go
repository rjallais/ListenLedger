package tidalsync

import (
	"strings"
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

func TestReconcileRelinkNeedsReview(t *testing.T) {
	catalog := []CatalogSong{
		{ID: "song-linked", Title: "Wave", ArtistName: "Antônio Carlos Jobim", TidalID: "tidal-other"},
		{ID: "song-fresh", Title: "Dindi", ArtistName: "Antônio Carlos Jobim"},
	}
	observed := []tidal.ObservedTrack{
		{ID: "tidal-a", Title: "Wave", ArtistNames: []string{"Antônio Carlos Jobim"}, AlbumTitle: "Wave", ReleaseDate: "1967-10-01"},
		{ID: "tidal-b", Title: "Dindi", ArtistNames: []string{"Antônio Carlos Jobim"}, AlbumTitle: "Wave", ReleaseDate: "1967-10-01"},
	}
	linkedAuto, freshAuto := true, false
	for _, p := range Reconcile(observed, catalog, DefaultReconcileOptions()) {
		if p.Kind != "link" {
			continue
		}
		switch p.TargetID {
		case "song-linked":
			linkedAuto = p.AutoApprove
		case "song-fresh":
			freshAuto = p.AutoApprove
		}
	}
	if linkedAuto {
		t.Errorf("relink over an existing tidal_id must not auto-approve")
	}
	if !freshAuto {
		t.Errorf("first-time link above threshold should auto-approve")
	}
}

func TestReconcileCompetingFavoritesNeedReview(t *testing.T) {
	catalog := []CatalogSong{
		{ID: "sg_claim", Title: "Song", ArtistName: "Artist", Album: "Song", ReleaseDate: "2020-03-15", ReleaseType: "album"},
	}
	observed := []tidal.ObservedTrack{
		{ID: "t_album", Title: "Song", ArtistNames: []string{"Artist"}, AlbumTitle: "Song", ReleaseDate: "2020-03-15"},
		{ID: "t_single", Title: "Song", ArtistNames: []string{"Artist"}, AlbumTitle: "Song", ReleaseDate: "2020-03-15"},
	}

	links := make(map[string]ProposalPlan)
	for _, p := range Reconcile(observed, catalog, DefaultReconcileOptions()) {
		if p.Kind == "link" && p.TargetID == "sg_claim" {
			links[p.TidalTrackID] = p
		}
	}
	if len(links) != 2 {
		t.Fatalf("link plans for sg_claim = %d, want 2", len(links))
	}
	if !links["t_album"].AutoApprove {
		t.Errorf("first favorite should auto-approve")
	}
	second := links["t_single"]
	if second.AutoApprove {
		t.Errorf("second favorite matching the same song must need review")
	}
	if !second.RequiresReview {
		t.Errorf("second favorite matching the same song must have RequiresReview=true")
	}
	if !strings.Contains(second.Evidence, "also matched TIDAL track t_album in this run") {
		t.Errorf("evidence = %q, want competing-track note", second.Evidence)
	}
}

// TestReconcileFillNeedsReviewWhenLinkedElsewhere asserts a metadata fill
// for a song already linked to a different TIDAL track stays manual: the
// fuzzy match may not be the same recording, so its metadata must not be
// written without review.
func TestReconcileFillNeedsReviewWhenLinkedElsewhere(t *testing.T) {
	catalog := []CatalogSong{
		{ID: "sg_linked", Title: "Wave", ArtistName: "Antônio Carlos Jobim", TidalID: "tidal-other"},
		{ID: "sg_unlinked", Title: "Dindi", ArtistName: "Antônio Carlos Jobim"},
	}
	observed := []tidal.ObservedTrack{
		{ID: "tidal-a", Title: "Wave", ArtistNames: []string{"Antônio Carlos Jobim"}, AlbumTitle: "Wave"},
		{ID: "tidal-b", Title: "Dindi", ArtistNames: []string{"Antônio Carlos Jobim"}, AlbumTitle: "Wave"},
	}

	linkedFill, unlinkedFill := -1, -1
	linkedRequiresReview := false
	for _, p := range Reconcile(observed, catalog, DefaultReconcileOptions()) {
		if p.Kind != "fill" {
			continue
		}
		switch p.TargetID {
		case "sg_linked":
			linkedFill = boolToInt(p.AutoApprove)
			linkedRequiresReview = p.RequiresReview
		case "sg_unlinked":
			unlinkedFill = boolToInt(p.AutoApprove)
		}
	}
	if linkedFill != 0 {
		t.Errorf("fill for a song linked to another TIDAL track: AutoApprove=%d, want 0 (review)", linkedFill)
	}
	if !linkedRequiresReview {
		t.Errorf("fill for a song linked to another TIDAL track: RequiresReview=false, want true")
	}
	if unlinkedFill != 1 {
		t.Errorf("fill for an unlinked song: AutoApprove=%d, want 1", unlinkedFill)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestReconcileCanonicalPrefersMatchQuality(t *testing.T) {
	catalog := []CatalogSong{
		{ID: "sg_go", Title: "Go", ArtistName: "Artist"},
		{ID: "sg_go_live", Title: "Go (Live)", ArtistName: "Artist", SpotifyID: "spot-live"},
	}
	observed := []tidal.ObservedTrack{
		{ID: "t_go", Title: "Go", ArtistNames: []string{"Artist"}},
	}
	var linkTarget, dupTarget string
	for _, p := range Reconcile(observed, catalog, DefaultReconcileOptions()) {
		switch p.Kind {
		case "link":
			linkTarget = p.TargetID
		case "duplicate":
			dupTarget = p.TargetID
		}
	}
	if linkTarget != "sg_go" {
		t.Errorf("link target = %q, want exact-match song sg_go", linkTarget)
	}
	if dupTarget != "" {
		t.Errorf("substring match %q must not be flagged as duplicate", dupTarget)
	}

	// An exact_tidal_id match (1.0) must not be displaced by a fuzzy match
	// carrying a Spotify ID (0.95): the already-linked song stays canonical
	// and no proposals fire.
	catalog = []CatalogSong{
		{ID: "sg_linked", Title: "Wave", ArtistName: "Jobim", TidalID: "t_wave"},
		{ID: "sg_other", Title: "Wave", ArtistName: "Jobim", SpotifyID: "spot-x"},
	}
	observed = []tidal.ObservedTrack{
		{ID: "t_wave", Title: "Wave", ArtistNames: []string{"Jobim"}},
	}
	if got := Reconcile(observed, catalog, DefaultReconcileOptions()); len(got) != 0 {
		t.Errorf("expected no proposals for an already-linked exact match, got %d", len(got))
	}
}

func TestGenerateSyntheticSongIDPocketBaseSafe(t *testing.T) {
	id := generateSyntheticSongID("tidal-1", "Wave")
	if len(id) != 15 {
		t.Fatalf("id = %q, want 15 chars", id)
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("id %q contains char outside PocketBase's [a-z0-9] pattern", id)
		}
	}
}
