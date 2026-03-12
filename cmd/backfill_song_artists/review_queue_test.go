//go:build goexperiment.jsonv2

package main

import (
	"testing"

	"ListenLedger/internal/songbackfill"
)

func TestBuildReviewItemCategorizesMissingArtistRecord(t *testing.T) {
	t.Parallel()

	item, ok := buildReviewItem(songbackfill.Resolution{
		SongID:             "song-1",
		Title:              "Spaceship",
		OriginalArtistName: "Benny Benassi, …",
		Action:             songbackfill.ActionSkipUnresolved,
		ExternalCandidates: []songbackfill.CandidateSummary{
			{
				Source:      "deezer_track",
				Title:       "Spaceship (feat. Kelis, apl.de.ap & Jean Baptiste) (Radio Edit)",
				ArtistNames: []string{"Benny Benassi", "Kelis", "apl.de.ap", "Jean Baptiste"},
				Confidence:  0.95,
			},
		},
		Notes: []string{
			`selected "Spaceship (feat. Kelis, apl.de.ap & Jean Baptiste) (Radio Edit)" from deezer_track with confidence 0.95`,
			`artist "Jean Baptiste" did not match an existing artist record`,
		},
	}, []songbackfill.ArtistInput{})
	if !ok {
		t.Fatalf("buildReviewItem() returned ok=false")
	}
	if item.Category != "missing_artist_record" {
		t.Fatalf("Category = %q, want missing_artist_record", item.Category)
	}
	if len(item.MissingArtistNames) != 1 || item.MissingArtistNames[0] != "Jean Baptiste" {
		t.Fatalf("MissingArtistNames = %#v", item.MissingArtistNames)
	}
	if got := len(item.SuggestedArtistNames); got != 4 {
		t.Fatalf("len(SuggestedArtistNames) = %d, want 4", got)
	}
}

func TestBuildReviewItemCategorizesAmbiguousCredit(t *testing.T) {
	t.Parallel()

	item, ok := buildReviewItem(songbackfill.Resolution{
		Title:              "Complicated",
		OriginalArtistName: "Dimitri Vegas, ...",
		Action:             songbackfill.ActionSkipAmbiguous,
		Notes:              []string{`external lookup returned multiple competing artist lists near confidence 0.98`},
	}, nil)
	if !ok {
		t.Fatalf("buildReviewItem() returned ok=false")
	}
	if item.Category != "ambiguous_external_credit" {
		t.Fatalf("Category = %q, want ambiguous_external_credit", item.Category)
	}
}

func TestSuggestExistingArtistsFindsLooseAlias(t *testing.T) {
	t.Parallel()

	suggestions := suggestExistingArtists("Dark Tranquillity", []songbackfill.ArtistInput{
		{Name: "Dark Tranquility", SpotifyID: "abc"},
		{Name: "Dark Funeral", SpotifyID: "def"},
	})
	if len(suggestions) == 0 {
		t.Fatalf("suggestExistingArtists() returned no suggestions")
	}
	if suggestions[0].SuggestedName != "Dark Tranquility" {
		t.Fatalf("SuggestedName = %q, want Dark Tranquility", suggestions[0].SuggestedName)
	}
}

func TestBuildReviewItemCategorizesBorderlineTidalPrefill(t *testing.T) {
	t.Parallel()

	item, ok := buildReviewItem(songbackfill.Resolution{
		SongID:             "song-tidal-1",
		Title:              "Complicated",
		OriginalArtistName: "Dimitri Vegas, ...",
		Action:             songbackfill.ActionSkipAmbiguous,
		ExternalCandidates: []songbackfill.CandidateSummary{{
			Source:      "tidal_track",
			Title:       "Complicated",
			ArtistNames: []string{"Dimitri Vegas", "David Guetta", "Kiiara"},
			Confidence:  0.86,
		}},
		Notes: []string{"tidal prefill candidate requires manual review before updating artist_name"},
	}, nil)
	if !ok {
		t.Fatalf("buildReviewItem() returned ok=false")
	}
	if item.Category != "ambiguous_tidal_prefill" {
		t.Fatalf("Category = %q, want ambiguous_tidal_prefill", item.Category)
	}
}
