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

func TestSelectCandidateForReviewMatchesCanonicalFilterNote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		note string
	}{
		{
			name: "canonical variant filtering",
			note: `selected "Spaceship" from musicbrainz with confidence 0.95 after canonical variant filtering`,
		},
		{
			name: "featured-track canonical filtering",
			note: `selected "Spaceship" from deezer_track with confidence 0.88 after featured-track canonical filtering`,
		},
		{
			name: "corroboration suffix",
			note: `selected "Spaceship" from tidal_track with confidence 0.90 after corroboration from 2 sources`,
		},
		{
			name: "plain (no suffix)",
			note: `selected "Spaceship" from musicbrainz with confidence 0.95`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolution := songbackfill.Resolution{
				Action: songbackfill.ActionSkipUnresolved,
				ExternalCandidates: []songbackfill.CandidateSummary{
					{Source: "wrong_source", Title: "Wrong", Confidence: 0.99},
					{Source: "musicbrainz", Title: "Spaceship", Confidence: 0.95},
					{Source: "deezer_track", Title: "Spaceship", Confidence: 0.88},
					{Source: "tidal_track", Title: "Spaceship", Confidence: 0.90},
				},
				Notes: []string{tt.note},
			}

			got := selectCandidateForReview(resolution)
			if got == nil {
				t.Fatalf("selectCandidateForReview() returned nil")
			}
			if got.Title != "Spaceship" {
				t.Fatalf("selectCandidateForReview().Title = %q, want Spaceship", got.Title)
			}
			if got.Source == "wrong_source" {
				t.Fatalf("selectCandidateForReview() picked wrong_source instead of the resolver-chosen candidate")
			}
		})
	}
}

func TestSelectCandidateForReviewDisambiguatesByConfidence(t *testing.T) {
	t.Parallel()

	// Two candidates share (source, title) but have different confidence and artist lists.
	// The resolver note pins confidence 0.88, so selectCandidateForReview must return the
	// candidate with confidence 0.88, not the one with 0.95.
	resolution := songbackfill.Resolution{
		Action: songbackfill.ActionSkipUnresolved,
		ExternalCandidates: []songbackfill.CandidateSummary{
			{Source: "deezer_track", Title: "Spaceship", ArtistNames: []string{"Benny Benassi"}, Confidence: 0.95},
			{Source: "deezer_track", Title: "Spaceship", ArtistNames: []string{"Benny Benassi", "Kelis"}, Confidence: 0.88},
		},
		Notes: []string{
			`selected "Spaceship" from deezer_track with confidence 0.88 after canonical variant filtering`,
		},
	}

	got := selectCandidateForReview(resolution)
	if got == nil {
		t.Fatalf("selectCandidateForReview() returned nil")
	}
	if got.Confidence != 0.88 {
		t.Fatalf("selectCandidateForReview().Confidence = %.2f, want 0.88", got.Confidence)
	}
	if len(got.ArtistNames) != 2 || got.ArtistNames[1] != "Kelis" {
		t.Fatalf("selectCandidateForReview().ArtistNames = %v, want [Benny Benassi Kelis]", got.ArtistNames)
	}
}
