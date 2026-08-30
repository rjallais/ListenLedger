// Package songbackfill provides track-candidate selection and song backfill helpers.
package songbackfill

import (
	"reflect"
	"testing"
)

func TestSelectTrackCandidatePrefersCorroboratedGroup(t *testing.T) {
	t.Parallel()

	selected, ambiguous, notes, ok := selectTrackCandidate([]TrackCandidate{
		{
			Source:      "musicbrainz_recording",
			Title:       "Complicated",
			ArtistNames: []string{"Dimitri Vegas & Like Mike", "David Guetta", "Kiiara"},
			Confidence:  0.92,
		},
		{
			Source:      "musicbrainz_recording",
			Title:       "Complicated",
			ArtistNames: []string{"Dimitri Vegas", "David Guetta", "Kiiara"},
			Confidence:  0.93,
		},
		{
			Source:      "deezer_track",
			Title:       "Complicated",
			ArtistNames: []string{"Dimitri Vegas & Like Mike", "David Guetta", "Kiiara"},
			Confidence:  0.91,
		},
	})
	if !ok || ambiguous {
		t.Fatalf("ok=%v ambiguous=%v notes=%v, want corroborated selection", ok, ambiguous, notes)
	}
	if selected.Source != "musicbrainz_recording" {
		t.Fatalf("selected.Source = %q, want highest-confidence corroborated candidate", selected.Source)
	}
	if got := normalizedArtistListKey(selected.ArtistNames); got != normalizedArtistListKey([]string{"Dimitri Vegas & Like Mike", "David Guetta", "Kiiara"}) {
		t.Fatalf("selected artist list = %q", got)
	}
}

func TestSelectTrackCandidatePrefersCanonicalOverRemixVariants(t *testing.T) {
	t.Parallel()

	selected, ambiguous, notes, ok := selectTrackCandidate([]TrackCandidate{
		{
			Source:      "deezer_track",
			Title:       "Take U There (feat. Kiesza) (Felix Cartal Remix)",
			ArtistNames: []string{"Jack U", "Skrillex", "Diplo", "Kiesza", "Felix Cartal"},
			Confidence:  0.98,
		},
		{
			Source:      "deezer_track",
			Title:       "Take U There (feat. Kiesza)",
			ArtistNames: []string{"Jack U", "Skrillex", "Diplo", "Kiesza"},
			Confidence:  0.95,
		},
	})
	if !ok || ambiguous {
		t.Fatalf("ok=%v ambiguous=%v notes=%v", ok, ambiguous, notes)
	}
	if selected.Title != "Take U There (feat. Kiesza)" {
		t.Fatalf("selected.Title = %q, want canonical non-remix candidate", selected.Title)
	}
}

func TestSelectTrackCandidatePrefersFewestArtistsAmongNearTopCandidates(t *testing.T) {
	t.Parallel()

	selected, ambiguous, notes, ok := selectTrackCandidate([]TrackCandidate{
		{
			Source:      "deezer_track",
			Title:       "Old Thing Back",
			ArtistNames: []string{"Matoma", "The Notorious B.I.G.", "Ja Rule", "Ralph Tresvant"},
			Confidence:  0.97,
		},
		{
			Source:      "musicbrainz_recording",
			Title:       "Old Thing Back",
			ArtistNames: []string{"Matoma", "The Notorious B.I.G."},
			Confidence:  0.97,
		},
		{
			Source:      "tidal_track",
			Title:       "Old Thing Back",
			ArtistNames: []string{"Matoma", "The Notorious B.I.G."},
			Confidence:  0.96,
		},
	})
	if !ok || ambiguous {
		t.Fatalf("ok=%v ambiguous=%v notes=%v, want non-ambiguous selection", ok, ambiguous, notes)
	}
	expectedArtists := []string{"Matoma", "The Notorious B.I.G."}
	if !reflect.DeepEqual(selected.ArtistNames, expectedArtists) {
		t.Fatalf("selected.ArtistNames = %#v, want %#v", selected.ArtistNames, expectedArtists)
	}
	if selected.Source != "musicbrainz_recording" {
		t.Fatalf("selected.Source = %q, want %q", selected.Source, "musicbrainz_recording")
	}
}

func TestSelectTrackCandidateKeepsFeaturedArtistsWhenTitleSignalsFeature(t *testing.T) {
	t.Parallel()

	selected, ambiguous, notes, ok := selectTrackCandidate([]TrackCandidate{
		{
			Source:      "deezer_track",
			Title:       "Old Thing Back (feat. Ja Rule and Ralph Tresvant)",
			ArtistNames: []string{"Matoma", "The Notorious B.I.G.", "Ja Rule", "Ralph Tresvant"},
			Confidence:  0.98,
		},
		{
			Source:      "musicbrainz_recording",
			Title:       "Old Thing Back",
			ArtistNames: []string{"Matoma", "The Notorious B.I.G."},
			Confidence:  0.97,
		},
	})
	if !ok || ambiguous {
		t.Fatalf("ok=%v ambiguous=%v notes=%v", ok, ambiguous, notes)
	}
	expectedArtists := []string{"Matoma", "The Notorious B.I.G.", "Ja Rule", "Ralph Tresvant"}
	if !reflect.DeepEqual(selected.ArtistNames, expectedArtists) {
		t.Fatalf("selected.ArtistNames = %#v, want %#v", selected.ArtistNames, expectedArtists)
	}
	if selected.Title != "Old Thing Back (feat. Ja Rule and Ralph Tresvant)" {
		t.Fatalf("selected.Title = %q", selected.Title)
	}
}
