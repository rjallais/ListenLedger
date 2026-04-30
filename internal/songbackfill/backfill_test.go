//go:build goexperiment.jsonv2

package songbackfill

import (
	"context"
	"strings"
	"testing"
)

type fakeLookup struct {
	candidates []TrackCandidate
	err        error
}

func (f fakeLookup) Lookup(context.Context, SongInput, string) ([]TrackCandidate, error) {
	return f.candidates, f.err
}

func TestResolverMatchesStoredArtistNames(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-camilo", Name: "Camilo", SpotifyID: "28gNT5KBp7IjEOQoevXf9N"},
		{RecordID: "artist-pedro", Name: "Pedro Capó", SpotifyID: "2EMAnMvWE2eb56ToJVfCWs"},
	}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-1",
		Title:      "Tutu",
		ArtistName: "Camilo, Pedro Capó",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q", got.Action, ActionUpdate)
	}
	if got.UpdatedArtistName != "Camilo, Pedro Capó" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if got.UpdatedArtistSpotifyIDs != "28gNT5KBp7IjEOQoevXf9N,2EMAnMvWE2eb56ToJVfCWs" {
		t.Fatalf("UpdatedArtistSpotifyIDs = %q", got.UpdatedArtistSpotifyIDs)
	}
}

func TestResolverUsesLooseNormalization(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-pink", Name: "P!nk", SpotifyID: "1KCSPY1glIKqW2TotWuXOR"},
		{RecordID: "artist-nate", Name: "Nate Ruess", SpotifyID: "7xTkKJkzFfJ0mJGUgM3YkX"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "musicbrainz_recording",
				Title:       "Just Give Me a Reason",
				ArtistNames: []string{"Pnk", "Nate Ruess"},
				Confidence:  0.95,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-3",
		Title:      "Just Give Me a Reason",
		ArtistName: "P!nk, …",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q", got.Action, ActionUpdate)
	}
	if len(got.MatchedArtists) != 2 || got.MatchedArtists[0].MatchType != "loose" {
		t.Fatalf("MatchedArtists = %#v, want loose normalization on the first artist", got.MatchedArtists)
	}
}

func TestResolverUsesCollapsedNormalizationForHyphenatedNames(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-benny", Name: "Benny Benassi", SpotifyID: "4Ws2otunReOa6BbwxxpCt6"},
		{RecordID: "artist-kelis", Name: "Kelis", SpotifyID: "0IF46Ma8u1l2SeD7LxsuZR"},
		{RecordID: "artist-apl", Name: "apl.de.ap", SpotifyID: "1RMUYCezWov2c5Ccy7WfPf"},
		{RecordID: "artist-jb", Name: "Jean-Baptiste", SpotifyID: "6FEUbUCBaqdNvXmMBz3yyO"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "deezer_track",
				Title:       "Spaceship (feat. Kelis, apl.de.ap & Jean Baptiste) (Radio Edit)",
				ArtistNames: []string{"Benny Benassi", "Kelis", "apl.de.ap", "Jean Baptiste"},
				Confidence:  0.95,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-flat-1",
		Title:      "Spaceship",
		ArtistName: "Benny Benassi, ...",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistSpotifyIDs != "4Ws2otunReOa6BbwxxpCt6,0IF46Ma8u1l2SeD7LxsuZR,1RMUYCezWov2c5Ccy7WfPf,6FEUbUCBaqdNvXmMBz3yyO" {
		t.Fatalf("UpdatedArtistSpotifyIDs = %q", got.UpdatedArtistSpotifyIDs)
	}
	if got.MatchedArtists[3].MatchType != "flat" {
		t.Fatalf("final MatchType = %q, want flat (all=%#v)", got.MatchedArtists[3].MatchType, got.MatchedArtists)
	}
}

func TestResolverUsesCollapsedNormalizationForStylizedFeatureNames(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-e40", Name: "E-40", SpotifyID: "3crnzLy8R4lVwaigKEOz7V"},
		{RecordID: "artist-tpain", Name: "T-Pain", SpotifyID: "3aQeKQSyrW4qWr35idm0cy"},
		{RecordID: "artist-kandi", Name: "Kandi Girl", SpotifyID: "2JZlL7dV4Sx7jQtKMP2kgG"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "deezer_track",
				Title:       "U and Dat (feat. T. Pain & Kandi Girl)",
				ArtistNames: []string{"E-40", "T. Pain", "Kandi Girl"},
				Confidence:  0.98,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-flat-2",
		Title:      "U and Dat",
		ArtistName: "E-40, ...",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistSpotifyIDs != "3crnzLy8R4lVwaigKEOz7V,3aQeKQSyrW4qWr35idm0cy,2JZlL7dV4Sx7jQtKMP2kgG" {
		t.Fatalf("UpdatedArtistSpotifyIDs = %q", got.UpdatedArtistSpotifyIDs)
	}
	if got.MatchedArtists[1].MatchType != "flat" {
		t.Fatalf("feature MatchType = %q, want flat (all=%#v)", got.MatchedArtists[1].MatchType, got.MatchedArtists)
	}
}

func TestResolverTreatsVariousArtistsAsSentinel(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(nil, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-sentinel-1",
		Title:      "Electric Worry",
		ArtistName: "Various Artists",
	})

	if got.Action != ActionSkipUnresolved {
		t.Fatalf("Action = %q, want %q", got.Action, ActionSkipUnresolved)
	}
	if !strings.Contains(strings.Join(got.Notes, " | "), "placeholder credit") {
		t.Fatalf("Notes = %v, want placeholder credit note", got.Notes)
	}
}

func TestResolverRejectsAmbiguousTypoMatches(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-one", Name: "Blacksmith", SpotifyID: "spotify-blacksmith"},
		{RecordID: "artist-two", Name: "Slacksmith", SpotifyID: "spotify-slacksmith"},
	}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-typo-3",
		Title:      "Example",
		ArtistName: "Clacksmith",
	})

	if got.Action != ActionSkipAmbiguous {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionSkipAmbiguous, got.Notes)
	}
}

func TestResolverSkipsAmbiguousExternalCandidates(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-1", Name: "Kehlani", SpotifyID: "0cGUm45nv7Z6M6qdXYQGTX"},
		{RecordID: "artist-2", Name: "Ty Dolla $ign", SpotifyID: "7c0XG5cIJTrrAgEC3ULPiq"},
		{RecordID: "artist-3", Name: "6LACK", SpotifyID: "4IVAbR2w4JJNJDDRFP3E83"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "musicbrainz_recording",
				Title:       "Nights Like This",
				ArtistNames: []string{"Kehlani", "Ty Dolla $ign"},
				Confidence:  0.95,
			},
			{
				Source:      "musicbrainz_recording",
				Title:       "Nights Like This",
				ArtistNames: []string{"Kehlani", "6LACK"},
				Confidence:  0.94,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-4",
		Title:      "Nights Like This",
		ArtistName: "Kehlani, …",
	})

	if got.Action != ActionSkipAmbiguous {
		t.Fatalf("Action = %q, want %q", got.Action, ActionSkipAmbiguous)
	}
}

func TestResolverAllowsStoredArtistLaterInTrailingEllipsisCredits(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-kayblack", Name: "Kayblack", SpotifyID: "2h5Ha0ZiMFmOQD3iYcSXsy"},
		{RecordID: "artist-wallhein", Name: "Wall Hein", SpotifyID: "5wEgjH2s4SAiffRmzkBqHB"},
		{RecordID: "artist-amusik", Name: "AMusik", SpotifyID: "48r1nXoaPXPSx1LoM0Rnzl"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "musicbrainz_recording",
				Title:       "Maturidade",
				ArtistNames: []string{"Kayblack", "Wall Hein", "AMusik"},
				Confidence:  0.93,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-5",
		Title:      "Maturidade",
		ArtistName: "AMusik, ...",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q", got.Action, ActionUpdate)
	}
	if got.UpdatedArtistName != "Kayblack, Wall Hein, AMusik" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
}

func TestResolverRequiresMultipleArtistsForEllipsis(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-aborted", Name: "Aborted", SpotifyID: "1XRhUgCyzIdeT8d9KMfeDR"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "musicbrainz_recording",
				Title:       "Dreadbringer",
				ArtistNames: []string{"Aborted"},
				Confidence:  0.97,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-6",
		Title:      "Dreadbringer",
		ArtistName: "Aborted, ...",
	})

	if got.Action != ActionSkipUnresolved {
		t.Fatalf("Action = %q, want %q", got.Action, ActionSkipUnresolved)
	}
}

func TestResolverPrefillCannotCollapseKnownMultiArtistRow(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-aborted", Name: "Aborted", SpotifyID: "1XRhUgCyzIdeT8d9KMfeDR",
	}}, Options{
		MinimumConfidence: 0.90,
		NamePrefillLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:      "tidal_track",
			Title:       "Dreadbringer",
			ArtistNames: []string{"Aborted"},
			Confidence:  0.97,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-prefill-2",
		Title:      "Dreadbringer",
		ArtistName: "Aborted, ...",
	})

	if got.Action != ActionSkipUnresolved {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionSkipUnresolved, got.Notes)
	}
	if !strings.Contains(strings.Join(got.Notes, " | "), "safe multi-artist expansion") {
		t.Fatalf("Notes = %v, want safe multi-artist expansion warning", got.Notes)
	}
}

func TestResolverReturnsNameOnlyUpdateForHighConfidenceTidalPrefill(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-kehlani", Name: "Kehlani", SpotifyID: "0cGUm45nv7Z6M6qdXYQGTX",
	}}, Options{
		MinimumConfidence: 0.90,
		NamePrefillLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:      "tidal_track",
			Title:       "Nights Like This",
			ArtistNames: []string{"Kehlani", "Ty Dolla $ign"},
			Confidence:  0.95,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-prefill-3",
		Title:      "Nights Like This",
		ArtistName: "Kehlani, ...",
	})

	if got.Action != ActionUpdateNameOnly {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdateNameOnly, got.Notes)
	}
	if got.UpdatedArtistName != "Kehlani, Ty Dolla $ign" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if got.UpdatedArtistSpotifyIDs != "" {
		t.Fatalf("UpdatedArtistSpotifyIDs = %q, want empty", got.UpdatedArtistSpotifyIDs)
	}
}

func TestResolverSendsBorderlineTidalPrefillToReview(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(nil, Options{
		MinimumConfidence: 0.90,
		NamePrefillLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:       "tidal_track",
			Title:        "Complicated",
			ArtistNames:  []string{"Dimitri Vegas", "David Guetta", "Kiiara"},
			Confidence:   0.86,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-prefill-4",
		Title:      "Complicated",
		ArtistName: "Dimitri Vegas, ...",
	})

	if got.Action != ActionSkipLowConfidence {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionSkipLowConfidence, got.Notes)
	}
	if !strings.Contains(strings.Join(got.Notes, " | "), "manual review") {
		t.Fatalf("Notes = %v, want manual review note", got.Notes)
	}
}
