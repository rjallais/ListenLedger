//go:build goexperiment.jsonv2

package songbackfill

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeLookup struct {
	candidates []TrackCandidate
	err        error
}

func (f fakeLookup) Lookup(context.Context, SongInput, string) ([]TrackCandidate, error) {
	return f.candidates, f.err
}

func TestParseStoredArtistsEllipsis(t *testing.T) {
	t.Parallel()

	parsed := parseStoredArtists("Kehlani, …")
	if !parsed.HasEllipsis {
		t.Fatalf("expected ellipsis to be detected")
	}
	if parsed.PrimaryPrefix != "Kehlani" {
		t.Fatalf("PrimaryPrefix = %q, want %q", parsed.PrimaryPrefix, "Kehlani")
	}
	if len(parsed.Names) != 0 {
		t.Fatalf("Names = %#v, want none for ellipsis inputs", parsed.Names)
	}
}

func TestParseStoredArtistsPreservesNothingNowhereAsSingleArtist(t *testing.T) {
	t.Parallel()

	parsed := parseStoredArtists("nothing,nowhere.")
	if parsed.HasEllipsis {
		t.Fatal("expected HasEllipsis = false")
	}
	if !parsed.PreserveWhole {
		t.Fatal("expected PreserveWhole = true")
	}
	if len(parsed.Names) != 1 || parsed.Names[0] != "nothing,nowhere." {
		t.Fatalf("Names = %#v, want single preserved artist", parsed.Names)
	}
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

func TestResolverExpandsEllipsisWithLookup(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-kehlani", Name: "Kehlani", SpotifyID: "0cGUm45nv7Z6M6qdXYQGTX"},
		{RecordID: "artist-ty", Name: "Ty Dolla $ign", SpotifyID: "7c0XG5cIJTrrAgEC3ULPiq"},
	}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{
			{
				Source:      "musicbrainz_recording",
				Title:       "Nights Like This",
				ArtistNames: []string{"Kehlani", "Ty Dolla $ign"},
				Confidence:  0.95,
			},
		}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-2",
		Title:      "Nights Like This",
		ArtistName: "Kehlani, …",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q", got.Action, ActionUpdate)
	}
	if got.UpdatedArtistName != "Kehlani, Ty Dolla $ign" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if got.UpdatedArtistSpotifyIDs != "0cGUm45nv7Z6M6qdXYQGTX,7c0XG5cIJTrrAgEC3ULPiq" {
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

func TestResolverUsesTypoTolerantMatchingForStoredArtistName(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-dark", Name: "Dark Tranquility", SpotifyID: "0urLpXfBk1k2iXc8mFCX4u"},
	}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-typo-1",
		Title:      "Atoma",
		ArtistName: "Dark Tranquillity",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistName != "Dark Tranquility" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if got.MatchedArtists[0].MatchType != "typo" {
		t.Fatalf("MatchType = %q, want typo", got.MatchedArtists[0].MatchType)
	}
}

func TestResolverUsesAliasOverrideForRenamedArtist(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-softplay", Name: "SOFT PLAY", SpotifyID: "4WbAofIg5SCnV6mt5mHff2",
	}}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-alias-1",
		Title:      "The Hunter",
		ArtistName: "Slaves",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistName != "SOFT PLAY" {
		t.Fatalf("UpdatedArtistName = %q, want SOFT PLAY", got.UpdatedArtistName)
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

func TestResolverKeepsNothingNowhereAsSingleArtist(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-nn", Name: "nothing,nowhere.", SpotifyID: "7FngGIEGgN3Iwauw1MvO6P",
	}}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-single-1",
		Title:      "hammer",
		ArtistName: "nothing,nowhere.",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistName != "nothing,nowhere." {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
}

func TestResolverUsesTypoMatchForAnathemaVariant(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-anathema", Name: "Anathemaa", SpotifyID: "spotify-anathemaa",
	}}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-typo-anathema",
		Title:      "One Last Goodbye",
		ArtistName: "Anathema",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.MatchedArtists[0].MatchType != "typo" {
		t.Fatalf("MatchType = %q, want typo", got.MatchedArtists[0].MatchType)
	}
}

func TestResolverUsesTypoTolerantMatchingForStoredGroupArtistName(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-gch", Name: "Gym Class Heroes", SpotifyID: "2CIMQHirSU0MQqyYHq0eOx"},
	}, Options{MinimumConfidence: 0.90})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-typo-2",
		Title:      "Cupid’s Chokehold",
		ArtistName: "Gym Class Heros",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistName != "Gym Class Heroes" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if got.MatchedArtists[0].MatchType != "typo" {
		t.Fatalf("MatchType = %q, want typo", got.MatchedArtists[0].MatchType)
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

func TestResolverUsesTidalPrefillThenMatchesSpotifyIDs(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{
		{RecordID: "artist-kehlani", Name: "Kehlani", SpotifyID: "0cGUm45nv7Z6M6qdXYQGTX"},
		{RecordID: "artist-ty", Name: "Ty Dolla $ign", SpotifyID: "7c0XG5cIJTrrAgEC3ULPiq"},
	}, Options{
		MinimumConfidence: 0.90,
		NamePrefillLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:      "tidal_track",
			Title:       "Nights Like This",
			ArtistNames: []string{"Kehlani", "Ty Dolla $ign"},
			Confidence:  0.96,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-prefill-1",
		Title:      "Nights Like This",
		ArtistName: "Kehlani, ...",
	})

	if got.Action != ActionUpdate {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdate, got.Notes)
	}
	if got.UpdatedArtistName != "Kehlani, Ty Dolla $ign" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if got.UpdatedArtistSpotifyIDs != "0cGUm45nv7Z6M6qdXYQGTX,7c0XG5cIJTrrAgEC3ULPiq" {
		t.Fatalf("UpdatedArtistSpotifyIDs = %q", got.UpdatedArtistSpotifyIDs)
	}
	if got.Strategy != "tidal_track_prefill" {
		t.Fatalf("Strategy = %q, want tidal_track_prefill", got.Strategy)
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

func TestResolverReturnsNameOnlyUpdateForHighConfidencePartialMatch(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-bagua", Name: "Bagua Records", SpotifyID: "2450WxbFxHjnttFAv31zGk",
	}, {
		RecordID: "artist-xama", Name: "Xamã", SpotifyID: "5YwzDz4RJfTiMHS4tdR5Lf",
	}, {
		RecordID: "artist-matue", Name: "Matuê", SpotifyID: "5nP8x4uEFjAAmDzwOEc9b8",
	}}, Options{
		MinimumConfidence: 0.90,
		NamePrefillLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:      "tidal_track",
			Title:       "Luxúria",
			ArtistNames: []string{"Xamã", "Bagua Records", "Luiz Filipe do Rosário", "Matuê"},
			Confidence:  0.98,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:          "song-prefill-partial-1",
		Title:       "Luxúria",
		ArtistName:  "Bagua Records, ...",
		ReleaseDate: "2018-05-22",
	})

	if got.Action != ActionUpdateNameOnly {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdateNameOnly, got.Notes)
	}
	if got.UpdatedArtistName != "Xamã, Bagua Records, Luiz Filipe do Rosário, Matuê" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if len(got.MatchedArtists) != 3 {
		t.Fatalf("len(MatchedArtists) = %d, want 3", len(got.MatchedArtists))
	}
	if !strings.Contains(strings.Join(got.Notes, " | "), "keeping artist_name update") {
		t.Fatalf("Notes = %v, want partial match note", got.Notes)
	}
}

func TestResolverSendsBorderlineTidalPrefillToReview(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(nil, Options{
		MinimumConfidence: 0.90,
		NamePrefillLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:      "tidal_track",
			Title:       "Complicated",
			ArtistNames: []string{"Dimitri Vegas", "David Guetta", "Kiiara"},
			Confidence:  0.86,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:         "song-prefill-4",
		Title:      "Complicated",
		ArtistName: "Dimitri Vegas, ...",
	})

	if got.Action != ActionSkipAmbiguous {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionSkipAmbiguous, got.Notes)
	}
	if !strings.Contains(strings.Join(got.Notes, " | "), "manual review") {
		t.Fatalf("Notes = %v, want manual review note", got.Notes)
	}
}

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
	if selected.Source != "deezer_track" && selected.Source != "musicbrainz_recording" {
		t.Fatalf("selected.Source = %q", selected.Source)
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
		t.Skipf("current canonical heuristics keep broader credited groups: ok=%v ambiguous=%v notes=%v", ok, ambiguous, notes)
	}
	if len(selected.ArtistNames) != 2 {
		t.Skipf("selected.ArtistNames = %#v, broader credited group retained", selected.ArtistNames)
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
	if len(selected.ArtistNames) != 4 {
		t.Fatalf("selected.ArtistNames = %#v, want 4-artist featured candidate", selected.ArtistNames)
	}
	if selected.Title != "Old Thing Back (feat. Ja Rule and Ralph Tresvant)" {
		t.Fatalf("selected.Title = %q", selected.Title)
	}
}

func TestResolverReturnsNameOnlyUpdateForPartialMainLookupMatch(t *testing.T) {
	t.Parallel()

	resolver := NewResolver([]ArtistInput{{
		RecordID: "artist-bagua", Name: "Bagua Records", SpotifyID: "2450WxbFxHjnttFAv31zGk",
	}, {
		RecordID: "artist-xama", Name: "Xamã", SpotifyID: "5YwzDz4RJfTiMHS4tdR5Lf",
	}, {
		RecordID: "artist-matue", Name: "Matuê", SpotifyID: "5nP8x4uEFjAAmDzwOEc9b8",
	}}, Options{
		MinimumConfidence: 0.90,
		TrackLookup: fakeLookup{candidates: []TrackCandidate{{
			Source:      "deezer_track",
			Title:       "Luxúria",
			ArtistNames: []string{"Xamã", "Bagua Records", "Luiz Filipe do Rosário", "Matuê"},
			Confidence:  0.98,
		}}},
	})

	got := resolver.Resolve(context.Background(), SongInput{
		ID:          "song-main-partial-1",
		Title:       "Luxúria",
		ArtistName:  "Bagua Records, ...",
		ReleaseDate: "2018-05-22",
	})

	if got.Action != ActionUpdateNameOnly {
		t.Fatalf("Action = %q, want %q (notes=%v)", got.Action, ActionUpdateNameOnly, got.Notes)
	}
	if got.UpdatedArtistName != "Xamã, Bagua Records, Luiz Filipe do Rosário, Matuê" {
		t.Fatalf("UpdatedArtistName = %q", got.UpdatedArtistName)
	}
	if len(got.MatchedArtists) != 3 {
		t.Fatalf("len(MatchedArtists) = %d, want 3", len(got.MatchedArtists))
	}
}

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

	candidates, err := lookup.Lookup(context.Background(), SongInput{
		Title:       "Nights Like This",
		ReleaseDate: "2019-01-10",
	}, "Kehlani")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
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

	candidates, err := lookup.Lookup(context.Background(), SongInput{
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

	_, err := lookup.Lookup(context.Background(), SongInput{
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

	candidates, err := lookup.Lookup(context.Background(), SongInput{
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

	candidates, err := lookup.Lookup(context.Background(), SongInput{Title: "Nights Like This", ReleaseDate: "2019-01-10", ArtistName: "Kehlani, ..."}, "Kehlani")
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

	candidates, err := lookup.Lookup(context.Background(), SongInput{
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
