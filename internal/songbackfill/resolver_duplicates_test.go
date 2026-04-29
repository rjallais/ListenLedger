//go:build goexperiment.jsonv2

package songbackfill

import (
	"context"
	"strings"
	"testing"
)

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
