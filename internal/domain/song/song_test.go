package song_test

import (
	"errors"
	"testing"

	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
)

func TestSongReplayRoundTrip(t *testing.T) {
	agg, err := song.NewSong("sg_1", "Rooster", "Alice in Chains", "Dirt", "1992-09-29", "album", "", 1992, 3, 1, true)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := agg.SetRecent(false, 0, 0); err != nil {
		t.Fatalf("recent: %v", err)
	}

	replayed, err := song.Replay("sg_1", agg.UncommittedEvents())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Version() != 2 || replayed.IsRecent || replayed.Title != "Rooster" {
		t.Fatalf("replayed state: %+v", replayed)
	}
}

func TestSongRejectsEmptyTitle(t *testing.T) {
	if _, err := song.NewSong("sg_1", " ", "x", "", "", "", "", 0, 0, 0, false); !errors.Is(err, song.ErrEmptyTitle) {
		t.Fatalf("expected empty title, got %v", err)
	}
}

func TestSongEnrichMetadataFillsOnlyMissing(t *testing.T) {
	agg, err := song.NewSong("sg_2", "Them Bones", "Alice in Chains", "", "", "", "", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	changed, err := agg.EnrichMetadata("Dirt", "1992-09-29", "", "tidal")
	if err != nil || !changed {
		t.Fatalf("enrich: changed=%v err=%v", changed, err)
	}
	if agg.Album != "Dirt" || agg.ReleaseDate != "1992-09-29" || agg.ReleaseYear != 1992 {
		t.Fatalf("enriched state: %+v", agg)
	}

	// Present fields win: enrichment must not overwrite, and with nothing
	// missing a rerun records no event.
	changed, err = agg.EnrichMetadata("Facelift", "1990-08-21", "", "tidal")
	if err != nil || changed {
		t.Fatalf("rerun: changed=%v err=%v", changed, err)
	}
	if agg.Album != "Dirt" || agg.ReleaseDate != "1992-09-29" {
		t.Fatalf("state overwritten: %+v", agg)
	}

	// Empty external values record nothing.
	changed, err = agg.EnrichMetadata("", "", "", "tidal")
	if err != nil || changed {
		t.Fatalf("empty values: changed=%v err=%v", changed, err)
	}

	replayed, err := song.Replay("sg_2", agg.UncommittedEvents())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Version() != 2 || replayed.Album != "Dirt" || replayed.ReleaseYear != 1992 {
		t.Fatalf("replayed state: %+v", replayed)
	}
}

func TestSongEnrichMetadataFillsYearFromExistingDate(t *testing.T) {
	agg, err := song.NewSong("sg_3", "Would?", "Alice in Chains", "", "1992", "", "", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Album arrives without a usable release date; the existing partial
	// date still yields the year.
	changed, err := agg.EnrichMetadata("Dirt", "", "", "tidal")
	if err != nil || !changed {
		t.Fatalf("enrich: changed=%v err=%v", changed, err)
	}
	if agg.Album != "Dirt" || agg.ReleaseDate != "1992" || agg.ReleaseYear != 1992 {
		t.Fatalf("enriched state: %+v", agg)
	}
}

func TestSongEnrichMetadataFillsReleaseType(t *testing.T) {
	agg, err := song.NewSong("sg_4", "Ylang Ylang", "FKJ", "", "", "", "", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	changed, err := agg.EnrichMetadata("Ylang Ylang", "2019-11-12", "ep", "tidal")
	if err != nil || !changed {
		t.Fatalf("enrich: changed=%v err=%v", changed, err)
	}
	if agg.ReleaseType != "ep" {
		t.Fatalf("ReleaseType = %q, want ep", agg.ReleaseType)
	}
	// A present type survives an enrichment without one.
	changed, err = agg.EnrichMetadata("Other Album", "2020-01-01", "", "tidal")
	if err != nil || changed {
		t.Fatalf("rerun: changed=%v err=%v", changed, err)
	}
	if agg.ReleaseType != "ep" {
		t.Fatalf("ReleaseType overwritten: %q", agg.ReleaseType)
	}
}

// Pre-field (v1) SongMetadataEnriched events carry no release_type; folding
// one must not wipe the type the song already had.
func TestSongApplyLegacyEnrichedKeepsReleaseType(t *testing.T) {
	agg, err := song.NewSong("sg_5", "Rooster", "Alice in Chains", "", "1992-09-29", "album", "", 1992, 0, 0, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	v1 := eventsourcing.Event{
		StreamID:   "sg_5",
		StreamType: song.StreamTypeSong,
		Version:    2,
		EventType:  song.EventTypeSongMetadataEnriched,
		Payload:    []byte(`{"song_id":"sg_5","album":"Dirt","release_date":"1992-09-29","release_year":1992,"source":"tidal"}`),
	}
	if err := agg.Apply(v1); err != nil {
		t.Fatalf("apply v1: %v", err)
	}
	if agg.Album != "Dirt" || agg.ReleaseType != "album" {
		t.Fatalf("folded state: %+v", agg)
	}
}

func TestSongTidalLinkingAndCorrections(t *testing.T) {
	agg, err := song.NewSong("sg_6", "Açaí", "Djavan, ...", "Luz - Single", "1982-01-01", "single", "spot_6", 1982, 0, 0, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Link Tidal ID
	linked, err := agg.LinkTidalID("tidal_123", "title_artist_match", 0.95)
	if err != nil || !linked {
		t.Fatalf("link tidal: changed=%v err=%v", linked, err)
	}
	if agg.TidalID != "tidal_123" {
		t.Errorf("expected TidalID 'tidal_123', got %q", agg.TidalID)
	}

	// Correct metadata (strip "- Single" suffix)
	correctedMeta, err := agg.CorrectMetadata("Luz", "1982-05-15", "album", "tidal")
	if err != nil || !correctedMeta {
		t.Fatalf("correct metadata: changed=%v err=%v", correctedMeta, err)
	}
	if agg.Album != "Luz" || agg.ReleaseType != "album" || agg.ReleaseDate != "1982-05-15" {
		t.Errorf("unexpected metadata after correction: %+v", agg)
	}

	// Correct artist credits
	correctedCredits, err := agg.CorrectArtistCredits("Djavan", "tidal")
	if err != nil || !correctedCredits {
		t.Fatalf("correct artist credits: changed=%v err=%v", correctedCredits, err)
	}
	if agg.ArtistName != "Djavan" {
		t.Errorf("expected ArtistName 'Djavan', got %q", agg.ArtistName)
	}

	// Mark duplicate
	markedDup, err := agg.MarkDuplicate("sg_canonical", "tidal")
	if err != nil || !markedDup {
		t.Fatalf("mark duplicate: changed=%v err=%v", markedDup, err)
	}
	if agg.DuplicateOf != "sg_canonical" {
		t.Errorf("expected DuplicateOf 'sg_canonical', got %q", agg.DuplicateOf)
	}

	// Replay roundtrip
	replayed, err := song.Replay("sg_6", agg.UncommittedEvents())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.TidalID != "tidal_123" || replayed.Album != "Luz" || replayed.ArtistName != "Djavan" || replayed.DuplicateOf != "sg_canonical" {
		t.Errorf("replayed state mismatch: %+v", replayed)
	}
}
