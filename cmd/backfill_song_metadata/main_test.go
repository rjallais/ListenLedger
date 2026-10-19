package main

import (
	"context"
	"errors"
	"testing"

	"ListenLedger/internal/songbackfill"
)

// fakeLookup stubs the TIDAL search per song title.
type fakeLookup struct {
	byTitle map[string][]songbackfill.TrackCandidate
	errBy   map[string]error
}

func (f *fakeLookup) Lookup(_ context.Context, song songbackfill.SongInput, _ string) ([]songbackfill.TrackCandidate, error) {
	if err, ok := f.errBy[song.Title]; ok {
		return nil, err
	}
	return f.byTitle[song.Title], nil
}

func TestResolveAllPlansFillsOnlyMissingFields(t *testing.T) {
	lookup := &fakeLookup{byTitle: map[string][]songbackfill.TrackCandidate{
		// Sorted-by-confidence input, as the real lookup returns: the top
		// candidate carries no album data, the second one does.
		"Full Song": {
			{Source: "tidal_track", Title: "Full Song", ArtistNames: []string{"Artist"}, Confidence: 0.99},
			{Source: "tidal_track", Title: "Full Song", ArtistNames: []string{"Artist"}, AlbumName: "The Album", ReleaseDate: "1992-09-29", Confidence: 0.95},
		},
		"Low Confidence": {
			{Source: "tidal_track", Title: "Low Confidence", ArtistNames: []string{"Artist"}, AlbumName: "Shaky", Confidence: 0.70},
		},
		"No Data": {
			{Source: "tidal_track", Title: "No Data", ArtistNames: []string{"Artist"}, Confidence: 0.99},
		},
	}, errBy: map[string]error{
		"Broken": errors.New("search failed"),
	}}

	rows := []songRow{
		{id: "sg_1", title: "Full Song", artistName: "Artist", album: "", releaseDate: "1990-01-01"},
		{id: "sg_2", title: "Low Confidence", artistName: "Artist"},
		{id: "sg_3", title: "No Data", artistName: "Artist"},
		{id: "sg_4", title: "Broken", artistName: "Artist"},
		{id: "sg_5", title: "Ghost", artistName: "Artist"},
	}

	resolutions := resolveAll(t.Context(), lookup, rows, nil, 0.90)
	if len(resolutions) != 5 {
		t.Fatalf("len(resolutions) = %d, want 5", len(resolutions))
	}

	byID := make(map[string]resolution, len(resolutions))
	for _, res := range resolutions {
		byID[res.SongID] = res
	}

	// sg_1 has a release date already: only the album is filled from TIDAL,
	// and the year is derived from the row's own date (matching the event).
	res := byID["sg_1"]
	if res.Action != actionUpdate || res.Fill == nil {
		t.Fatalf("sg_1 = %+v, want update with fill", res)
	}
	if res.Fill.Album != "The Album" || res.Fill.ReleaseDate != "" || res.Fill.ReleaseYear != 1990 {
		t.Fatalf("sg_1 fill = %+v, want album plus year from the existing date", *res.Fill)
	}
	if res.Confidence != 0.95 {
		t.Fatalf("sg_1 confidence = %v, want 0.95", res.Confidence)
	}

	if res := byID["sg_2"]; res.Action != actionSkipLowConfidence || res.Fill == nil {
		t.Fatalf("sg_2 = %+v, want low-confidence skip with fill planned", res)
	}
	if res := byID["sg_3"]; res.Action != actionSkipNoData {
		t.Fatalf("sg_3 = %+v, want skip_no_data", res)
	}
	if res := byID["sg_4"]; res.Action != actionSkipLookupFailed || res.Note == "" {
		t.Fatalf("sg_4 = %+v, want lookup-failure skip with note", res)
	}
	if res := byID["sg_5"]; res.Action != actionSkipUnresolved {
		t.Fatalf("sg_5 = %+v, want skip_unresolved", res)
	}

	s := summarize(resolutions)
	if s.SongsInspected != 5 || s.UpdatesPlanned != 1 || s.SkippedLowConfidence != 1 ||
		s.SkippedNoData != 1 || s.SkippedLookupFailed != 1 || s.SkippedUnresolved != 1 {
		t.Fatalf("summary = %+v", s)
	}
}

func TestResolveAllPortsPocketBaseReleaseType(t *testing.T) {
	lookup := &fakeLookup{byTitle: map[string][]songbackfill.TrackCandidate{
		// Type-less candidate: without the PocketBase label this row
		// would be skip_no_data.
		"Only Type Missing": {
			{Source: "tidal_track", Title: "Only Type Missing", ArtistNames: []string{"Artist"}, Confidence: 0.99},
		},
		"Suffix Song": {
			{Source: "tidal_track", Title: "Suffix Song", ArtistNames: []string{"Artist"}, AlbumName: "Cool Track", ReleaseType: "single", Confidence: 0.95},
		},
		// Below the confidence threshold: the PB label must win, not the
		// shaky album.
		"Shaky Match": {
			{Source: "tidal_track", Title: "Shaky Match", ArtistNames: []string{"Artist"}, AlbumName: "Wrong Album", Confidence: 0.70},
		},
	}}

	rows := []songRow{
		{id: "pb_1", title: "Only Type Missing", artistName: "Artist", album: "Some Album", releaseDate: "2020-01-01", releaseYear: 2020},
		{id: "pb_2", title: "No Candidates", artistName: "Artist"},
		{id: "pb_3", title: "Suffix Song", artistName: "Artist"},
		{id: "pb_4", title: "Shaky Match", artistName: "Artist"},
	}
	pbTypes := map[string]string{"pb_1": "album", "pb_2": "single", "pb_3": "ep", "pb_4": "single"}

	resolutions := resolveAll(t.Context(), lookup, rows, pbTypes, 0.90)
	if len(resolutions) != 4 {
		t.Fatalf("len(resolutions) = %d, want 4", len(resolutions))
	}
	byID := make(map[string]resolution, len(resolutions))
	for _, res := range resolutions {
		byID[res.SongID] = res
	}

	// pb_1: the label fills the only missing field; the dataless TIDAL
	// candidate must not win the loop and mask the fill as TIDAL-sourced.
	res := byID["pb_1"]
	if res.Action != actionUpdate || res.Fill == nil || res.Fill.ReleaseType != "album" || res.Fill.Album != "" {
		t.Fatalf("pb_1 = %+v, want update with the PocketBase type only", res)
	}
	if res.Candidate != nil || res.Confidence != 1 {
		t.Fatalf("pb_1 candidate = %+v confidence = %v, want a candidate-less PocketBase fill", res.Candidate, res.Confidence)
	}
	// pb_2: no TIDAL candidates at all; the label still ports, source-less.
	res = byID["pb_2"]
	if res.Action != actionUpdate || res.Fill == nil || res.Fill.ReleaseType != "single" ||
		res.Candidate != nil || res.Note == "" {
		t.Fatalf("pb_2 = %+v, want candidate-less update from the PocketBase label", res)
	}
	// pb_3: the user-curated label wins over the TIDAL suffix.
	res = byID["pb_3"]
	if res.Action != actionUpdate || res.Fill == nil || res.Fill.Album != "Cool Track" || res.Fill.ReleaseType != "ep" {
		t.Fatalf("pb_3 = %+v, want the suffix album plus the PocketBase type", res)
	}
	// pb_4: TIDAL's best is below the threshold; the trusted label ports
	// instead of skipping, and the shaky album is dropped.
	res = byID["pb_4"]
	if res.Action != actionUpdate || res.Fill == nil || res.Fill.ReleaseType != "single" || res.Fill.Album != "" ||
		res.Candidate != nil || res.Confidence != 1 || res.Note != notePBReleaseType {
		t.Fatalf("pb_4 = %+v, want a candidate-less PocketBase fill below the confidence threshold", res)
	}
}
