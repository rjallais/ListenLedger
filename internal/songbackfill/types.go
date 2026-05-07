//go:build goexperiment.jsonv2

// Package songbackfill resolves missing Spotify artist IDs for songs by matching
// stored artist names against an artist index and external metadata lookups.
package songbackfill

import (
	"context"
	"strings"
)

const (
	ActionUpdate            = "update"
	ActionUpdateNameOnly    = "update_name_only"
	ActionSkipAmbiguous     = "skip_ambiguous"
	ActionSkipLowConfidence = "skip_low_confidence"
	ActionSkipExisting      = "skip_existing"
	ActionSkipUnresolved    = "skip_unresolved"
)

const NoteEllipsisMultiArtistNotFound = "external lookup did not find a confident multi-artist credit for an ellipsis-based song"

type SongInput struct {
	ID               string
	Title            string
	ArtistName       string
	ReleaseDate      string
	ArtistSpotifyIDs string
}

type ArtistInput struct {
	RecordID  string
	Name      string
	SpotifyID string
}

type ArtistMatch struct {
	RecordID  string `json:"record_id"`
	Name      string `json:"name"`
	SpotifyID string `json:"spotify_id"`
	MatchType string `json:"match_type"`
}

type CandidateSummary struct {
	Source      string   `json:"source"`
	Title       string   `json:"title"`
	ArtistNames []string `json:"artist_names,omitempty"`
	ReleaseYear int      `json:"release_year,omitempty"`
	Confidence  float64  `json:"confidence"`
}

type Resolution struct {
	SongID                   string            `json:"song_id"`
	Title                    string            `json:"title"`
	ReleaseDate              string            `json:"release_date,omitempty"`
	OriginalArtistName       string            `json:"original_artist_name"`
	OriginalArtistSpotifyIDs string            `json:"original_artist_spotify_ids,omitempty"`
	UpdatedArtistName        string            `json:"updated_artist_name,omitempty"`
	UpdatedArtistSpotifyIDs  string            `json:"updated_artist_spotify_ids,omitempty"`
	Action                   string            `json:"action"`
	Strategy                 string            `json:"strategy,omitempty"`
	Confidence               float64           `json:"confidence"`
	MatchedArtists           []ArtistMatch     `json:"matched_artists,omitempty"`
	ExternalCandidates       []CandidateSummary `json:"external_candidates,omitempty"`
	Notes                    []string          `json:"notes,omitempty"`
}

func (r Resolution) Approved(minimumConfidence float64) bool {
	return r.Action == ActionUpdate &&
		r.UpdatedArtistSpotifyIDs != "" &&
		r.Confidence >= minimumConfidence
}

func (r Resolution) NamePrefillApproved(minimumConfidence float64) bool {
	return r.Action == ActionUpdateNameOnly &&
		r.UpdatedArtistName != "" &&
		r.UpdatedArtistName != r.OriginalArtistName &&
		r.Confidence >= minimumConfidence
}

func (r *Resolution) applyMatches(matches []ArtistMatch, strategy string, confidence float64) {
	if len(matches) == 0 {
		return
	}

	artistNames := make([]string, 0, len(matches))
	artistSpotifyIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		artistNames = append(artistNames, match.Name)
		artistSpotifyIDs = append(artistSpotifyIDs, match.SpotifyID)
	}

	r.Action = ActionUpdate
	r.Strategy = strategy
	r.Confidence = confidence
	r.MatchedArtists = matches
	r.UpdatedArtistName = strings.Join(artistNames, ", ")
	r.UpdatedArtistSpotifyIDs = strings.Join(artistSpotifyIDs, ",")
}

type Options struct {
	NamePrefillLookup TrackMetadataLookup
	TrackLookup       TrackMetadataLookup
	MinimumConfidence float64
}

type TrackMetadataLookup interface {
	Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error)
}

type TrackCandidate struct {
	Source      string
	Title       string
	ArtistNames []string
	ReleaseYear int
	Confidence  float64
}
