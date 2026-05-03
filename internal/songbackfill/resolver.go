//go:build goexperiment.jsonv2

// Package songbackfill provides utilities for resolving and backfilling
// artist metadata on song records using stored artist data and external
// track metadata lookups.
package songbackfill

import (
	"context"
	"fmt"
	"strings"
)

type Resolver struct {
	index              artistIndex
	namePrefillLookup  TrackMetadataLookup
	trackLookup        TrackMetadataLookup
	minimumConfidence  float64
}

func NewResolver(artists []ArtistInput, opts Options) *Resolver {
	minimumConfidence := opts.MinimumConfidence
	if minimumConfidence <= 0 {
		minimumConfidence = 0.90
	}

	return &Resolver{
		index:             newArtistIndex(artists),
		namePrefillLookup: opts.NamePrefillLookup,
		trackLookup:       opts.TrackLookup,
		minimumConfidence: minimumConfidence,
	}
}

func (r *Resolver) MinimumConfidence() float64 {
	return r.minimumConfidence
}

func (r *Resolver) Resolve(ctx context.Context, song SongInput) Resolution {
	resolution := Resolution{
		SongID:                  song.ID,
		Title:                   strings.TrimSpace(song.Title),
		ReleaseDate:             strings.TrimSpace(song.ReleaseDate),
		OriginalArtistName:      strings.TrimSpace(song.ArtistName),
		OriginalArtistSpotifyIDs: strings.TrimSpace(song.ArtistSpotifyIDs),
		Action:                  ActionSkipUnresolved,
	}

	if resolution.OriginalArtistSpotifyIDs != "" {
		resolution.Action = ActionSkipExisting
		resolution.Confidence = 1
		resolution.Notes = append(resolution.Notes, "artist_spotify_ids already populated")
		return resolution
	}

	parsed := parseStoredArtists(song.ArtistName)
	if len(parsed.Names) == 0 && parsed.PrimaryPrefix == "" {
		resolution.Notes = append(resolution.Notes, "artist_name is empty after normalization")
		return resolution
	}

	if done := r.applyStoredNameResolution(&resolution, parsed.Names); done {
		return resolution
	}

	if prefilled, ok := r.resolveViaNamePrefill(ctx, song, parsed, resolution); ok {
		return prefilled
	} else {
		resolution.Notes = append(resolution.Notes, prefilled.Notes...)
		resolution.ExternalCandidates = prefilled.ExternalCandidates
	}

	if !parsed.HasEllipsis {
		r.notePrefixMatch(&resolution, parsed.PrimaryPrefix)
		return resolution
	}

	return r.resolveEllipsis(ctx, song, parsed, &resolution)
}

func (r *Resolver) notePrefixMatch(resolution *Resolution, prefix string) {
	if prefix == "" {
		return
	}
	artist, ok := r.index.resolvePrefix(prefix)
	if !ok {
		return
	}
	resolution.Notes = append(
		resolution.Notes,
		fmt.Sprintf("closest prefix match is %q (%s) but collaborators remain unresolved", artist.Name, artist.SpotifyID),
	)
}

func (r *Resolver) resolveEllipsis(ctx context.Context, song SongInput, parsed parsedArtists, resolution *Resolution) Resolution {
	if parsed.PrimaryPrefix == "" {
		resolution.Notes = append(resolution.Notes, "ellipsis artist name has no usable primary artist prefix")
		return *resolution
	}

	if artist, ok := r.index.resolvePrefix(parsed.PrimaryPrefix); ok {
		resolution.Notes = append(
			resolution.Notes,
			fmt.Sprintf("primary artist prefix uniquely matches %q (%s)", artist.Name, artist.SpotifyID),
		)
	}

	if r.trackLookup == nil {
		resolution.Notes = append(resolution.Notes, "no external track metadata lookup configured")
		return *resolution
	}

	candidates, err := r.trackLookup.Lookup(ctx, song, parsed.PrimaryPrefix)
	if err != nil {
		resolution.Notes = append(resolution.Notes, fmt.Sprintf("external lookup failed: %v", err))
		return *resolution
	}

	candidates, done := r.applyExternalEllipsisFilter(resolution, parsed.HasEllipsis, candidates)
	if done {
		return *resolution
	}

	return r.applyTrackCandidateMatch(resolution, candidates)
}

func (r *Resolver) applyTrackCandidateMatch(resolution *Resolution, candidates []TrackCandidate) Resolution {
	candidate, ambiguous, notes, ok := selectTrackCandidate(candidates)
	resolution.Notes = append(resolution.Notes, notes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
	}
	if !ok {
		return *resolution
	}

	matches, ambiguous, notes, ok := r.matchNames(candidate.ArtistNames)
	resolution.Notes = append(resolution.Notes, notes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
	}
	if !ok {
		r.applyPartialMatchUpdate(resolution, candidate)
		return *resolution
	}

	if candidate.Confidence < r.minimumConfidence {
		resolution.Action = ActionSkipLowConfidence
		resolution.Notes = append(resolution.Notes, fmt.Sprintf("candidate confidence %.2f below threshold %.2f", candidate.Confidence, r.minimumConfidence))
		return *resolution
	}

	resolution.applyMatches(matches, candidate.Source, candidate.Confidence)
	return *resolution
}

func (r *Resolver) applyStoredNameResolution(resolution *Resolution, names []string) bool {
	matches, ambiguous, notes, ok := r.matchNames(names)
	resolution.Notes = append(resolution.Notes, notes...)
	if ok {
		resolution.applyMatches(matches, "stored_artist_name", confidenceForMatches(matches))
		return true
	}
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
	}
	return false
}

func (r *Resolver) applyExternalEllipsisFilter(resolution *Resolution, hasEllipsis bool, candidates []TrackCandidate) ([]TrackCandidate, bool) {
	if hasEllipsis {
		candidates = filterCandidatesWithAdditionalArtists(candidates)
		if len(candidates) == 0 {
			resolution.Notes = append(
				resolution.Notes,
				NoteEllipsisMultiArtistNotFound,
			)
			return nil, true
		}
	}
	resolution.ExternalCandidates = summarizeCandidates(candidates)
	return candidates, false
}

func (r *Resolver) applyPartialMatchUpdate(resolution *Resolution, candidate TrackCandidate) {
	partialMatches, partialNotes, partialAmbiguous := r.matchNamesAllowPartial(candidate.ArtistNames)
	resolution.Notes = append(resolution.Notes, partialNotes...)
	if partialAmbiguous {
		resolution.Action = ActionSkipAmbiguous
		return
	}
	if len(partialMatches) > 0 && candidate.Confidence >= r.minimumConfidence {
		resolution.Action = ActionUpdateNameOnly
		resolution.Strategy = candidate.Source
		resolution.Confidence = candidate.Confidence
		resolution.MatchedArtists = partialMatches
		dedupedNames := dedupeArtistNames(candidate.ArtistNames)
		resolution.UpdatedArtistName = strings.Join(dedupedNames, ", ")
		resolution.UpdatedArtistSpotifyIDs = ""
		resolution.Notes = append(
			resolution.Notes,
			fmt.Sprintf("matched %d of %d artists; keeping artist_name update for later Spotify ID backfill", len(partialMatches), len(dedupedNames)),
		)
	}
}

func (r *Resolver) resolveViaNamePrefill(ctx context.Context, song SongInput, parsed parsedArtists, base Resolution) (Resolution, bool) {
	if r.namePrefillLookup == nil {
		return Resolution{}, false
	}
	if !requiresMultiArtistPrefill(song.ArtistName) {
		return Resolution{}, false
	}

	primaryArtistPrefix := parsed.PrimaryPrefix
	if primaryArtistPrefix == "" && len(parsed.Names) > 0 {
		primaryArtistPrefix = parsed.Names[0]
	}

	candidates, err := r.namePrefillLookup.Lookup(ctx, song, primaryArtistPrefix)
	resolution := base
	if err != nil {
		resolution.Notes = append(resolution.Notes, fmt.Sprintf("tidal prefill lookup failed: %v", err))
		return resolution, false
	}
	if len(candidates) == 0 {
		return Resolution{}, false
	}

	multiArtistCandidates := filterCandidatesForStoredMultiplicity(song.ArtistName, candidates)
	if len(multiArtistCandidates) == 0 {
		resolution.ExternalCandidates = summarizeCandidates(candidates)
		resolution.Notes = append(resolution.Notes, "tidal prefill did not yield a safe multi-artist expansion")
		return resolution, false
	}

	return r.applyPrefillCandidate(resolution, song, multiArtistCandidates)
}

func (r *Resolver) applyPrefillCandidate(resolution Resolution, song SongInput, candidates []TrackCandidate) (Resolution, bool) {
	resolution.ExternalCandidates = summarizeCandidates(candidates)
	candidate, ambiguous, notes, ok := selectTrackCandidate(candidates)
	resolution.Notes = append(resolution.Notes, notes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
		return resolution, true
	}
	if !ok {
		return resolution, false
	}

	prefilledNames := dedupeArtistNames(candidate.ArtistNames)
	if !preservesStoredMultiplicity(song.ArtistName, prefilledNames) {
		resolution.Notes = append(resolution.Notes, "tidal prefill candidate would collapse a known multi-artist credit")
		return resolution, false
	}

	resolution.UpdatedArtistName = strings.Join(prefilledNames, ", ")
	resolution.Strategy = candidate.Source + "_prefill"
	resolution.Confidence = candidate.Confidence
	resolution.Notes = append(
		resolution.Notes,
		fmt.Sprintf("prefilled artist_name from %s with confidence %.2f", candidate.Source, candidate.Confidence),
	)

	return r.applyPrefillMatches(resolution, candidate, prefilledNames)
}

func (r *Resolver) applyPrefillMatches(resolution Resolution, candidate TrackCandidate, prefilledNames []string) (Resolution, bool) {
	matches, ambiguous, matchNotes, ok := r.matchNames(prefilledNames)
	resolution.Notes = append(resolution.Notes, matchNotes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
		return resolution, true
	}
	if ok {
		effectiveConf := minFloat(candidate.Confidence, confidenceForMatches(matches))
		if effectiveConf < r.minimumConfidence {
			resolution.Action = ActionSkipLowConfidence
			resolution.Notes = append(resolution.Notes, fmt.Sprintf("effective confidence %.2f below threshold %.2f", effectiveConf, r.minimumConfidence))
			return resolution, true
		}
		resolution.applyMatches(matches, candidate.Source+"_prefill", effectiveConf)
		return resolution, true
	}

	partialMatches, partialNotes, partialAmbiguous := r.matchNamesAllowPartial(prefilledNames)
	resolution.Notes = append(resolution.Notes, partialNotes...)
	if partialAmbiguous {
		resolution.Action = ActionSkipAmbiguous
		return resolution, true
	}

	r.applyPrefillNameOnly(&resolution, candidate, prefilledNames, partialMatches)
	return resolution, true
}

func (r *Resolver) applyPrefillNameOnly(resolution *Resolution, candidate TrackCandidate, prefilledNames []string, partialMatches []ArtistMatch) {
	if len(partialMatches) > 0 && candidate.Confidence >= r.minimumConfidence {
		resolution.Action = ActionUpdateNameOnly
		resolution.UpdatedArtistSpotifyIDs = ""
		resolution.MatchedArtists = partialMatches
		resolution.Notes = append(
			resolution.Notes,
			fmt.Sprintf("prefill matched %d of %d artists; keeping artist_name update for later Spotify ID backfill", len(partialMatches), len(prefilledNames)),
		)
		return
	}
	if candidate.Confidence >= r.minimumConfidence {
		resolution.Notes = append(
			resolution.Notes,
			"prefill candidate did not match any known artists; manual review required before updating artist_name",
		)
		return
	}
	resolution.Action = ActionSkipLowConfidence
	resolution.Notes = append(resolution.Notes, "tidal prefill candidate requires manual review before updating artist_name")
}

type matchNamesResult struct {
	matches   []ArtistMatch
	notes     []string
	ambiguous bool
	ok        bool
}

func resolveArtistNames(index *artistIndex, names []string, allowPartial bool) matchNamesResult {
	if len(names) == 0 {
		return matchNamesResult{}
	}

	matches := make([]ArtistMatch, 0, len(names))
	notes := []string{}
	seenSpotifyIDs := map[string]bool{}
	missing := false

	for _, name := range names {
		match, ambiguous, matchNotes, ok := index.resolveExact(name)
		notes = append(notes, matchNotes...)

		if ambiguous {
			return matchNamesResult{notes: notes, ambiguous: true}
		}

		if !ok {
			missing = true
			if !allowPartial {
				return matchNamesResult{notes: notes}
			}
			continue
		}

		if !seenSpotifyIDs[match.SpotifyID] {
			seenSpotifyIDs[match.SpotifyID] = true
			matches = append(matches, match)
		}
	}

	return finalizeMatchResult(matches, notes, missing, allowPartial)
}

func finalizeMatchResult(matches []ArtistMatch, notes []string, missing, allowPartial bool) matchNamesResult {
	if len(matches) == 0 {
		return matchNamesResult{notes: notes}
	}
	if allowPartial && !missing {
		return matchNamesResult{notes: notes}
	}
	return matchNamesResult{matches: matches, notes: notes, ok: true}
}

func (r *Resolver) matchNames(names []string) ([]ArtistMatch, bool, []string, bool) {
	result := resolveArtistNames(&r.index, names, false)
	return result.matches, result.ambiguous, result.notes, result.ok
}

func (r *Resolver) matchNamesAllowPartial(names []string) ([]ArtistMatch, []string, bool) {
	result := resolveArtistNames(&r.index, names, true)
	return result.matches, result.notes, result.ambiguous
}

func confidenceForMatches(matches []ArtistMatch) float64 {
	if len(matches) == 0 {
		return 0
	}

	confidence := 0.98
	for _, match := range matches {
		switch match.MatchType {
		case "exact":
		case "loose":
			confidence = minFloat(confidence, 0.95)
		default:
			confidence = minFloat(confidence, 0.90)
		}
	}
	return confidence
}
