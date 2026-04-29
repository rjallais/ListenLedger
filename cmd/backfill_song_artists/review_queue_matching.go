//go:build goexperiment.jsonv2

package main

import (
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"ListenLedger/internal/songbackfill"
)

func suggestExistingArtists(inputName string, artists []songbackfill.ArtistInput) []reviewArtistSuggestion {
	inputKey := normalizeReviewKey(inputName)
	if inputKey == "" {
		return nil
	}

	suggestions := make([]reviewArtistSuggestion, 0, 6)
	for _, artist := range artists {
		artistKey := normalizeReviewKey(artist.Name)
		if artistKey == "" {
			continue
		}

		score := matchSuggestionScore(inputKey, artistKey)
		if score <= 0 {
			continue
		}

		suggestions = append(suggestions, reviewArtistSuggestion{
			InputName:          inputName,
			SuggestedName:      artist.Name,
			SuggestedSpotifyID: artist.SpotifyID,
			Score:              score,
		})
	}

	sort.SliceStable(suggestions, func(i, j int) bool {
		if suggestions[i].Score != suggestions[j].Score {
			return suggestions[i].Score > suggestions[j].Score
		}
		if suggestions[i].SuggestedName != suggestions[j].SuggestedName {
			return suggestions[i].SuggestedName < suggestions[j].SuggestedName
		}
		return suggestions[i].SuggestedSpotifyID < suggestions[j].SuggestedSpotifyID
	})

	if len(suggestions) > 5 {
		suggestions = suggestions[:5]
	}
	return suggestions
}

func matchSuggestionScore(inputKey, artistKey string) float64 {
	switch {
	case inputKey == artistKey:
		return 1.0
	case strings.HasPrefix(inputKey, artistKey), strings.HasPrefix(artistKey, inputKey):
		return 0.86
	}

	distance := levenshteinDistance(inputKey, artistKey)
	maxLength := max(len([]rune(inputKey)), len([]rune(artistKey)))
	if maxLength == 0 {
		return 0
	}

	similarity := 1 - float64(distance)/float64(maxLength)
	switch {
	case distance <= 1 && similarity >= 0.85:
		return 0.93
	case distance <= 2 && similarity >= 0.75:
		return 0.82
	case distance <= 3 && similarity >= 0.70:
		return 0.74
	default:
		return 0
	}
}

func normalizeReviewKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, "…", "...")
	value = norm.NFD.String(value)

	var builder strings.Builder
	lastSpace := false
	for _, r := range value {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			builder.WriteRune(r)
			lastSpace = false
		case unicode.IsSpace(r):
			if !lastSpace {
				builder.WriteByte(' ')
				lastSpace = true
			}
		}
	}

	return strings.TrimSpace(strings.Join(strings.Fields(builder.String()), " "))
}

func levenshteinDistance(left, right string) int {
	leftRunes := []rune(left)
	rightRunes := []rune(right)

	if len(leftRunes) == 0 {
		return len(rightRunes)
	}
	if len(rightRunes) == 0 {
		return len(leftRunes)
	}

	column := make([]int, len(rightRunes)+1)
	for j := range column {
		column[j] = j
	}

	for i, leftRune := range leftRunes {
		prevDiagonal := column[0]
		column[0] = i + 1
		for j, rightRune := range rightRunes {
			insertCost := column[j+1] + 1
			deleteCost := column[j] + 1
			replaceCost := prevDiagonal
			if leftRune != rightRune {
				replaceCost++
			}
			prevDiagonal = column[j+1]
			column[j+1] = min(insertCost, min(deleteCost, replaceCost))
		}
	}

	return column[len(rightRunes)]
}
