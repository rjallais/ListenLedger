package songbackfill

import (
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// NormalizeEllipsis normalizes unicode ellipses to three dots.
func NormalizeEllipsis(value string) string {
	value = strings.ReplaceAll(value, "…", "...")
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func normalizeEllipsis(value string) string {
	return NormalizeEllipsis(value)
}

// NormalizeKey normalizes artist and title strings for fuzzy matching.
func NormalizeKey(value string) string {
	value = normalizeEllipsis(value)
	value = strings.ToLower(value)
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
		default:
			builder.WriteRune(r)
			lastSpace = false
		}
	}

	return strings.TrimSpace(strings.Join(strings.Fields(builder.String()), " "))
}

func normalizeKey(value string) string {
	return NormalizeKey(value)
}

func normalizeLooseKey(value string) string {
	value = normalizeKey(value)

	var builder strings.Builder
	lastSpace := false
	for _, r := range value {
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

func normalizeFlatKey(value string) string {
	value = normalizeLooseKey(value)
	if value == "" {
		return ""
	}
	return strings.ReplaceAll(value, " ", "")
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func nearlyEqualFloat(left, right float64) bool {
	const epsilon = 1e-9
	if left > right {
		return left-right < epsilon
	}
	return right-left < epsilon
}

func parseReleaseYear(value string) int {
	value = strings.TrimSpace(value)
	if len(value) < 4 {
		return 0
	}

	year, err := strconv.Atoi(value[:4])
	if err != nil || year < 1000 {
		return 0
	}
	return year
}

// ParseReleaseYear extracts the leading 4-digit year from a release date
// ("2019-01-10" -> 2019). Zero when no year is readable.
func ParseReleaseYear(value string) int {
	return parseReleaseYear(value)
}
