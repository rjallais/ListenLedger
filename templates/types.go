package templates

import (
	"fmt"

	"ListenLedger/config"
)

const StatusWaiting = "waiting"

// IsDevEnvironment reports whether hot-reload dev affordances should render.
func IsDevEnvironment() bool {
	return config.IsDevEnvironment()
}

// Theme represents a UI theme
type Theme struct {
	Name  string
	Icon  string
	Value string
}

// NavItem represents a navigation menu item
type NavItem struct {
	Href   string
	Icon   string
	Label  string
	Target string // "_blank" for external links
	Active bool
}

// Album represents an album for display
type Album struct {
	ID         string
	Title      string
	ArtistName string
	Status     string

	CollectionSongs int
	TotalSongs      int
}

// Artist represents an artist for display
type Artist struct {
	ID        string
	Name      string
	SpotifyID string

	GenreGroup  string
	ListStatus  string
	FetchStatus string
	LastUpdated string

	MonthlyListeners int
	CollectionSongs  int
	TotalSongs       int
}

// Song represents a song for display
type Song struct {
	ID          string
	Title       string
	ArtistName  string
	ReleaseDate string
	ReleaseType string // "album", "ep", "single"
	Album       string

	BatchSeq int
	BatchPos int
	IsRecent bool
}

// Pagination holds pagination state
type Pagination struct {
	Genre       string
	CurrentPage int
	TotalPages  int
	Limit       int
	TotalCount  int
}

// FormatNumber formats an integer with thousand separators
func FormatNumber(n int) string {
	if n == 0 {
		return "—"
	}
	// Simple formatting with commas
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
