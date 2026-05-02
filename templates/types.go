//go:build goexperiment.jsonv2

package templates

import "fmt"

const StatusWaiting = "waiting"

// Theme represents a DaisyUI theme
type Theme struct {
	Name  string
	Icon  string
	Value string
}

var DarkThemes = []Theme{
	{Name: "Dark", Icon: "🌙", Value: "dark"},
	{Name: "Synthwave", Icon: "🌆", Value: "synthwave"},
	{Name: "Halloween", Icon: "🎃", Value: "halloween"},
	{Name: "Forest", Icon: "🌲", Value: "forest"},
	{Name: "Black", Icon: "⬛", Value: "black"},
	{Name: "Luxury", Icon: "💎", Value: "luxury"},
	{Name: "Dracula", Icon: "🧛", Value: "dracula"},
	{Name: "Business", Icon: "💼", Value: "business"},
	{Name: "Night", Icon: "🌃", Value: "night"},
	{Name: "Coffee", Icon: "☕", Value: "coffee"},
	{Name: "Dim", Icon: "🔅", Value: "dim"},
	{Name: "Sunset", Icon: "🌅", Value: "sunset"},
}

var LightThemes = []Theme{
	{Name: "Light", Icon: "☀️", Value: "light"},
	{Name: "Cupcake", Icon: "🧁", Value: "cupcake"},
	{Name: "Bumblebee", Icon: "🐝", Value: "bumblebee"},
	{Name: "Emerald", Icon: "💚", Value: "emerald"},
	{Name: "Corporate", Icon: "🏢", Value: "corporate"},
	{Name: "Retro", Icon: "📺", Value: "retro"},
	{Name: "Cyberpunk", Icon: "🤖", Value: "cyberpunk"},
	{Name: "Valentine", Icon: "💕", Value: "valentine"},
	{Name: "Garden", Icon: "🌷", Value: "garden"},
	{Name: "Lofi", Icon: "🎵", Value: "lofi"},
	{Name: "Pastel", Icon: "🎨", Value: "pastel"},
	{Name: "Fantasy", Icon: "🏰", Value: "fantasy"},
	{Name: "Wireframe", Icon: "📐", Value: "wireframe"},
	{Name: "CMYK", Icon: "🖨️", Value: "cmyk"},
	{Name: "Autumn", Icon: "🍂", Value: "autumn"},
	{Name: "Acid", Icon: "🧪", Value: "acid"},
	{Name: "Lemonade", Icon: "🍋", Value: "lemonade"},
	{Name: "Winter", Icon: "❄️", Value: "winter"},
	{Name: "Nord", Icon: "🏔️", Value: "nord"},
	{Name: "Aqua", Icon: "💧", Value: "aqua"},
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
