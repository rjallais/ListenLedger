package tidalsync

import (
	"testing"
)

func TestCleanAlbumTitle(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedTitle string
		expectedType  string
		hasEdition    bool
	}{
		{
			name:          "standard album unchanged",
			input:         "Mariner",
			expectedTitle: "Mariner",
			expectedType:  "",
			hasEdition:    false,
		},
		{
			name:          "deluxe in parentheses",
			input:         "B.o.B Presents: The Adventures of Bobby Ray (Deluxe)",
			expectedTitle: "B.o.B Presents: The Adventures of Bobby Ray",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "deluxe edition in parentheses",
			input:         "Awake (Deluxe Edition)",
			expectedTitle: "Awake",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "deluxe version in parentheses",
			input:         "The Tyranny of Will (Deluxe Version)",
			expectedTitle: "The Tyranny of Will",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "remastered in parentheses",
			input:         "Panopticon (Remastered)",
			expectedTitle: "Panopticon",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "special edition in brackets",
			input:         "Inflikted [Special Edition]",
			expectedTitle: "Inflikted",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "expanded in brackets",
			input:         "Bump Ahead [Expanded]",
			expectedTitle: "Bump Ahead",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "compound anniversary and deluxe",
			input:         "Ace of Spades (40th Anniversary Edition) [Deluxe]",
			expectedTitle: "Ace of Spades",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "remastered in brackets and parens",
			input:         "(What's The Story) Morning Glory? ([Remastered])",
			expectedTitle: "(What's The Story) Morning Glory?",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "preserves movie subtitle in parens but strips deluxe",
			input:         "Mac and Devin Go to High School (Music from and Inspired by the Movie) [Deluxe]",
			expectedTitle: "Mac and Devin Go to High School (Music from and Inspired by the Movie)",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "preserves title with unparenthesized deluxe word",
			input:         "Hellbilly Deluxe",
			expectedTitle: "Hellbilly Deluxe",
			expectedType:  "",
			hasEdition:    false,
		},
		{
			name:          "steven wilson mix and master",
			input:         "Aqualung (Steven Wilson Mix and Master)",
			expectedTitle: "Aqualung",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "deluxe edition slash remastered with year",
			input:         "Fallen (Deluxe Edition / Remastered 2023)",
			expectedTitle: "Fallen",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "reissue hyphen deluxe edition",
			input:         "Touch (Reissue - Deluxe Edition)",
			expectedTitle: "Touch",
			expectedType:  "",
			hasEdition:    true,
		},
		{
			name:          "single suffix with clean album",
			input:         "Chega de Saudade - Single",
			expectedTitle: "Chega de Saudade",
			expectedType:  "single",
			hasEdition:    false,
		},
		{
			name:          "ep suffix with clean album",
			input:         "Something Good - EP",
			expectedTitle: "Something Good",
			expectedType:  "ep",
			hasEdition:    false,
		},
		{
			name:          "hyphen deluxe edition",
			input:         "Album Name - Deluxe Edition",
			expectedTitle: "Album Name",
			expectedType:  "",
			hasEdition:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			title, relType, edition := CleanAlbumTitle(tt.input)
			if title != tt.expectedTitle {
				t.Errorf("CleanAlbumTitle(%q) title = %q, want %q", tt.input, title, tt.expectedTitle)
			}
			if relType != tt.expectedType {
				t.Errorf("CleanAlbumTitle(%q) relType = %q, want %q", tt.input, relType, tt.expectedType)
			}
			if (edition != "") != tt.hasEdition {
				t.Errorf("CleanAlbumTitle(%q) hasEdition = %v (edition=%q), want %v", tt.input, edition != "", edition, tt.hasEdition)
			}
		})
	}
}

func TestIsImpreciseReleaseDate(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"1980-01-01", true},
		{"1969-03-01", true},
		{"2015-01-01", true},
		{"1995-10-10", false},
		{"2018-05-23", false},
		{"2014-09-16", false},
		{"1984-02-17", false},
		{"2006", true},
		{"2006-05", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isImpreciseReleaseDate(tt.input)
			if got != tt.expected {
				t.Errorf("isImpreciseReleaseDate(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
