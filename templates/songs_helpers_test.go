package templates

import "testing"

func TestJSONStringEscapesPlaylistSort(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "added_desc", want: `"added_desc"`},
		{in: `x'y`, want: `"x'y"`},
		{in: `x"y\z`, want: `"x\"y\\z"`},
		{in: "a\nb", want: `"a\nb"`},
	}
	for _, tt := range tests {
		if got := jsonString(tt.in); got != tt.want {
			t.Fatalf("jsonString(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}
