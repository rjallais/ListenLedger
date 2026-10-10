package templates

import "testing"

func TestFormatUint(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{input: 0, want: "0"},
		{input: 42, want: "42"},
		{input: 1000, want: "1000"},
	}

	for _, tt := range tests {
		got := FormatUint(tt.input)
		if got != tt.want {
			t.Errorf("FormatUint(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatInt64(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{input: 0, want: "0"},
		{input: -1, want: "-1"},
		{input: 999, want: "999"},
	}

	for _, tt := range tests {
		got := FormatInt64(tt.input)
		if got != tt.want {
			t.Errorf("FormatInt64(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
