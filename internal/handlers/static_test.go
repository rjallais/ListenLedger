package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNegotiateEncoding(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{
			name:     "empty",
			header:   "",
			expected: "",
		},
		{
			name:     "both supported, equal preference selects br",
			header:   "gzip, deflate, br",
			expected: "br",
		},
		{
			name:     "gzip higher preference",
			header:   "gzip;q=1.0, br;q=0.5",
			expected: "gzip",
		},
		{
			name:     "wildcard selects br by default",
			header:   "*",
			expected: "br",
		},
		{
			name:     "wildcard with br refused selects gzip",
			header:   "*;q=0.8, br;q=0",
			expected: "gzip",
		},
		{
			name:     "wildcard with both refused selects none",
			header:   "*;q=0.8, br;q=0, gzip;q=0",
			expected: "",
		},
		{
			name:     "unsupported encodings only",
			header:   "deflate, zstd",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				req.Header.Set("Accept-Encoding", tc.header)
			}
			actual := negotiateEncoding(req)
			if actual != tc.expected {
				t.Fatalf("expected %q, got %q", tc.expected, actual)
			}
		})
	}
}
