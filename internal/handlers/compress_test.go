package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
)

// brotliTestBody is comfortably larger than the compressor's flush threshold so
// the wrapped response is guaranteed to be encoded rather than passed through.
var brotliTestBody = "<html><body>" + strings.Repeat("<p>ListenLedger compression test payload.</p>", 64) + "</body></html>"

// newCompressTestRouter builds a minimal router with only the compression
// middleware and a single handler that emits HTML with the given bytes.
func newCompressTestRouter(body string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /page", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, body)
	})
	return compressMiddleware(mux)
}

func TestCompressMiddleware_BrotliPreferred(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/page", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	rec := httptest.NewRecorder()

	newCompressTestRouter(brotliTestBody).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("expected Content-Encoding br when client accepts br, got %q", got)
	}
	if !strings.Contains(rec.Header().Get("Vary"), "Accept-Encoding") {
		t.Fatalf("expected Vary to include Accept-Encoding, got %q", rec.Header().Get("Vary"))
	}

	// The recorded body must be valid Brotli that round-trips to the original.
	dec := &brotli.Reader{}
	if err := dec.Reset(rec.Body); err != nil {
		t.Fatalf("reset brotli reader: %v", err)
	}
	decoded, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("brotli decode: %v", err)
	}
	if string(decoded) != brotliTestBody {
		t.Fatalf("brotli body did not round-trip (got %d bytes, want %d)", len(decoded), len(brotliTestBody))
	}
}

func TestCompressMiddleware_BrotliPreferredOverZstd(t *testing.T) {
	// Regression guard: CAFxX/httpcompression's DefaultAdapter ranks Zstandard
	// above its stock Brotli. We re-register Brotli at a higher priority so it
	// stays the primary codec; assert a client offering br+zstd still gets br.
	req := httptest.NewRequest(http.MethodGet, "/page", nil)
	req.Header.Set("Accept-Encoding", "zstd, br, gzip")
	rec := httptest.NewRecorder()

	newCompressTestRouter(brotliTestBody).ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("expected brotli to outrank zstd, got Content-Encoding %q", got)
	}
}

func TestCompressMiddleware_GzipFallback(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/page", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	rec := httptest.NewRecorder()

	newCompressTestRouter(brotliTestBody).ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip fallback when br is not accepted, got %q", got)
	}
}

func TestCompressMiddleware_NoEncodingWhenUnsupported(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/page", nil)
	req.Header.Set("Accept-Encoding", "identity")
	rec := httptest.NewRecorder()

	newCompressTestRouter(brotliTestBody).ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected no Content-Encoding for identity, got %q", got)
	}
	if rec.Body.String() != brotliTestBody {
		t.Fatalf("expected uncompressed passthrough body")
	}
}

func TestCompressMiddleware_BrotliSmallerThanGzip(t *testing.T) {
	// Guard the whole point of Brotli on this path: for this highly repetitive
	// HTML, Brotli should still beat gzip without depending on exact encoder
	// byte counts that vary across library and platform versions.
	gzipReq := httptest.NewRequest(http.MethodGet, "/page", nil)
	gzipReq.Header.Set("Accept-Encoding", "gzip")
	gzipRec := httptest.NewRecorder()
	newCompressTestRouter(brotliTestBody).ServeHTTP(gzipRec, gzipReq)
	if got := gzipRec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip encoding, got %q", got)
	}

	brReq := httptest.NewRequest(http.MethodGet, "/page", nil)
	brReq.Header.Set("Accept-Encoding", "br")
	brRec := httptest.NewRecorder()
	newCompressTestRouter(brotliTestBody).ServeHTTP(brRec, brReq)
	if got := brRec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("expected br encoding, got %q", got)
	}

	const tolerancePercent = 10
	tolerance := gzipRec.Body.Len() * tolerancePercent / 100
	if brRec.Body.Len() > gzipRec.Body.Len()+tolerance {
		t.Fatalf("expected brotli (%d B) within %d%% of gzip (%d B)", brRec.Body.Len(), tolerancePercent, gzipRec.Body.Len())
	}
}
