//go:build goexperiment.jsonv2

package handlers

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/pocketbase/pocketbase/core"
)

const (
	// staticCacheControl matches the previous Cache-Control on static files.
	staticCacheControl = "public, max-age=3600"
	// staticBrotliLevel mirrors the brotli level used for SSE responses
	// (see sseOpts in shared.go) so both channels compress consistently.
	staticBrotliLevel = 5
	// staticMinCompressSize avoids paying compression overhead (CPU + gzip/brotli
	// framing) for files too small to benefit.
	staticMinCompressSize = 1024
)

// staticEncoded caches the compressed variants of a static file, keyed by path.
// Rebuilt lazily when the file's modification time changes.
type staticEncoded struct {
	mod    time.Time
	brotli []byte
	gzip   []byte
}

var (
	staticEncodedMu    sync.Mutex
	staticEncodedCache = map[string]*staticEncoded{}
)

// encodeStatic reads the file at fullPath once and returns cached brotli+gzip
// variants for it, recompressing if the file changed on disk.
func encodeStatic(fullPath string, fi os.FileInfo) (*staticEncoded, error) {
	mod := fi.ModTime().UTC()

	staticEncodedMu.Lock()
	defer staticEncodedMu.Unlock()

	if enc, ok := staticEncodedCache[fullPath]; ok && enc.mod.Equal(mod) {
		return enc, nil
	}

	raw, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read static file %q: %w", fullPath, err)
	}

	enc := &staticEncoded{mod: mod}

	if len(raw) >= staticMinCompressSize {
		var brBuf bytes.Buffer
		bw := brotli.NewWriterLevel(&brBuf, staticBrotliLevel)
		if _, err := bw.Write(raw); err != nil {
			return nil, fmt.Errorf("brotli compress %q: %w", fullPath, err)
		}
		if err := bw.Close(); err != nil {
			return nil, fmt.Errorf("brotli close %q: %w", fullPath, err)
		}
		enc.brotli = brBuf.Bytes()

		var gzBuf bytes.Buffer
		gz := gzip.NewWriter(&gzBuf)
		if _, err := gz.Write(raw); err != nil {
			return nil, fmt.Errorf("gzip compress %q: %w", fullPath, err)
		}
		if err := gz.Close(); err != nil {
			return nil, fmt.Errorf("gzip close %q: %w", fullPath, err)
		}
		enc.gzip = gzBuf.Bytes()
	}

	staticEncodedCache[fullPath] = enc
	return enc, nil
}

// negotiateEncoding picks brotli, then gzip, from the client's Accept-Encoding
// header, respecting q-values. It returns "" to serve the raw file.
func negotiateEncoding(r *http.Request) string {
	header := r.Header.Get("Accept-Encoding")
	if header == "" {
		return ""
	}

	// br is preferred over gzip when both are offered at equal quality
	// (that is what the datastar-go SDK does with WithClientPriority)
	priority := map[string]int{"br": 2, "gzip": 1}

	var best string
	bestQ := 0.0

	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		name := part
		q := 1.0
		if i := strings.Index(part, ";"); i >= 0 {
			name = part[:i]
			// parse q-value (only if it matches the standard ";q=0.x" form)
			for _, param := range strings.Split(part[i+1:], ";") {
				param = strings.TrimSpace(param)
				if rest, ok := strings.CutPrefix(param, "q="); ok {
					if parsed, err := strconv.ParseFloat(rest, 64); err == nil {
						q = parsed
					}
				}
			}
		}

		p, ok := priority[strings.TrimSpace(name)]
		if !ok || q <= 0 {
			continue
		}

		if q > bestQ || (q == bestQ && p > priority[best]) {
			best, bestQ = strings.TrimSpace(name), q
		}
	}

	return best
}

// handleStatic serves files from h.staticDir with binary-level compression:
// brotli for clients that accept it, gzip as the common fallback, and the raw
// file otherwise (including Range/conditional requests).
func (h *Handler) handleStatic(e *core.RequestEvent) error {
	name := strings.TrimPrefix(e.Request.PathValue("path"), "/")
	if name == "" ||
		strings.Contains(name, "..") ||
		strings.HasPrefix(name, "\\") ||
		filepath.IsAbs(name) {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "static file not found"})
	}

	fullPath := filepath.Join(h.staticDir, filepath.FromSlash(name))

	// Make sure the resolved path cannot escape the static directory even with
	// odd inputs (we already rejected ".."; this additionally resolves symlinks
	// so a link inside static/ cannot point outside of it).
	resolved, err := filepath.Abs(fullPath)
	if err != nil {
		return fmt.Errorf("resolve static path %q: %w", fullPath, err)
	}
	if !pathWithinDir(resolved, h.staticDirAbs) {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "static file not found"})
	}

	fi, err := os.Stat(resolved)
	if err != nil {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "static file not found"})
	}
	if fi.IsDir() {
		return e.JSON(http.StatusNotFound, map[string]string{"error": "static file not found"})
	}

	e.Response.Header().Set("Cache-Control", staticCacheControl)
	e.Response.Header().Add("Vary", "Accept-Encoding")

	// Shared with FileFS: set Last-Modified on every response so conditional
	// GETs work for both the compressed and raw serving paths.
	//
	// validator is truncated to whole seconds to match http.TimeFormat, which
	// drops fractional seconds. Comparing If-Modified-Since against the full
	// precision mod would let a client that echoes the header back receive
	// 200 OK instead of 304 (fractional part makes mod strictly after t).
	mod := fi.ModTime().UTC()
	validator := mod.Truncate(time.Second)
	e.Response.Header().Set("Last-Modified", validator.Format(http.TimeFormat))

	// Small files, unknown encodings and Range requests are served raw through
	// the existing FileFS path (stdlib http.ServeContent handles the details).
	enc := negotiateEncoding(e.Request)
	if enc == "" || fi.Size() < staticMinCompressSize ||
		e.Request.Header.Get("Range") != "" {
		return h.serveRawStatic(e, name)
	}

	encoded, err := encodeStatic(resolved, fi)
	if err != nil {
		return err
	}

	var body []byte
	switch enc {
	case "br":
		body = encoded.brotli
	case "gzip":
		body = encoded.gzip
	}
	if body == nil {
		// file is inside the min-size threshold for a variant; serve raw
		return h.serveRawStatic(e, name)
	}

	// Conditional GET support (Last-Modified), mirrors ServeContent behavior.
	if ims := e.Request.Header.Get("If-Modified-Since"); ims != "" {
		if t, parseErr := http.ParseTime(ims); parseErr == nil && !validator.After(t) {
			e.Response.WriteHeader(http.StatusNotModified)
			return nil
		}
	}

	ct := mime.TypeByExtension(filepath.Ext(fi.Name()))
	if ct == "" {
		ct = "application/octet-stream"
	}

	e.Response.Header().Set("Content-Type", ct)
	e.Response.Header().Set("Content-Encoding", enc)
	e.Response.Header().Set("Content-Length", strconv.Itoa(len(body)))
	e.Response.WriteHeader(http.StatusOK)

	if _, err := e.Response.Write(body); err != nil {
		return fmt.Errorf("write static file %q: %w", name, err)
	}
	return nil
}

func (h *Handler) serveRawStatic(e *core.RequestEvent, name string) error {
	// FileFS handles Last-Modified, Content-Type and If-Modified-Since.
	return e.FileFS(os.DirFS(h.staticDir), name)
}

// pathWithinDir reports whether resolved stays inside dirAbs after symlink
// resolution. A missing file is reported as outside so the caller can 404.
func pathWithinDir(resolved, dirAbs string) bool {
	realResolved, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		return false
	}
	realDir, err := filepath.EvalSymlinks(dirAbs)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(realDir, realResolved)
	if err != nil {
		return false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return false
	}
	return true
}
