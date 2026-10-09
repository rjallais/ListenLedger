package handlers

import (
	"bytes"
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
	"github.com/benbjohnson/hashfs"
	"github.com/klauspost/compress/gzip"
	"github.com/pocketbase/pocketbase/core"
)

const (
	// staticCacheControl instructs browsers and proxies to cache static files
	// with revalidation. CSS/JS carry cache-busters or change on restart;
	// conditional requests (ETag/Last-Modified) return 304 without payload.
	staticCacheControl = "public, max-age=86400, stale-while-revalidate=604800"

	// staticMinCompressSize: files smaller than 512 B are not worth compressing
	// (headers and compression dictionary overhead exceed savings).
	staticMinCompressSize = 512
)

// staticEncoded holds the pre-compressed byte variants of a static file.
type staticEncoded struct {
	modTime time.Time
	size    int64
	brotli  []byte
	gzip    []byte
}

var (
	staticCacheMu sync.RWMutex
	staticCache   = make(map[string]*staticEncoded)
)

// encodeStatic returns cached or newly compressed variants for fullPath.
// Cache is invalidated when modTime or size differs from the cached entry.
func encodeStatic(fullPath string, fi os.FileInfo) (*staticEncoded, error) {
	staticCacheMu.RLock()
	cached, ok := staticCache[fullPath]
	staticCacheMu.RUnlock()

	if ok && cached.modTime.Equal(fi.ModTime()) && cached.size == fi.Size() {
		return cached, nil
	}

	raw, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read static file: %w", err)
	}

	var brBuf bytes.Buffer
	// Quality 5 is the optimal trade-off: 90%+ of max brotli compression at
	// near-gzip speed, safe to run dynamically without hurting CPU.
	brWriter := brotli.NewWriterLevel(&brBuf, 5)
	defer func() { _ = brWriter.Close() }()
	if _, err := brWriter.Write(raw); err != nil {
		return nil, fmt.Errorf("brotli compress %s: %w", fullPath, err)
	}
	if err := brWriter.Close(); err != nil {
		return nil, fmt.Errorf("brotli close %s: %w", fullPath, err)
	}

	var gzBuf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&gzBuf, gzip.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("gzip writer %s: %w", fullPath, err)
	}
	defer func() { _ = gzWriter.Close() }()
	if _, err := gzWriter.Write(raw); err != nil {
		return nil, fmt.Errorf("gzip compress %s: %w", fullPath, err)
	}
	if err := gzWriter.Close(); err != nil {
		return nil, fmt.Errorf("gzip close %s: %w", fullPath, err)
	}

	entry := &staticEncoded{
		modTime: fi.ModTime(),
		size:    fi.Size(),
		brotli:  brBuf.Bytes(),
		gzip:    gzBuf.Bytes(),
	}

	staticCacheMu.Lock()
	staticCache[fullPath] = entry
	staticCacheMu.Unlock()

	return entry, nil
}

// negotiateEncoding parses Accept-Encoding and returns "br" (priority 1),
// "gzip" (priority 2), or "" if neither is accepted or accepted with q=0.
func negotiateEncoding(r *http.Request) string {
	ae := r.Header.Get("Accept-Encoding")
	if ae == "" {
		return ""
	}

	type qval struct {
		enc string
		q   float64
	}
	var parsed []qval
	var refused map[string]bool

	for part := range strings.SplitSeq(ae, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		var enc string
		q := 1.0
		if semi := strings.IndexByte(part, ';'); semi >= 0 {
			enc = strings.TrimSpace(part[:semi])
			params := strings.TrimSpace(part[semi+1:])
			if strings.HasPrefix(params, "q=") {
				if parsedQ, err := strconv.ParseFloat(params[2:], 64); err == nil {
					q = parsedQ
				}
			}
		} else {
			enc = part
		}
		if q > 0 {
			parsed = append(parsed, qval{enc: enc, q: q})
		} else {
			if refused == nil {
				refused = make(map[string]bool)
			}
			refused[enc] = true
		}
	}

	// First pass: find highest-q match among supported encodings.
	// When q values are equal (e.g. "gzip, deflate, br"), give priority to br.
	best := ""
	bestQ := -1.0
	for _, p := range parsed {
		if refused[p.enc] {
			continue
		}
		switch p.enc {
		case "br":
			if p.q >= bestQ {
				best = "br"
				bestQ = p.q
			}
		case "gzip":
			// br has higher priority on tie
			if p.q > bestQ || (p.q == bestQ && best != "br") {
				best = "gzip"
				bestQ = p.q
			}
		case "*":
			if best == "" && p.q > 0 {
				if !refused["br"] {
					best = "br"
					bestQ = p.q
				} else if !refused["gzip"] {
					best = "gzip"
					bestQ = p.q
				}
			}
		}
	}
	return best
}

// HandleStatic serves files from h.staticDir with binary-level compression:
// brotli for clients that accept it, gzip as the common fallback, and the raw
// file otherwise (including Range/conditional requests).
// If the filename contains a valid content hash (generated via hashfs), it serves
// with immutable caching headers (Cache-Control: public, max-age=31536000, immutable)
// and handles fast ETag/If-None-Match revalidation.
func (h *Handler) HandleStatic(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(getRouteParam(r, "path"), "/")
	if name == "" ||
		strings.Contains(name, "..") ||
		strings.HasPrefix(name, "\\") ||
		filepath.IsAbs(name) {
		writeError(w, http.StatusNotFound, "static file not found")
		return
	}

	baseName, hash := hashfs.ParseName(name)
	isHashed := hash != ""
	if isHashed {
		name = baseName
	}

	fullPath := filepath.Join(h.staticDir, filepath.FromSlash(name))

	// Make sure the resolved path cannot escape the static directory even with
	// odd inputs.
	resolved, err := filepath.Abs(fullPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("resolve static path %q: %v", fullPath, err), http.StatusInternalServerError)
		return
	}
	if !pathWithinDir(resolved, h.staticDirAbs) {
		writeError(w, http.StatusNotFound, "static file not found")
		return
	}

	fi, err := os.Stat(resolved)
	if err != nil || fi.IsDir() {
		writeError(w, http.StatusNotFound, "static file not found")
		return
	}

	w.Header().Add("Vary", "Accept-Encoding")
	if isHashed {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Header().Set("ETag", `"`+hash+`"`)
		if inm := r.Header.Get("If-None-Match"); inm != "" {
			trimmed := strings.Trim(inm, `"`)
			if trimmed == hash || strings.HasPrefix(trimmed, hash+"-") {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
	} else {
		w.Header().Set("Cache-Control", staticCacheControl)
	}

	mod := fi.ModTime().UTC()
	validator := mod.Truncate(time.Second)
	w.Header().Set("Last-Modified", validator.Format(http.TimeFormat))

	enc := negotiateEncoding(r)
	if enc == "" || fi.Size() < staticMinCompressSize ||
		r.Header.Get("Range") != "" {
		h.serveRawStaticHTTP(w, r, name)
		return
	}

	encoded, err := encodeStatic(resolved, fi)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var body []byte
	switch enc {
	case "br":
		body = encoded.brotli
	case "gzip":
		body = encoded.gzip
	}
	if body == nil {
		h.serveRawStaticHTTP(w, r, name)
		return
	}

	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, parseErr := http.ParseTime(ims); parseErr == nil && !validator.After(t) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	ct := mime.TypeByExtension(filepath.Ext(fi.Name()))
	if ct == "" {
		ct = "application/octet-stream"
	}

	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Encoding", enc)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if isHashed {
		w.Header().Set("ETag", fmt.Sprintf(`"%s-%s"`, hash, enc))
	}
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(body)
}

func (h *Handler) handleStatic(e *core.RequestEvent) error {
	h.HandleStatic(e.Response, e.Request)
	return nil
}

func (h *Handler) serveRawStaticHTTP(w http.ResponseWriter, r *http.Request, name string) {
	http.ServeFileFS(w, r, os.DirFS(h.staticDir), name)
}

func (h *Handler) serveRawStatic(e *core.RequestEvent, name string) error {
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
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
