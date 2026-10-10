package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/benbjohnson/hashfs"
	"github.com/go-chi/chi/v5"

	"ListenLedger/config"
	"ListenLedger/templates"
)

func newTestHandler() *Handler {
	staticDir := "static"
	if _, err := os.Stat(staticDir); err != nil {
		if _, err := os.Stat("../../static"); err == nil {
			staticDir = "../../static"
		}
	}
	return New(nil, nil, nil, &config.Config{
		StaticDir: staticDir,
	})
}

func TestChiRouter_Robots(t *testing.T) {
	h := newTestHandler()
	r := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/robots.txt", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "User-agent: *") {
		t.Fatalf("expected robots.txt to contain 'User-agent: *', got: %s", body)
	}
}

func TestChiRouter_HealthEndpoint(t *testing.T) {
	h := newTestHandler()
	r := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/api/listenledger/health", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var data map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if data["app"] != "ListenLedger" {
		t.Fatalf("expected app 'ListenLedger', got %v", data["app"])
	}
	if data["status"] != "ok" {
		t.Fatalf("expected status 'ok', got %v", data["status"])
	}
}

func TestChiRouter_QuotaEndpoint(t *testing.T) {
	h := newTestHandler()
	r := h.Routes()

	req := httptest.NewRequest(http.MethodGet, "/api/quota", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var data map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if _, ok := data["providers"]; !ok {
		t.Fatalf("expected 'providers' key in response, got %v", data)
	}
}

func TestChiRouter_GetRouteParam(t *testing.T) {
	r := chi.NewRouter()
	var capturedID, capturedStatus string

	r.Get("/test/{albumId}/status/{status}", func(w http.ResponseWriter, req *http.Request) {
		capturedID = getRouteParam(req, "albumId")
		capturedStatus = getRouteParam(req, "status")
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test/rec12345/status/waiting", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if capturedID != "rec12345" {
		t.Fatalf("expected capturedID 'rec12345', got %q", capturedID)
	}
	if capturedStatus != "waiting" {
		t.Fatalf("expected capturedStatus 'waiting', got %q", capturedStatus)
	}
}

func TestChiRouter_RouteMatching(t *testing.T) {
	h := newTestHandler()
	r := h.Routes()

	tests := []struct {
		name         string
		method       string
		path         string
		expectStatus int
	}{
		{"robots", http.MethodGet, "/robots.txt", http.StatusOK},
		{"health", http.MethodGet, "/api/listenledger/health", http.StatusOK},
		{"quota", http.MethodGet, "/api/quota", http.StatusOK},
		{"admin status", http.MethodGet, "/api/admin/status", http.StatusOK},
		{"not found route", http.MethodGet, "/api/nonexistent/endpoint", http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()

			r.ServeHTTP(rec, req)

			if rec.Code != tc.expectStatus {
				t.Errorf("%s %s: expected status %d, got %d", tc.method, tc.path, tc.expectStatus, rec.Code)
			}
		})
	}
}

func TestGetRouteParam_FallbackPathValue(t *testing.T) {
	// Tests fallback when not using chi URLParam context
	req := httptest.NewRequest(http.MethodGet, "/example", nil)
	req.SetPathValue("foo", "bar")

	val := getRouteParam(req, "foo")
	if val != "bar" {
		t.Fatalf("expected 'bar', got %q", val)
	}
}

func TestParamParsingHelpers(t *testing.T) {
	// 1. getRouteParamInt
	req := httptest.NewRequest(http.MethodGet, "/items/42", nil)
	req.SetPathValue("id", "42")
	id, err := getRouteParamInt(req, "id")
	if err != nil || id != 42 {
		t.Fatalf("expected id 42, got %d (err: %v)", id, err)
	}

	reqBad := httptest.NewRequest(http.MethodGet, "/items/abc", nil)
	reqBad.SetPathValue("id", "abc")
	if _, err := getRouteParamInt(reqBad, "id"); err == nil {
		t.Fatal("expected error parsing non-integer route param")
	}

	reqMissing := httptest.NewRequest(http.MethodGet, "/items", nil)
	if _, err := getRouteParamInt(reqMissing, "missing"); err == nil {
		t.Fatal("expected error for missing route param")
	}

	// 2. getQueryParam
	reqQuery := httptest.NewRequest(http.MethodGet, "/test?q=hello&empty=", nil)
	if got := getQueryParam(reqQuery, "q", "fallback"); got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
	if got := getQueryParam(reqQuery, "empty", "fallback"); got != "fallback" {
		t.Fatalf("expected 'fallback', got %q", got)
	}
	if got := getQueryParam(reqQuery, "absent", "fallback"); got != "fallback" {
		t.Fatalf("expected 'fallback', got %q", got)
	}

	// 3. parseBoundedInt
	if got := parseBoundedInt("50", 10, 1, 100); got != 50 {
		t.Fatalf("expected 50, got %d", got)
	}
	if got := parseBoundedInt("-5", 10, 0, 100); got != 0 {
		t.Fatalf("expected clamped min 0, got %d", got)
	}
	if got := parseBoundedInt("150", 10, 1, 100); got != 100 {
		t.Fatalf("expected clamped max 100, got %d", got)
	}
	if got := parseBoundedInt("invalid", 10, 1, 100); got != 10 {
		t.Fatalf("expected fallback 10, got %d", got)
	}
	if got := parseBoundedInt("", 10, 1, 100); got != 10 {
		t.Fatalf("expected fallback 10, got %d", got)
	}
	// No upper bound when maxVal <= 0
	if got := parseBoundedInt("999999", 10, 0, 0); got != 999999 {
		t.Fatalf("expected 999999 when maxVal <= 0, got %d", got)
	}

	// 4. getQueryParamInt
	reqPagination := httptest.NewRequest(http.MethodGet, "/test?offset=25&limit=250", nil)
	if got := getQueryParamInt(reqPagination, "offset", 0, 0, 0); got != 25 {
		t.Fatalf("expected offset 25, got %d", got)
	}
	if got := getQueryParamInt(reqPagination, "limit", 50, 1, 100); got != 100 {
		t.Fatalf("expected clamped limit 100, got %d", got)
	}
	if got := getQueryParamInt(reqPagination, "missing", 15, 1, 100); got != 15 {
		t.Fatalf("expected fallback limit 15, got %d", got)
	}
}

func TestHandleStatic_HashedAndUnhashed(t *testing.T) {
	h := newTestHandler()
	r := h.Routes()

	// 1. Unhashed request
	req := httptest.NewRequest(http.MethodGet, "/static/datastar.js", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /static/datastar.js, got %d", rec.Code)
	}
	cc := rec.Header().Get("Cache-Control")
	if strings.Contains(cc, "immutable") {
		t.Fatalf("unhashed asset should not have immutable Cache-Control: %s", cc)
	}

	// 2. Hashed request via AssetURL
	hashedURL := templates.AssetURL("datastar.js")
	if !strings.HasPrefix(hashedURL, "/static/datastar-") {
		t.Fatalf("expected hashed url for datastar.js, got: %s", hashedURL)
	}

	_, hash := hashfs.ParseName(strings.TrimPrefix(hashedURL, "/static/"))
	if hash == "" {
		t.Fatalf("expected valid hash in URL: %s", hashedURL)
	}

	reqHashed := httptest.NewRequest(http.MethodGet, hashedURL, nil)
	recHashed := httptest.NewRecorder()
	r.ServeHTTP(recHashed, reqHashed)

	if recHashed.Code != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", hashedURL, recHashed.Code)
	}
	ccHashed := recHashed.Header().Get("Cache-Control")
	if !strings.Contains(ccHashed, "immutable") || !strings.Contains(ccHashed, "max-age=31536000") {
		t.Fatalf("expected immutable 1-year Cache-Control for hashed asset, got: %s", ccHashed)
	}
	etag := recHashed.Header().Get("ETag")
	if etag != `"`+hash+`"` {
		t.Fatalf("expected ETag %q, got %q", `"`+hash+`"`, etag)
	}

	// 3. Conditional 304 revalidation
	reqConditional := httptest.NewRequest(http.MethodGet, hashedURL, nil)
	reqConditional.Header.Set("If-None-Match", `"`+hash+`"`)
	recConditional := httptest.NewRecorder()
	r.ServeHTTP(recConditional, reqConditional)

	if recConditional.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got %d", recConditional.Code)
	}

	// 4. Compressed hashed request includes encoding in ETag
	reqBr := httptest.NewRequest(http.MethodGet, hashedURL, nil)
	reqBr.Header.Set("Accept-Encoding", "br")
	recBr := httptest.NewRecorder()
	r.ServeHTTP(recBr, reqBr)

	if recBr.Code != http.StatusOK {
		t.Fatalf("expected 200 for compressed %s, got %d", hashedURL, recBr.Code)
	}
	etagBr := recBr.Header().Get("ETag")
	expectedBrETag := `"` + hash + `-br"`
	if etagBr != expectedBrETag {
		t.Fatalf("expected compressed ETag %q, got %q", expectedBrETag, etagBr)
	}

	// 5. Conditional 304 with compressed ETag
	reqBrCond := httptest.NewRequest(http.MethodGet, hashedURL, nil)
	reqBrCond.Header.Set("Accept-Encoding", "br")
	reqBrCond.Header.Set("If-None-Match", expectedBrETag)
	recBrCond := httptest.NewRecorder()
	r.ServeHTTP(recBrCond, reqBrCond)

	if recBrCond.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified for compressed asset, got %d", recBrCond.Code)
	}
}

func TestFormatReleaseDateForUI(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"2023-05-18", "18 May 2023"},
		{"2023-05", "May 2023"},
		{"2023", "2023"},
		{"", ""},
		{"invalid", "invalid"},
		{"18 May 2023", "18 May 2023"},
	}

	for _, tc := range tests {
		if got := formatReleaseDateForUI(tc.input); got != tc.want {
			t.Errorf("formatReleaseDateForUI(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestHotReloadEndpoint(t *testing.T) {
	t.Setenv("ENV", "development")
	h := newTestHandler()
	r := h.Routes()

	// 1. RemoteAddr empty -> forbidden
	reqEmpty := httptest.NewRequest(http.MethodGet, "/hotreload", nil)
	reqEmpty.RemoteAddr = ""
	recEmpty := httptest.NewRecorder()
	r.ServeHTTP(recEmpty, reqEmpty)
	if recEmpty.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for empty RemoteAddr, got %d", recEmpty.Code)
	}

	// 2. Non-loopback RemoteAddr -> forbidden
	reqExternal := httptest.NewRequest(http.MethodGet, "/hotreload", nil)
	reqExternal.RemoteAddr = "192.168.1.50:54321"
	recExternal := httptest.NewRecorder()
	r.ServeHTTP(recExternal, reqExternal)
	if recExternal.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for external RemoteAddr, got %d", recExternal.Code)
	}

	// 3. Loopback RemoteAddr -> allowed
	reqLoopback := httptest.NewRequest(http.MethodGet, "/hotreload", nil)
	reqLoopback.RemoteAddr = "127.0.0.1:54321"
	recLoopback := httptest.NewRecorder()
	r.ServeHTTP(recLoopback, reqLoopback)
	if recLoopback.Code != http.StatusOK {
		t.Fatalf("expected 200 for loopback RemoteAddr, got %d", recLoopback.Code)
	}
}
