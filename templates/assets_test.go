package templates

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAssetURL(t *testing.T) {
	tempDir := t.TempDir()
	cssContent := []byte("body { color: red; }")
	cssPath := filepath.Join(tempDir, "styles.css")
	if err := os.WriteFile(cssPath, cssContent, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	prevDir := os.Getenv("STATIC_DIR")
	if prevDir == "" {
		prevDir = "static"
	}
	t.Cleanup(func() { SetAssetDir(prevDir) })
	SetAssetDir(tempDir)

	url := AssetURL("styles.css")
	if !strings.HasPrefix(url, "/static/styles-") || !strings.HasSuffix(url, ".css") {
		t.Fatalf("expected hashed url for styles.css, got: %q", url)
	}

	// Non-existent file should fall back cleanly
	nonExistent := AssetURL("missing.js")
	if nonExistent != "/static/missing.js" {
		t.Fatalf("expected /static/missing.js, got: %q", nonExistent)
	}

	// Leading slash and static prefix should be handled gracefully
	urlWithPrefix := AssetURL("/static/styles.css")
	if urlWithPrefix != url {
		t.Fatalf("expected identical url with /static prefix, got: %q vs %q", urlWithPrefix, url)
	}
}
