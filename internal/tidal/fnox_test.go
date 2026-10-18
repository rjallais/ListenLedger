package tidal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindProjectConfig(t *testing.T) {
	tmp := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() { _ = os.Chdir(origWd) }()

	// Case 1: no fnox.toml in tree
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	got, err := findProjectConfig()
	if err != nil {
		t.Fatalf("findProjectConfig error: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty config, got %q", got)
	}

	// Case 2: fnox.toml in parent dir, current is sub-dir
	fnoxPath := filepath.Join(tmp, "fnox.toml")
	if err := os.WriteFile(fnoxPath, []byte("[secrets]\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	subDir := filepath.Join(tmp, "sub", "deep")
	if err := os.MkdirAll(subDir, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Chdir(subDir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	got, err = findProjectConfig()
	if err != nil {
		t.Fatalf("findProjectConfig in sub-dir error: %v", err)
	}
	if got != fnoxPath {
		t.Errorf("expected %q, got %q", fnoxPath, got)
	}
}
