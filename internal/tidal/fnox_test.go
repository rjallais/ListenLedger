package tidal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindProjectConfig(t *testing.T) {
	tmp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}

	// Case 1: no fnox.toml in tree
	t.Chdir(tmp)
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
	t.Chdir(subDir)
	got, err = findProjectConfig()
	if err != nil {
		t.Fatalf("findProjectConfig in sub-dir error: %v", err)
	}
	if got != fnoxPath {
		t.Errorf("expected %q, got %q", fnoxPath, got)
	}
}
