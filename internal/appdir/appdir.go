package appdir

import (
	"os"
	"path/filepath"
)

// ResolveDataDir returns a stable PocketBase data directory.
// It prefers PB_DATA_DIR, otherwise uses the repo root (go.mod) when available.
func ResolveDataDir() string {
	if dir := os.Getenv("PB_DATA_DIR"); dir != "" {
		return dir
	}

	if root := findProjectRoot(); root != "" {
		return filepath.Join(root, "pb_data")
	}

	if wd, err := os.Getwd(); err == nil {
		return filepath.Join(wd, "pb_data")
	}

	return "pb_data"
}

func findProjectRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}

	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return ""
}
