package templates

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/benbjohnson/hashfs"
)

var (
	assetMu  sync.RWMutex
	assetFS  *hashfs.FS
	assetDir string
)

func init() {
	dir := os.Getenv("STATIC_DIR")
	if dir == "" {
		dir = "static"
	}
	SetAssetDir(dir)
}

// SetAssetDir sets the filesystem directory used for content hashing of static assets.
func SetAssetDir(dir string) {
	assetMu.Lock()
	defer assetMu.Unlock()
	assetDir = dir
	assetFS = hashfs.NewFS(os.DirFS(dir))
}

// AssetURL returns a cache-busted, content-hashed URL path for a static asset.
// In production, the URL includes the SHA-256 digest (e.g., "/static/styles-9d2a...css").
// In development, the hash is dynamically recalculated so changes are immediately visible.
// If the file is not found or hashing fails, it safely falls back to "/static/" + path.
func AssetURL(path string) string {
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimPrefix(path, "static/")

	assetMu.RLock()
	hfs := assetFS
	dir := assetDir
	assetMu.RUnlock()

	if hfs == nil {
		return "/static/" + path
	}

	// In development, recreate the hashfs view so hot-reloaded stylesheets or scripts get fresh hashes.
	if IsDevEnvironment() {
		hfs = hashfs.NewFS(os.DirFS(dir))
	}

	hashedName := hfs.HashName(filepath.ToSlash(path))
	return "/static/" + hashedName
}
