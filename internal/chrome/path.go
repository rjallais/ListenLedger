//go:build goexperiment.jsonv2

// Package chrome provides Chrome executable path resolution across platforms.
package chrome

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

// ResolvePath returns an executable path for Chrome/Chromium/Edge.
//
// Resolution order:
// 1. explicit path argument
// 2. CHROME_PATH / GOOGLE_CHROME_BIN environment variables
// 3. known executable names on PATH
// 4. platform-specific known install locations
func ResolvePath(explicit string) string {
	if p := strings.TrimSpace(explicit); p != "" {
		if canStartFromGo(p) {
			return p
		}
		return ""
	}

	for _, key := range []string{"CHROME_PATH", "GOOGLE_CHROME_BIN"} {
		if p := strings.TrimSpace(os.Getenv(key)); p != "" {
			if canStartFromGo(p) {
				return p
			}
		}
	}

	for _, name := range []string{
		"google-chrome",
		"google-chrome-stable",
		"chromium",
		"chromium-browser",
		"chrome",
		"microsoft-edge",
		"msedge",
	} {
		if p, err := exec.LookPath(name); err == nil {
			if canStartFromGo(p) {
				return p
			}
		}
	}

	for _, p := range platformInstallCandidates() {
		if _, err := os.Stat(p); err == nil {
			if canStartFromGo(p) {
				return p
			}
		}
	}

	return ""
}

func platformInstallCandidates() []string {
	switch runtime.GOOS {
	case "windows":
		return windowsInstallCandidates()
	case "darwin":
		return []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Google Chrome Dev.app/Contents/MacOS/Google Chrome Dev",
			"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
		}
	default:
		candidates := linuxInstallCandidates()
		// IMPORTANT:
		// When running on Linux (including WSL), auto-detecting Windows-mounted
		// *.exe Chrome/Edge binaries can cause a visible Windows Chrome window to
		// pop up even when chromedp requests headless mode.
		//
		// To avoid that, do NOT include Windows .exe candidates by default.
		// If you really want to use a Windows Chrome from WSL, set CHROME_PATH or
		// GOOGLE_CHROME_BIN explicitly and opt in via ALLOW_WINDOWS_CHROME=1.
		if IsWSL() && os.Getenv("ALLOW_WINDOWS_CHROME") == "1" {
			candidates = append(candidates, wslInstallCandidates()...)
		}
		return unique(candidates)
	}
}

func linuxInstallCandidates() []string {
	return []string{
		"/usr/bin/google-chrome",
		"/usr/bin/google-chrome-stable",
		"/usr/bin/chromium",
		"/usr/bin/chromium-browser",
		"/snap/bin/chromium",
		"/usr/bin/microsoft-edge",
		"/usr/bin/msedge",
	}
}

func windowsInstallCandidates() []string {
	candidates := []string{}

	if pf := strings.TrimSpace(os.Getenv("ProgramFiles")); pf != "" {
		candidates = append(candidates,
			filepath.Join(pf, "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(pf, "Google", "Chrome Dev", "Application", "chrome.exe"),
			filepath.Join(pf, "Microsoft", "Edge", "Application", "msedge.exe"),
		)
	}
	if pfx86 := strings.TrimSpace(os.Getenv("ProgramFiles(x86)")); pfx86 != "" {
		candidates = append(candidates,
			filepath.Join(pfx86, "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(pfx86, "Microsoft", "Edge", "Application", "msedge.exe"),
		)
	}
	if local := strings.TrimSpace(os.Getenv("LocalAppData")); local != "" {
		candidates = append(candidates,
			filepath.Join(local, "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(local, "Google", "Chrome Dev", "Application", "chrome.exe"),
			filepath.Join(local, "Microsoft", "Edge", "Application", "msedge.exe"),
		)
	}

	// Static fallback for environments where Windows env vars are not available.
	candidates = append(candidates,
		`C:\Program Files\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files\Google\Chrome Dev\Application\chrome.exe`,
		`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files\Microsoft\Edge\Application\msedge.exe`,
		`C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`,
	)

	return unique(candidates)
}

func wslInstallCandidates() []string {
	candidates := []string{
		"/mnt/c/Program Files/Google/Chrome/Application/chrome.exe",
		"/mnt/c/Program Files/Google/Chrome Dev/Application/chrome.exe",
		"/mnt/c/Program Files (x86)/Google/Chrome/Application/chrome.exe",
		"/mnt/c/Program Files/Microsoft/Edge/Application/msedge.exe",
		"/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
	}

	// Add user-local installs from any Windows profile mounted in /mnt/c/Users.
	patterns := []string{
		"/mnt/c/Users/*/AppData/Local/Google/Chrome/Application/chrome.exe",
		"/mnt/c/Users/*/AppData/Local/Google/Chrome Dev/Application/chrome.exe",
		"/mnt/c/Users/*/AppData/Local/Microsoft/Edge/Application/msedge.exe",
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		slices.Sort(matches)
		candidates = append(candidates, matches...)
	}

	return unique(candidates)
}

func IsWSL() bool {
	if runtime.GOOS != "linux" {
		return false
	}

	if os.Getenv("WSL_INTEROP") != "" || os.Getenv("WSL_DISTRO_NAME") != "" {
		return true
	}

	for _, p := range []string{"/proc/sys/kernel/osrelease", "/proc/version"} {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		lower := strings.ToLower(string(raw))
		if strings.Contains(lower, "microsoft") || strings.Contains(lower, "wsl") {
			return true
		}
	}

	return false
}

func unique(paths []string) []string {
	result := make([]string, 0, len(paths))
	seen := map[string]struct{}{}
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		result = append(result, p)
	}
	return result
}

// NeedsNoSandbox reports if Chrome should run with --no-sandbox (WSL/root cases).
func NeedsNoSandbox() bool {
	return IsWSL() || os.Geteuid() == 0
}

func canStartFromGo(path string) bool {
	// Keep startup probe short; this runs at process initialization.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, path, "--version")
	err := cmd.Run()
	if err == nil {
		return true
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// Process started but didn't exit quickly.
		return true
	}

	if _, ok := errors.AsType[*exec.ExitError](err); ok {
		// Command executed and exited non-zero, but executable is runnable.
		return true
	}

	if pathErr, ok := errors.AsType[*os.PathError](err); ok {
		if errors.Is(pathErr.Err, syscall.EACCES) || errors.Is(pathErr.Err, syscall.ENOEXEC) {
			return false
		}
	}

	return false
}
