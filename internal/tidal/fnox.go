package tidal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ResolveSecret resolves a secret by checking environment variables first,
// and falling back to querying the FNOX CLI ('fnox get <key>').
func ResolveSecret(ctx context.Context, key string) (string, error) {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val, nil
	}

	fnoxPath, err := exec.LookPath("fnox")
	if err != nil {
		return "", nil // fnox not found, treat as unset
	}

	cmd := exec.CommandContext(ctx, fnoxPath, "get", key)
	out, err := cmd.Output()
	if err != nil {
		// Key not set or fnox error
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

// SaveSecret securely stores or updates a secret via the FNOX CLI
// ('fnox set -g <key>'), writing to the global config
// (~/.config/fnox/config.toml). Global storage keeps rotating secrets (the
// TIDAL refresh token rotates on every sync) from rewriting the tracked
// project fnox.toml; ResolveSecret and 'fnox exec' resolve global keys
// transparently. The value is passed through standard input to avoid
// leaking via arguments.
func SaveSecret(ctx context.Context, key, value string) error {
	fnoxPath, err := exec.LookPath("fnox")
	if err != nil {
		return fmt.Errorf("fnox executable not found in PATH: %w", err)
	}

	cmd := exec.CommandContext(ctx, fnoxPath, "set", "-g", key)
	cmd.Stdin = strings.NewReader(value)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("fnox set %s failed: %w (%s)", key, err, strings.TrimSpace(string(out)))
	}

	// If a project-scoped fnox.toml exists and contains an older entry for this key,
	// remove it using explicit project config scope so it doesn't shadow the global value.
	cleanProjectOverride(ctx, fnoxPath, key)

	// Verify that FNOX resolves the key to the new global value.
	getCmd := exec.CommandContext(ctx, fnoxPath, "get", key)
	got, err := getCmd.Output()
	if err != nil {
		return fmt.Errorf("verifying stored secret %s: %w", key, err)
	}
	if strings.TrimSpace(string(got)) != value {
		return fmt.Errorf("stored secret %s mismatch after save", key)
	}
	return nil
}

func cleanProjectOverride(ctx context.Context, fnoxPath, key string) {
	configPath, err := findProjectConfig()
	if err != nil || configPath == "" {
		return
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	if !strings.Contains(string(data), key) {
		return
	}
	cmd := exec.CommandContext(ctx, fnoxPath, "remove", "-c", configPath, key)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("[tidal] Warning: fnox remove -c %s %s failed (project entry may shadow global value): %v (%s)",
			configPath, key, err, strings.TrimSpace(string(out)))
	}
}

func findProjectConfig() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, "fnox.toml")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", nil
}
