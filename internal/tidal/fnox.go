package tidal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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

// SaveSecret securely stores or updates a secret via the FNOX CLI ('fnox set <key>'),
// passing the value through standard input to avoid leaking via arguments.
func SaveSecret(ctx context.Context, key, value string) error {
	fnoxPath, err := exec.LookPath("fnox")
	if err != nil {
		return fmt.Errorf("fnox executable not found in PATH: %w", err)
	}

	cmd := exec.CommandContext(ctx, fnoxPath, "set", key)
	cmd.Stdin = strings.NewReader(value)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("fnox set %s failed: %w (%s)", key, err, strings.TrimSpace(string(out)))
	}
	return nil
}
