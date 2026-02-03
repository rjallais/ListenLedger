//go:build goexperiment.jsonv2

// Package storage provides file-based persistence for artist data and configuration.
package storage

import (
	"bufio"
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Storage defines the interface for data persistence
type Storage interface {
	LoadArtistIDs(filename string) ([]string, error)
	LoadOptionalArtistIDs(filename string) ([]string, error)
	LoadResults(filename string) (map[string]int, error)
	SaveResults(filename string, results map[string]int) error
	AppendMissedIDs(filename string, ids []string) (int, error)
	SaveMissedIDs(filename string, ids []string) error
}

// FileStorage implements file-based storage
type FileStorage struct{}

// NewFileStorage creates a new file storage instance
func NewFileStorage() *FileStorage {
	return &FileStorage{}
}

// LoadArtistIDs reads artist IDs from a file, with improved performance
// G304: Potential file inclusion via variable - this is acceptable as we control the filename
func (fs *FileStorage) LoadArtistIDs(filename string) ([]string, error) {
	return fs.loadArtistIDs(filename, true)
}

// LoadOptionalArtistIDs reads artist IDs from a file.
// It returns an empty slice when the file doesn't exist or contains no valid IDs.
// G304: Potential file inclusion via variable - this is acceptable as we control the filename
func (fs *FileStorage) LoadOptionalArtistIDs(filename string) ([]string, error) {
	return fs.loadArtistIDs(filename, false)
}

func (fs *FileStorage) loadArtistIDs(filename string, requireFile bool) ([]string, error) {
	file, err := os.Open(filename) // #nosec G304
	if err != nil {
		if !requireFile && os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to open %s: %w", filename, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close file %s: %v\n", filename, closeErr)
		}
	}()

	artistIDs := make([]string, 0, 150) // Based on current input.txt size
	scanner := bufio.NewScanner(file)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Basic validation of artist ID format (should be alphanumeric, 22 chars)
		if len(line) != 22 {
			_, _ = fmt.Fprintf(os.Stderr, "warning: line %d has invalid artist ID format: %s\n", lineNum, line)
			continue
		}

		artistIDs = append(artistIDs, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filename, err)
	}

	if requireFile && len(artistIDs) == 0 {
		return nil, fmt.Errorf("no valid artist IDs found in %s", filename)
	}

	return artistIDs, nil
}

// LoadResults reads existing results from JSON file with better error handling
// G304: Potential file inclusion via variable - this is acceptable as we control the filename
func (fs *FileStorage) LoadResults(filename string) (map[string]int, error) {
	results := make(map[string]int)

	data, err := os.ReadFile(filename) // #nosec G304
	if err != nil {
		if os.IsNotExist(err) {
			return results, nil // Return empty map if file doesn't exist
		}
		return nil, fmt.Errorf("failed to read %s: %w", filename, err)
	}

	if len(data) == 0 {
		return results, nil // Return empty map if file is empty
	}

	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON from %s: %w", filename, err)
	}

	return results, nil
}

// SaveResults saves results to JSON file with atomic write and formatting
func (fs *FileStorage) SaveResults(filename string, results map[string]int) error {
	if results == nil {
		return errors.New("results map cannot be nil")
	}

	// Marshal to JSON first
	data, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	// Format with proper indentation
	v := jsontext.Value(data)
	if err := (&v).Indent(jsontext.WithIndent("  "), jsontext.WithIndentPrefix("")); err != nil {
		return fmt.Errorf("failed to format JSON: %w", err)
	}

	// Atomic write: write to temporary file first, then rename
	tempFile := filename + ".tmp"

	// Use more restrictive permissions for security
	if err := os.WriteFile(tempFile, v, 0600); err != nil {
		return fmt.Errorf("failed to write temporary file %s: %w", tempFile, err)
	}

	if err := os.Rename(tempFile, filename); err != nil {
		// Clean up temporary file on failure
		if removeErr := os.Remove(tempFile); removeErr != nil {
			// Log but don't override the main error
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to remove temporary file %s: %v\n", tempFile, removeErr)
		}
		return fmt.Errorf("failed to rename %s to %s: %w", tempFile, filename, err)
	}

	return nil
}

// AppendMissedIDs appends unique missed artist IDs to a file, creating it if needed.
// It returns the number of IDs appended.
// G304: Potential file inclusion via variable - this is acceptable as we control the filename
func (fs *FileStorage) AppendMissedIDs(filename string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	// Deduplicate incoming IDs while preserving order.
	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || strings.HasPrefix(id, "#") {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}

	if len(unique) == 0 {
		return 0, nil
	}

	existing := make(map[string]struct{})
	hasTrailingNewline := false
	hasContent := false

	data, err := os.ReadFile(filename) // #nosec G304
	if err == nil {
		if len(data) > 0 {
			hasContent = true
			hasTrailingNewline = data[len(data)-1] == '\n'
		}

		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			existing[line] = struct{}{}
		}

		if scanErr := scanner.Err(); scanErr != nil {
			return 0, fmt.Errorf("error reading %s: %w", filename, scanErr)
		}
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("failed to read %s: %w", filename, err)
	}

	toAppend := make([]string, 0, len(unique))
	for _, id := range unique {
		if _, ok := existing[id]; ok {
			continue
		}
		toAppend = append(toAppend, id)
	}

	if len(toAppend) == 0 {
		return 0, nil
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600) // #nosec G304
	if err != nil {
		return 0, fmt.Errorf("failed to open %s for append: %w", filename, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close file %s: %v\n", filename, closeErr)
		}
	}()

	if hasContent && !hasTrailingNewline {
		if _, err := file.WriteString("\n"); err != nil {
			return 0, fmt.Errorf("failed to append newline to %s: %w", filename, err)
		}
	}

	for _, id := range toAppend {
		if _, err := file.WriteString(id + "\n"); err != nil {
			return 0, fmt.Errorf("failed to append to %s: %w", filename, err)
		}
	}

	return len(toAppend), nil
}

// SaveMissedIDs rewrites the missed IDs file with the provided list.
// It creates the file if it doesn't exist and truncates it when the list is empty.
// G304: Potential file inclusion via variable - this is acceptable as we control the filename
func (fs *FileStorage) SaveMissedIDs(filename string, ids []string) error {
	// Deduplicate and clean while preserving order.
	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || strings.HasPrefix(id, "#") {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}

	var buf bytes.Buffer
	for _, id := range unique {
		buf.WriteString(id)
		buf.WriteByte('\n')
	}

	tempFile := filename + ".tmp"
	if err := os.WriteFile(tempFile, buf.Bytes(), 0600); err != nil {
		return fmt.Errorf("failed to write temporary file %s: %w", tempFile, err)
	}

	if err := os.Rename(tempFile, filename); err != nil {
		if removeErr := os.Remove(tempFile); removeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to remove temporary file %s: %v\n", tempFile, removeErr)
		}
		return fmt.Errorf("failed to rename %s to %s: %w", tempFile, filename, err)
	}

	return nil
}
