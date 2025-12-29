//go:build goexperiment.jsonv2

// Package storage provides file-based persistence for artist data and configuration.
package storage

import (
	"bufio"
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
	LoadResults(filename string) (map[string]int, error)
	SaveResults(filename string, results map[string]int) error
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
	file, err := os.Open(filename) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", filename, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close file %s: %v\n", filename, closeErr)
		}
	}()

	var artistIDs []string
	scanner := bufio.NewScanner(file)
	
	// Pre-allocate slice with reasonable capacity to reduce allocations
	artistIDs = make([]string, 0, 150) // Based on current input.txt size

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
			fmt.Fprintf(os.Stderr, "warning: line %d has invalid artist ID format: %s\n", lineNum, line)
			continue
		}
		
		artistIDs = append(artistIDs, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filename, err)
	}

	if len(artistIDs) == 0 {
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
			fmt.Fprintf(os.Stderr, "warning: failed to remove temporary file %s: %v\n", tempFile, removeErr)
		}
		return fmt.Errorf("failed to rename %s to %s: %w", tempFile, filename, err)
	}

	return nil
}