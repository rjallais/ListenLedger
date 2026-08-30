package main

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsUniqueConstraintError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"pocketbase not unique", errors.New("validation_not_unique"), true},
		{"pocketbase field detail", fmt.Errorf("validation_not_unique: title must be unique"), true},
		{"sqlite unique", errors.New("UNIQUE constraint failed: albums.title, albums.artist_name"), true},
		{"check constraint", errors.New("CHECK constraint failed: status"), false},
		{"foreign key constraint", errors.New("FOREIGN KEY constraint failed"), false},
		{"unrelated", errors.New("no such table: artists"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUniqueConstraintError(tt.err); got != tt.want {
				t.Fatalf("isUniqueConstraintError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
