package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// SetupDB opens and initializes the SQLite database with WAL mode and runs embedded SQL migrations.
func SetupDB(ctx context.Context, log *slog.Logger, dataFolder string, shouldClear bool) (*toolbeltdb.Database, error) {
	migrationsDir := "migrations"
	migrationsFiles, err := migrationsFS.ReadDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}
	migrationsFiles = slices.DeleteFunc(migrationsFiles, func(de fs.DirEntry) bool {
		return !de.IsDir() && strings.HasSuffix(de.Name(), ".down.sql")
	})
	slices.SortFunc(migrationsFiles, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	migrations := make([]string, len(migrationsFiles))
	for i, file := range migrationsFiles {
		fn := filepath.Join(migrationsDir, file.Name())
		fnts := filepath.ToSlash(fn)
		content, err := fs.ReadFile(migrationsFS, fnts)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", file.Name(), err)
		}

		migrations[i] = string(content)
	}

	dbFolder := filepath.Join(dataFolder, "sqlite")
	if shouldClear {
		log.InfoContext(ctx, "clearing database folder", "dbfolder", dbFolder)
		if err := os.RemoveAll(dbFolder); err != nil {
			return nil, fmt.Errorf("failed to remove database folder: %w", err)
		}
	}
	dbFilename := filepath.Join(dbFolder, "ledger.sqlite")
	db, err := toolbeltdb.NewDatabase(
		ctx,
		toolbeltdb.DatabaseWithFilename(dbFilename),
		toolbeltdb.DatabaseWithMigrations(migrations),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create database at %s: %w", dbFilename, err)
	}

	return db, nil
}
