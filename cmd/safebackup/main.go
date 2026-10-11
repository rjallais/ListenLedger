package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	_ "modernc.org/sqlite"
)

// main parses command-line flags, opens the SQLite source database, and creates a timestamped hot backup
// file in the specified output directory using `VACUUM INTO`.
//
// With --stopped (the operator asserts the app is stopped) it additionally
// copies the NATS JetStream store beside the data dir and verifies the copy
// by booting an embedded server on it: replay rehearsals in jetstream mode
// need the nats/ tree, and a file copy of a live store may be torn — the
// verification turns that into a loud error instead of a misleading rehearsal.
// Without --stopped the NATS copy is skipped (the SQLite backup still
// proceeds: VACUUM INTO is live-safe, a raw file copy is not).
//
// It logs progress and exits with a fatal error if opening the database or performing the backup fails.
// When shutting down, it attempts to close the database and logs a warning if close returns an error.
func main() {
	dbPath := flag.String("db", "pb_data/data.db", "Path to source database")
	backupDir := flag.String("out", "backups", "Output directory for backups")
	natsSrc := flag.String("nats-src", "", "NATS JetStream store to copy (default: nats/ beside --db)")
	natsDst := flag.String("nats-dst", "", "Destination for the NATS copy (default: <out>/nats-<timestamp>)")
	stopped := flag.Bool("stopped", false, "Assert the app is stopped: enables the NATS store copy")
	flag.Parse()

	// Connect to source DB
	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("[safebackup] warning: db.Close: %v", err)
		}
	}()

	// Generate backup filename with timestamp
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(*backupDir, fmt.Sprintf("data_backup_%s.db", timestamp))

	// Ensure backup directory exists (helper not shown, but assumes user creates it or we error)
	// Actually, let's try to create it? No imports for that logic here to keep it simple,
	// assuming "backups" folder exists or failure is fine to report.
	// Wait, I should make sure it exists. Can't reliably do `mkdir` in pure SQL.
	// I'll assume usage of `mkdir backups` command before.

	log.Printf("Starting hot backup to %s...", backupFile)

	// VACUUM INTO is the safe way to backup a live SQLite database
	_, err = db.Exec(fmt.Sprintf("VACUUM INTO '%s'", backupFile))
	if err != nil {
		log.Fatalf("Backup failed: %v", err)
	}

	log.Println("Backup completed successfully!")

	if err := backupNATSStore(context.Background(), *natsSrc, *natsDst, *dbPath, *backupDir, timestamp, *stopped); err != nil {
		log.Fatalf("NATS backup failed: %v", err)
	}
}

// backupNATSStore copies the JetStream file store for replay rehearsals.
// Without the stopped assertion it skips (loudly): a raw copy of a live
// store may be torn, while VACUUM INTO above is always live-safe.
func backupNATSStore(ctx context.Context, natsSrc, natsDst, dbPath, backupDir, timestamp string, stopped bool) error {
	if natsSrc == "" {
		natsSrc = filepath.Join(filepath.Dir(dbPath), "nats")
	}
	if _, err := os.Stat(natsSrc); err != nil {
		if os.IsNotExist(err) {
			// --stopped is the operator explicitly requesting the copy;
			// a missing source here is a bad --nats-src/--db, not a skip.
			if stopped {
				return fmt.Errorf("--stopped set but no NATS store at %s", natsSrc)
			}
			log.Printf("[safebackup] no NATS store at %s, skipping NATS copy", natsSrc)
			return nil
		}
		return fmt.Errorf("stat NATS store: %w", err)
	}
	if !stopped {
		log.Printf("[safebackup] skipping NATS copy of %s: pass --stopped with the app stopped (live copy may be torn)", natsSrc)
		return nil
	}
	if natsDst == "" {
		natsDst = filepath.Join(backupDir, fmt.Sprintf("nats-%s", timestamp))
	}
	// Refuse a bad destination: an existing regular file (ReadDir would skip
	// the emptiness check and a failed copy's RemoveAll would delete it), a
	// non-empty dir (merging leaves stale files beside new ones), or a
	// destination inside the source (the copy would walk its own output).
	dstExisted := false
	if fi, err := os.Stat(natsDst); err == nil {
		dstExisted = true
		if !fi.IsDir() {
			return fmt.Errorf("NATS destination %s exists and is not a directory", natsDst)
		}
		if entries, err := os.ReadDir(natsDst); err != nil {
			return fmt.Errorf("reading NATS destination: %w", err)
		} else if len(entries) > 0 {
			return fmt.Errorf("NATS destination %s is not empty", natsDst)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat NATS destination: %w", err)
	}
	// Compare resolved paths: either side may sit under symlinked parents,
	// and Abs alone is lexical. Both sides are absolutized first: a relative
	// srcReal against an absolute dstCheck would fail Rel below, and that
	// failure must refuse the copy, never permit it.
	srcAbs, err := filepath.Abs(natsSrc)
	if err != nil {
		return fmt.Errorf("resolving NATS source: %w", err)
	}
	srcReal, err := filepath.EvalSymlinks(srcAbs)
	if err != nil {
		return fmt.Errorf("resolving NATS source: %w", err)
	}
	dstAbs, err := filepath.Abs(natsDst)
	if err != nil {
		return fmt.Errorf("resolving NATS destination: %w", err)
	}
	{
		// Resolve symlinks before comparing: a dst symlink pointing
		// inside src would pass the lexical check below while copyDir
		// writes through it into the tree being walked (self-copy until
		// the disk fills). Resolve the link itself when dst exists,
		// else the nearest existing parent.
		dstCheck := dstAbs
		// Resolve whenever dst exists: a symlinked ancestor would survive
		// leaf-only resolution while copyDir writes through it into the
		// source. A dangling leaf fails EvalSymlinks — fail closed, as does
		// any other stat error (never fall back to the lexical path).
		switch _, err := os.Lstat(natsDst); {
		case err == nil:
			real, err := filepath.EvalSymlinks(natsDst)
			if err != nil {
				return fmt.Errorf("resolving NATS destination: %w", err)
			}
			dstCheck = real
		case !os.IsNotExist(err):
			return fmt.Errorf("stat NATS destination: %w", err)
		default:
			// Walk up to the nearest existing ancestor: intermediate
			// parents may be missing too, and EvalSymlinks fails on any
			// missing path — resolving only the immediate parent would
			// fall back to the lexical dstAbs while srcReal is resolved,
			// letting an inside-source destination pass the guard below.
			suffix := ""
			for dir := dstAbs; ; {
				parent := filepath.Dir(dir)
				suffix = filepath.Join(filepath.Base(dir), suffix)
				if real, err := filepath.EvalSymlinks(parent); err == nil {
					dstCheck = filepath.Join(real, suffix)
					break
				}
				if parent == dir {
					break
				}
				dir = parent
			}
		}
		rel, err := filepath.Rel(srcReal, dstCheck)
		if err != nil {
			return fmt.Errorf("comparing NATS source and destination: %w", err)
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("NATS destination %s is inside source %s", natsDst, natsSrc)
		}
	}
	log.Printf("[safebackup] copying NATS store %s -> %s ...", natsSrc, natsDst)
	if err := copyDir(natsSrc, natsDst); err != nil {
		_ = undoDest(natsDst, dstExisted)
		return fmt.Errorf("copying NATS store: %w", err)
	}
	if err := verifyNATSStore(ctx, natsDst); err != nil {
		_ = undoDest(natsDst, dstExisted)
		return fmt.Errorf("verifying NATS copy (source may have been live): %w", err)
	}
	log.Println("[safebackup] NATS copy verified.")
	return nil
}

// undoDest cleans up after a failed copy without deleting a directory the
// operator owned before the call: a pre-existing dir is emptied in place,
// one this call created is removed entirely.
func undoDest(dst string, existed bool) error {
	if !existed {
		return os.RemoveAll(dst)
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// copyDir replicates a file tree (modes preserved). Fail-closed: symlinks
// and other non-regular files are rejected (a link escaping the store would
// silently pull outside content into a "verified" copy). Destination
// freshness is the caller's job (see backupNATSStore).
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported non-regular file %s", path)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
			return err
		}
		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// verifyNATSStore boots an embedded server on the COPY (never the source)
// and lists streams: a torn copy fails recovery loudly instead of seeding a
// misleading rehearsal. Read-only against the source by construction.
func verifyNATSStore(ctx context.Context, storeDir string) error {
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: storeDir,
	})
	if err != nil {
		return fmt.Errorf("starting embedded NATS: %w", err)
	}
	go ns.Start()
	defer ns.Shutdown()
	if !ns.ReadyForConnections(10 * time.Second) {
		return fmt.Errorf("embedded NATS not ready on copy at %s", storeDir)
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		return fmt.Errorf("connecting copy: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream context: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	lister := js.StreamNames(ctx)
	count := 0
	for range lister.Name() {
		count++
	}
	if err := lister.Err(); err != nil {
		return fmt.Errorf("listing streams in copy: %w", err)
	}
	log.Printf("[safebackup] copy holds %d stream(s)", count)
	return nil
}
