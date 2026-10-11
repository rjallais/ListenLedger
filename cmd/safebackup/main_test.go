package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCopyDir(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub", "deep"), 0750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("a"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "deep", "b.txt"), []byte("bb"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "copy")
	if err := copyDir(src, dst); err != nil {
		t.Fatalf("copyDir: %v", err)
	}
	for file, want := range map[string]string{
		"a.txt":          "a",
		"sub/deep/b.txt": "bb",
	} {
		got, err := os.ReadFile(filepath.Join(dst, file))
		if err != nil || string(got) != want {
			t.Fatalf("copy %s = %q, %v; want %q", file, got, err, want)
		}
	}
}

func TestBackupNATSStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Seed a real JetStream store, then shut the server down (the rehearsal
	// precondition: copies happen stopped).
	src := filepath.Join(t.TempDir(), "nats")
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: src,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("Connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("New: %v", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "BACKUP_TEST", Subjects: []string{"backup.test"}, Storage: jetstream.FileStorage,
	}); err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("CreateOrUpdateStream: %v", err)
	}
	if _, err := js.Publish(ctx, "backup.test", []byte(`{}`)); err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("Publish: %v", err)
	}
	nc.Close()
	ns.Shutdown()

	// Without --stopped the copy is skipped, never attempted.
	skipDst := filepath.Join(t.TempDir(), "skipped")
	if err := backupNATSStore(t.Context(), src, skipDst, "pb_data/data.db", t.TempDir(), "ts", false); err != nil {
		t.Fatalf("skip path: %v", err)
	}
	if _, err := os.Stat(skipDst); !os.IsNotExist(err) {
		t.Fatalf("unstopped copy should not exist, stat err = %v", err)
	}

	// Stopped: copy + self-verification against the copy.
	dst := filepath.Join(t.TempDir(), "nats-copy")
	if err := backupNATSStore(t.Context(), src, dst, "pb_data/data.db", t.TempDir(), "ts", true); err != nil {
		t.Fatalf("stopped copy: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("stopped copy should exist: %v", err)
	}
}

func TestBackupNATSStoreMissingSource(t *testing.T) {
	// Without --stopped a missing store is an expected skip (e.g. a fresh
	// deployment before the first boot).
	if err := backupNATSStore(
		t.Context(),
		filepath.Join(t.TempDir(), "absent"), filepath.Join(t.TempDir(), "dst"),
		"pb_data/data.db", t.TempDir(), "ts", false,
	); err != nil {
		t.Fatalf("absent source without --stopped should skip cleanly: %v", err)
	}
	// With --stopped the operator explicitly requested the copy: a missing
	// source is a bad --nats-src/--db and must fail, not skip.
	if err := backupNATSStore(
		t.Context(),
		filepath.Join(t.TempDir(), "absent"), filepath.Join(t.TempDir(), "dst"),
		"pb_data/data.db", t.TempDir(), "ts", true,
	); err == nil {
		t.Fatal("absent source with --stopped should error, got nil")
	}
}

// TestBackupNATSStoreKeepsExistingDestinationOnFailure guards the cleanup
// contract: a failed copy must clear the contents of a pre-existing
// destination directory, never delete the directory itself.
func TestBackupNATSStoreKeepsExistingDestinationOnFailure(t *testing.T) {
	src := filepath.Join(t.TempDir(), "nats")
	if err := os.MkdirAll(src, 0750); err != nil {
		t.Fatal(err)
	}
	// copyDir rejects symlinks fail-closed: a link in the source makes the
	// copy fail partway, which drives the undoDest cleanup path.
	if err := os.Symlink(filepath.Join(src, "self"), filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "bogus.txt"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "rehearsal", "nats")
	if err := os.MkdirAll(dst, 0750); err != nil {
		t.Fatal(err)
	}
	if err := backupNATSStore(
		t.Context(),
		src, dst, "pb_data/data.db", t.TempDir(), "ts", true,
	); err == nil {
		t.Fatal("copy with a symlink in the source should fail")
	}
	if fi, err := os.Stat(dst); err != nil {
		t.Fatalf("pre-existing destination must survive failed copy: %v", err)
	} else if !fi.IsDir() {
		t.Fatal("pre-existing destination must remain a directory")
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed copy must clear destination contents, found %d entries", len(entries))
	}
}

func TestBackupNATSStoreRejectsSymlinkedDestination(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(src, 0750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("x"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Destination symlink pointing inside the source: lexical Abs/Rel sees
	// only the link path, so without resolution the copy would walk src and
	// write through the link into its own output.
	inner, err := os.MkdirTemp(src, "inner")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	link := filepath.Join(t.TempDir(), "dstlink")
	if err := os.Symlink(inner, link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	err = backupNATSStore(t.Context(), src, link, "pb_data/data.db", t.TempDir(), "ts", true)
	if err == nil {
		t.Fatal("symlinked destination inside source should be refused")
	}
}

func TestBackupNATSStoreRejectsSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.MkdirAll(src, 0750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("x"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Real dir inside src, reached via a symlinked parent outside it: the
	// lexical dst path looks outside the source while writes land inside.
	linkParent := filepath.Join(t.TempDir(), "linkparent")
	if err := os.Symlink(src, linkParent); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	dst := filepath.Join(linkParent, "dst")
	if err := backupNATSStore(t.Context(), src, dst, "pb_data/data.db", t.TempDir(), "ts", true); err == nil {
		t.Fatal("destination under a symlinked ancestor inside source should be refused")
	}
}
