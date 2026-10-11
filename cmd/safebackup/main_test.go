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
	if err := backupNATSStore(src, skipDst, "pb_data/data.db", t.TempDir(), "ts", false); err != nil {
		t.Fatalf("skip path: %v", err)
	}
	if _, err := os.Stat(skipDst); !os.IsNotExist(err) {
		t.Fatalf("unstopped copy should not exist, stat err = %v", err)
	}

	// Stopped: copy + self-verification against the copy.
	dst := filepath.Join(t.TempDir(), "nats-copy")
	if err := backupNATSStore(src, dst, "pb_data/data.db", t.TempDir(), "ts", true); err != nil {
		t.Fatalf("stopped copy: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("stopped copy should exist: %v", err)
	}
}

func TestBackupNATSStoreMissingSource(t *testing.T) {
	if err := backupNATSStore(
		filepath.Join(t.TempDir(), "absent"), filepath.Join(t.TempDir(), "dst"),
		"pb_data/data.db", t.TempDir(), "ts", true,
	); err != nil {
		t.Fatalf("absent source should skip cleanly: %v", err)
	}
}
