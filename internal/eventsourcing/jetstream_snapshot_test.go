package eventsourcing

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	"ListenLedger/internal/messaging"
)

func appendRange(t *testing.T, store *JetStreamStore, ctx context.Context, streamID string, from, to int64) {
	t.Helper()
	for v := from; v <= to; v++ {
		if err := store.Append(ctx, streamID, v-1, mustTestEvent(t, streamID, v)); err != nil {
			t.Fatalf("Append %s v%d: %v", streamID, v, err)
		}
	}
}

func TestJetStreamStore_SnapshotTailRoundTrip(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	if err := messaging.EnsureKVBuckets(ctx, store.js); err != nil {
		t.Fatalf("EnsureKVBuckets: %v", err)
	}

	// Interleave a second aggregate so the target's stream sequences are not
	// consecutive: the anchor must be a real sequence, not base+version.
	appendRange(t, store, ctx, "ar_snap_1", 1, 5)
	appendRange(t, store, ctx, "ar_other", 1, 3)

	if err := store.SaveSnapshot(ctx, Snapshot{StreamID: "ar_snap_1", StreamType: "artist", Version: 5, Payload: []byte(`{"v":5}`)}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	snap, err := store.LoadSnapshot(ctx, "ar_snap_1")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.Version != 5 || snap.StreamType != "artist" || string(snap.Payload) != `{"v":5}` {
		t.Fatalf("LoadSnapshot = %+v, want v5 artist {\"v\":5}", snap)
	}

	// Caught up: empty tail without a consumer.
	tail, err := store.LoadAfter(ctx, "ar_snap_1", 5)
	if err != nil || len(tail) != 0 {
		t.Fatalf("LoadAfter(5) at tip = %d events, err %v; want 0", len(tail), err)
	}

	appendRange(t, store, ctx, "ar_snap_1", 6, 6)
	appendRange(t, store, ctx, "ar_other", 4, 4)
	tail, err = store.LoadAfter(ctx, "ar_snap_1", 5)
	if err != nil || len(tail) != 1 || tail[0].Version != 6 {
		t.Fatalf("LoadAfter(5) single tail = %v, err %v; want [v6]", tail, err)
	}

	appendRange(t, store, ctx, "ar_snap_1", 7, 9)
	tail, err = store.LoadAfter(ctx, "ar_snap_1", 5)
	if err != nil {
		t.Fatalf("LoadAfter(5): %v", err)
	}
	if len(tail) != 4 {
		t.Fatalf("LoadAfter(5) = %d events, want 4", len(tail))
	}
	for i, evt := range tail {
		if want := int64(6 + i); evt.Version != want || evt.StreamID != "ar_snap_1" {
			t.Fatalf("tail[%d] = %s v%d, want ar_snap_1 v%d", i, evt.StreamID, evt.Version, want)
		}
	}

	// An afterVersion with no matching snapshot anchor still works (scan + filter).
	tail, err = store.LoadAfter(ctx, "ar_snap_1", 7)
	if err != nil || len(tail) != 2 || tail[0].Version != 8 {
		t.Fatalf("LoadAfter(7) = %v, err %v; want [v8 v9]", tail, err)
	}
}

func TestJetStreamStore_SaveSnapshotSkipsWhenTipMoved(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	if err := messaging.EnsureKVBuckets(ctx, store.js); err != nil {
		t.Fatalf("EnsureKVBuckets: %v", err)
	}
	appendRange(t, store, ctx, "ar_snap_2", 1, 5)

	if err := store.SaveSnapshot(ctx, Snapshot{StreamID: "ar_snap_2", StreamType: "artist", Version: 3, Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("SaveSnapshot(stale version) = %v, want nil skip", err)
	}
	if _, err := store.LoadSnapshot(ctx, "ar_snap_2"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("LoadSnapshot after skipped save = %v, want ErrSnapshotNotFound", err)
	}
}

func TestJetStreamStore_LoadSnapshotRejectsStaleAnchor(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	if err := messaging.EnsureKVBuckets(ctx, store.js); err != nil {
		t.Fatalf("EnsureKVBuckets: %v", err)
	}
	appendRange(t, store, ctx, "ar_snap_3", 1, 4)
	appendRange(t, store, ctx, "ar_snap_4", 1, 4)

	// Forge a record for ar_snap_3 whose anchor sequence names ar_snap_4's
	// event (what a rebuilt stream with shifted sequences would look like).
	stream, err := store.js.Stream(ctx, messaging.DomainEventsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	foreign, err := stream.GetLastMsgForSubject(ctx, messaging.SubjectDomainEventScope("ar_snap_4"))
	if err != nil {
		t.Fatalf("GetLastMsgForSubject: %v", err)
	}
	body, err := json.Marshal(jetStreamSnapshot{StreamType: "artist", Version: 4, Seq: foreign.Sequence, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := messaging.KVPut(ctx, store.js, messaging.KVSnapshots, snapshotKey("ar_snap_3"), string(body)); err != nil {
		t.Fatalf("KVPut: %v", err)
	}
	if _, err := store.LoadSnapshot(ctx, "ar_snap_3"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("LoadSnapshot with foreign anchor = %v, want ErrSnapshotNotFound", err)
	}
}

func TestJetStreamStore_SnapshotWithoutBucketIsNotFound(t *testing.T) {
	store, ctx := setupTestJSStore(t)
	appendRange(t, store, ctx, "ar_snap_5", 1, 2)
	if _, err := store.LoadSnapshot(ctx, "ar_snap_5"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("LoadSnapshot without KV bucket = %v, want ErrSnapshotNotFound", err)
	}
}

func TestSnapshotKeyIsLegalForArbitraryIDs(t *testing.T) {
	for _, id := range []string{"ar_1", "a.b", "has space", ".leading", "unicode-é", "a/b*c>d"} {
		key := snapshotKey(id)
		for _, r := range key {
			ok := r == '-' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
			if !ok {
				t.Fatalf("snapshotKey(%q) = %q contains illegal rune %q", id, key, r)
			}
		}
	}
	if snapshotKey("a.b") == snapshotKey("a_b") {
		t.Fatal("snapshotKey must be injective")
	}
}
