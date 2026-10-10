// Package messaging KV buckets for the JetStream-as-truth model.
//
// KV holds only latest-value-per-key state, file-backed in the embedded
// StoreDir. KV is never the event log — streams own history, KV owns
// current values. All buckets use History 1.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// KVCorrelation maps artistID -> requestID (5m TTL). Replaces the
	// in-memory correlation registry; survives restarts.
	KVCorrelation = "correlation"
	// KVCorrelationTTL matches the old registry TTL.
	KVCorrelationTTL = 5 * time.Minute

	// KVSucceededRequests maps requestID -> completedAt (30m TTL). Replaces
	// worker.succeededRequests; ack-skip idempotency survives restarts.
	KVSucceededRequests = "succeeded_requests"
	// KVSucceededRequestsTTL matches the old requestSuccessCacheTTL.
	KVSucceededRequestsTTL = 30 * time.Minute

	// KVSnapshots maps streamID -> snapshot JSON (no TTL). Matches snapshot
	// semantics exactly (one freeze-frame per stream); derivable from the
	// log, so loss only costs replay time.
	KVSnapshots = "snapshots"
)

// EnsureKVBuckets creates or updates the three KV buckets. Idempotent.
func EnsureKVBuckets(ctx context.Context, js jetstream.JetStream) error {
	buckets := []jetstream.KeyValueConfig{
		{Bucket: KVCorrelation, History: 1, TTL: KVCorrelationTTL, Storage: jetstream.FileStorage},
		{Bucket: KVSucceededRequests, History: 1, TTL: KVSucceededRequestsTTL, Storage: jetstream.FileStorage},
		{Bucket: KVSnapshots, History: 1, Storage: jetstream.FileStorage},
	}
	for _, cfg := range buckets {
		if _, err := js.CreateOrUpdateKeyValue(ctx, cfg); err != nil {
			return fmt.Errorf("ensure KV bucket %s: %w", cfg.Bucket, err)
		}
	}
	return nil
}

// KVGet returns the latest value for key, or ("", false, nil) when missing.
func KVGet(ctx context.Context, js jetstream.JetStream, bucket, key string) (string, bool, error) {
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		return "", false, fmt.Errorf("KV bucket %s: %w", bucket, err)
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("KV get %s/%s: %w", bucket, key, err)
	}
	return string(entry.Value()), true, nil
}

// KVPut stores value for key.
func KVPut(ctx context.Context, js jetstream.JetStream, bucket, key, value string) error {
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		return fmt.Errorf("KV bucket %s: %w", bucket, err)
	}
	if _, err := kv.PutString(ctx, key, value); err != nil {
		return fmt.Errorf("KV put %s/%s: %w", bucket, key, err)
	}
	return nil
}

// KVPop returns the value for key and deletes it (correlation pop).
// Missing keys return ("", false, nil).
func KVPop(ctx context.Context, js jetstream.JetStream, bucket, key string) (string, bool, error) {
	val, ok, err := KVGet(ctx, js, bucket, key)
	if err != nil || !ok {
		return val, ok, err
	}
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		return "", false, fmt.Errorf("KV bucket %s: %w", bucket, err)
	}
	if err := kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", false, fmt.Errorf("KV delete %s/%s: %w", bucket, key, err)
	}
	return val, true, nil
}
