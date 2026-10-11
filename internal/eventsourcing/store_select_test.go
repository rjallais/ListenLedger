package eventsourcing

import (
	"testing"
)

func TestSelectedStoreName(t *testing.T) {
	for env, want := range map[string]string{"": EventStoreSQLite, "sqlite": EventStoreSQLite, "jetstream": EventStoreJetStream, "  JetStream  ": EventStoreJetStream} {
		t.Setenv(EventStoreEnv, env)
		got, err := SelectedStoreName()
		if err != nil || got != want {
			t.Fatalf("SelectedStoreName(%q) = %q, %v; want %q", env, got, err, want)
		}
	}
	t.Setenv(EventStoreEnv, "kafka")
	if _, err := SelectedStoreName(); err == nil {
		t.Fatal("SelectedStoreName(kafka) should fail fast")
	}
}

func TestSelectedStore_JetStreamWithoutJSFails(t *testing.T) {
	t.Setenv(EventStoreEnv, "jetstream")
	if _, err := SelectedStore(nil, nil); err == nil {
		t.Fatal("SelectedStore(jetstream, nil js) should fail, not diverge")
	}
}
