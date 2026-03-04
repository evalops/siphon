package store

import "testing"

func TestInMemoryCheckpointStore(t *testing.T) {
	s := NewInMemoryCheckpointStore()

	if _, ok := s.Get("hubspot"); ok {
		t.Fatalf("expected no checkpoint initially")
	}

	if err := s.Set("hubspot", "2026-03-03T14:22:00Z"); err != nil {
		t.Fatalf("set checkpoint: %v", err)
	}
	v, ok := s.Get("hubspot")
	if !ok {
		t.Fatalf("expected checkpoint to exist")
	}
	if v != "2026-03-03T14:22:00Z" {
		t.Fatalf("unexpected checkpoint value %q", v)
	}
}

func TestInMemorySnapshotStoreReturnsCopy(t *testing.T) {
	s := NewInMemorySnapshotStore()

	orig := map[string]any{"stage": "open", "amount": 100}
	if err := s.Put("hubspot", "deal", "d1", orig); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}

	got, ok := s.Get("hubspot", "deal", "d1")
	if !ok {
		t.Fatalf("expected snapshot")
	}
	if got["stage"] != "open" {
		t.Fatalf("unexpected snapshot stage: %+v", got)
	}

	got["stage"] = "won"
	orig["amount"] = 999

	again, ok := s.Get("hubspot", "deal", "d1")
	if !ok {
		t.Fatalf("expected snapshot on second read")
	}
	if again["stage"] != "open" {
		t.Fatalf("store should not be mutated by returned map edits: %+v", again)
	}
	if again["amount"] != 100 {
		t.Fatalf("store should keep copied input values: %+v", again)
	}
}
