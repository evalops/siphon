package poller

import "testing"

func TestDiffSnapshots(t *testing.T) {
	prev := map[string]any{"stage": "open", "amount": 10}
	curr := map[string]any{"stage": "won", "amount": 10, "owner": "alice"}

	changes := DiffSnapshots(prev, curr)
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}
	if ch, ok := changes["stage"]; !ok || ch.From != "open" || ch.To != "won" {
		t.Fatalf("unexpected stage change: %+v", ch)
	}
	if ch, ok := changes["owner"]; !ok || ch.From != nil || ch.To != "alice" {
		t.Fatalf("unexpected owner change: %+v", ch)
	}
}
