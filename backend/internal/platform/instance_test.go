package platform

import (
	"regexp"
	"testing"
	"time"
)

func TestNewInstanceGeneratesStableIdentityShapeAndRandomGeneration(t *testing.T) {
	now := time.Date(2026, 9, 20, 2, 0, 0, 0, time.UTC)
	first, err := NewInstance("api", "", now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewInstance("api", "", now)
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`^api_[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !pattern.MatchString(first.ID) || !pattern.MatchString(second.ID) {
		t.Fatalf("generated IDs = %q, %q", first.ID, second.ID)
	}
	if first.Generation == 0 || second.Generation == 0 || first.Generation == second.Generation {
		t.Fatalf("generations = %d, %d; want distinct non-zero values", first.Generation, second.Generation)
	}
	if first.StartedAt != now || first.Role != "api" {
		t.Fatalf("instance = %+v", first)
	}
}

func TestNewInstancePreservesValidatedConfiguredID(t *testing.T) {
	now := time.Now().UTC()
	instance, err := NewInstance("worker", "worker_primary-1", now)
	if err != nil {
		t.Fatal(err)
	}
	if instance.ID != "worker_primary-1" || instance.Role != "worker" || instance.StartedAt != now || instance.Generation == 0 {
		t.Fatalf("instance = %+v", instance)
	}
	for _, tc := range []struct{ role, id string }{{"", "api_1"}, {"admin", "api_1"}, {"api", "bad id"}, {"worker", "bad{id}"}} {
		if _, err := NewInstance(tc.role, tc.id, now); err == nil {
			t.Fatalf("NewInstance(%q, %q) succeeded", tc.role, tc.id)
		}
	}
}
