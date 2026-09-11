package natsbroker

import (
	"strings"
	"testing"
)

func TestAppTokenIsOpaqueStableAndSubjectSafe(t *testing.T) {
	const appID = "app_super-secret/customer.42"

	first, err := AppToken(appID)
	if err != nil {
		t.Fatalf("AppToken() error = %v", err)
	}
	second, err := AppToken(appID)
	if err != nil {
		t.Fatalf("AppToken() second call error = %v", err)
	}
	if first != second {
		t.Fatalf("AppToken() = %q then %q, want stable result", first, second)
	}
	if strings.Contains(first, appID) || strings.ContainsAny(first, ".*>/ ") {
		t.Fatalf("AppToken() = %q, want opaque subject-safe token", first)
	}
	if len(first) != 32 {
		t.Fatalf("len(AppToken()) = %d, want 32", len(first))
	}
}

func TestAppTokenRejectsInvalidIdentifiers(t *testing.T) {
	for _, appID := range []string{"", " ", strings.Repeat("a", 257), "app_\x00bad"} {
		if _, err := AppToken(appID); err == nil {
			t.Fatalf("AppToken(%q) error = nil, want rejection", appID)
		}
	}
}

func TestInternalSubjectsNeverContainApplicationID(t *testing.T) {
	const appID = "app_customer_42"

	subjects, err := SubjectsForApp(appID)
	if err != nil {
		t.Fatalf("SubjectsForApp() error = %v", err)
	}
	wantPrefixes := map[string]string{
		subjects.Deliveries:  "rh.deliveries.",
		subjects.Callbacks:   "rh.callbacks.",
		subjects.DeadLetters: "rh.dlq.",
		subjects.Observe:     "rh.observe.",
		subjects.Functions:   "rh.functions.",
	}
	for subject, prefix := range wantPrefixes {
		if !strings.HasPrefix(subject, prefix) || strings.Contains(subject, appID) {
			t.Errorf("subject %q, want prefix %q and no raw app ID", subject, prefix)
		}
	}
}
