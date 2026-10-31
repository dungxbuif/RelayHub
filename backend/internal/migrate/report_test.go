package migrate

import "testing"

func TestMigrationReportIsSanitizedAndDetectsUnsupportedRows(t *testing.T) {
	report := Report{
		SourceSystem: "legacy-go",
		Counts:       map[string]int64{"admin_users": 1, "applications": 4, "queue_subscriptions": 1},
		Unsupported:  map[string]int64{"queue_subscriptions": 1},
	}
	if err := report.Validate(); err == nil {
		t.Fatal("report with unsupported rows must not validate")
	}
	if report.ContainsSecretText() {
		t.Fatal("count-only report must not be treated as secret material")
	}
}

func TestMigrationModeValidation(t *testing.T) {
	for _, mode := range []string{"plan", "apply", "verify", "rollback-report"} {
		if err := ValidateMode(mode); err != nil {
			t.Fatalf("ValidateMode(%q): %v", mode, err)
		}
	}
	if err := ValidateMode("drop"); err == nil {
		t.Fatal("destructive mode must be rejected")
	}
}
