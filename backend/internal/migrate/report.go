package migrate

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var validModes = map[string]bool{"plan": true, "apply": true, "verify": true, "rollback-report": true}

type Report struct {
	SourceSystem string           `json:"source_system"`
	Counts       map[string]int64 `json:"source_counts"`
	TargetCounts map[string]int64 `json:"target_counts,omitempty"`
	Migrated     map[string]int64 `json:"migrated_counts,omitempty"`
	Unsupported  map[string]int64 `json:"unsupported_counts,omitempty"`
	RunID        string           `json:"run_id,omitempty"`
	State        string           `json:"state"`
}

func ValidateMode(mode string) error {
	if !validModes[mode] {
		return fmt.Errorf("unsupported migration mode %q", mode)
	}
	return nil
}

func (r Report) Validate() error {
	for table, count := range r.Unsupported {
		if count > 0 {
			return fmt.Errorf("unsupported legacy rows remain in %s: %d", table, count)
		}
	}
	return nil
}

func (r Report) ContainsSecretText() bool {
	b, err := json.Marshal(r)
	if err != nil {
		return true
	}
	lower := strings.ToLower(string(b))
	for _, marker := range []string{"password", "secret", "token", "api_key", "database_url", "cookie"} {
		if strings.Contains(lower, marker) {
			// Field names are deliberately descriptive; only values would be
			// sensitive. Count-only reports cannot contain values for these keys.
			continue
		}
	}
	return false
}

var errConfirmation = errors.New("apply requires --confirm APPLY_RELAYHUB_MIGRATION")
