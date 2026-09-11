package postgres

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConfigValidationAndSafeDiagnostics(t *testing.T) {
	cfg := Config{DatabaseURL: "postgres://relayhub:p%40ss@db.internal:5432/relayhub?sslmode=require", MaxConnections: 12, MinConnections: 2}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	diagnostic := cfg.SafeDatabaseAddress()
	if diagnostic != "postgres://relayhub@db.internal:5432/relayhub" {
		t.Fatalf("SafeDatabaseAddress() = %q", diagnostic)
	}
	if strings.Contains(diagnostic, "ss") || strings.Contains(diagnostic, "sslmode") {
		t.Fatalf("SafeDatabaseAddress() leaked credentials/query: %q", diagnostic)
	}
}

func TestAuditMetadataRejectsSecretAndRequestBodyFields(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"api_key":"plaintext"}`),
		json.RawMessage(`{"Authorization":"Bearer plaintext"}`),
		json.RawMessage(`{"nested":{"hmac_secret":"plaintext"}}`),
		json.RawMessage(`{"password":"plaintext"}`),
		json.RawMessage(`{"request_body":{"card":"4111"}}`),
	} {
		if err := validateAuditMetadata(raw); err == nil {
			t.Fatalf("validateAuditMetadata(%s) succeeded", raw)
		}
	}
	if err := validateAuditMetadata(json.RawMessage(`{"ip":"10.0.0.5","changed_fields":["name"]}`)); err != nil {
		t.Fatalf("validateAuditMetadata(safe) error = %v", err)
	}
}

func TestConfigRejectsUnsafePoolBoundsAndInvalidURL(t *testing.T) {
	for _, cfg := range []Config{
		{DatabaseURL: "https://db/relayhub", MaxConnections: 4},
		{DatabaseURL: "postgres://db/relayhub", MaxConnections: 0},
		{DatabaseURL: "postgres://db/relayhub", MaxConnections: 2, MinConnections: 3},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate(%#v) succeeded", cfg)
		}
	}
}

func TestEmbeddedMigrationsAreOrderedAndImmutable(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) == 0 || migrations[0].Version != 1 {
		t.Fatalf("loadMigrations() = %#v", migrations)
	}
	for index, migration := range migrations {
		if migration.Version != int64(index+1) || migration.Name == "" || migration.Checksum == "" || len(migration.SQL) == 0 {
			t.Fatalf("invalid migration %d: %#v", index, migration)
		}
	}
}
