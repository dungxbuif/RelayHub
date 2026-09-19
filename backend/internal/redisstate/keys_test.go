package redisstate

import (
	"errors"
	"strings"
	"testing"
)

func TestKeyspaceBuildsExactClusterSafeKeys(t *testing.T) {
	t.Parallel()
	keys := Keyspace{Prefix: "rh"}

	tests := []struct {
		name string
		want string
		key  func() (string, error)
	}{
		{name: "admin session", want: "rh:{session:sess_1}:admin", key: func() (string, error) { return keys.AdminSession("sess_1") }},
		{name: "rate limit", want: "rh:{app:app_1}:rate:publish:1789920000", key: func() (string, error) { return keys.RateLimit("app_1", "publish", "1789920000") }},
		{name: "connection owner", want: "rh:{connection:conn_1}:owner", key: func() (string, error) { return keys.ConnectionOwner("conn_1") }},
		{name: "instance member", want: "rh:{instances}:member:api_1", key: func() (string, error) { return keys.InstanceMember("api_1") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.key()
			if err != nil {
				t.Fatalf("key error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("key = %q, want %q", got, tt.want)
			}
		})
	}
	if got := keys.InstanceIndex(); got != "rh:{instances}:live" {
		t.Fatalf("InstanceIndex() = %q", got)
	}
}

func TestKeyspaceRejectsInvalidPartsWithoutEchoingThem(t *testing.T) {
	t.Parallel()
	keys := Keyspace{Prefix: "rh"}
	invalid := []string{"", strings.Repeat("a", 129), "has space", "has\tcontrol", "has\ncontrol", "has{brace", "has}brace"}

	for _, part := range invalid {
		part := part
		t.Run(strings.ReplaceAll(part, "\n", "newline"), func(t *testing.T) {
			t.Parallel()
			builders := []func() (string, error){
				func() (string, error) { return keys.AdminSession(part) },
				func() (string, error) { return keys.RateLimit(part, "publish", "1") },
				func() (string, error) { return keys.RateLimit("app_1", part, "1") },
				func() (string, error) { return keys.RateLimit("app_1", "publish", part) },
				func() (string, error) { return keys.ConnectionOwner(part) },
				func() (string, error) { return keys.InstanceMember(part) },
			}
			for _, build := range builders {
				got, err := build()
				if got != "" || !errors.Is(err, ErrInvalidKeyPart) {
					t.Fatalf("build() = %q, %v; want ErrInvalidKeyPart", got, err)
				}
				if part != "" && strings.Contains(err.Error(), part) {
					t.Fatalf("error leaked rejected key part: %q", err)
				}
			}
		})
	}
}

func TestKeyspaceRejectsInvalidPrefix(t *testing.T) {
	t.Parallel()
	for _, prefix := range []string{"", "RH", "rh tenant", "rh{tenant}", strings.Repeat("a", 33)} {
		keys := Keyspace{Prefix: prefix}
		if _, err := keys.AdminSession("sess_1"); !errors.Is(err, ErrInvalidKeyPart) {
			t.Fatalf("prefix %q error = %v, want ErrInvalidKeyPart", prefix, err)
		}
		if got := keys.InstanceIndex(); got != "" {
			t.Fatalf("prefix %q InstanceIndex() = %q, want empty", prefix, got)
		}
	}
}
