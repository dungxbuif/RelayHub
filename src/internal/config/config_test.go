package config

import "testing"

func TestDefaultLoopback(t *testing.T) {
	c, err := Load(func(string) string { return "" })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Addr != "127.0.0.1:8080" {
		t.Fatalf("Addr=%q", c.Addr)
	}
	if c.BackendToken == "" || c.WorkerToken == "" || c.RealtimeToken == "" {
		t.Fatal("missing default tokens")
	}
	if c.BackendToken == c.WorkerToken || c.BackendToken == c.RealtimeToken || c.WorkerToken == c.RealtimeToken {
		t.Fatal("default tokens must differ")
	}
}

func TestAddressAndTokenValidation(t *testing.T) {
	for _, addr := range []string{"localhost", ":8080", "127.0.0.1:-1", "127.0.0.1:99999", "127.0.0.1:http"} {
		t.Run(addr, func(t *testing.T) {
			_, err := Load(func(string) string { return addr })
			if err == nil {
				t.Fatalf("accepted invalid addr %q", addr)
			}
		})
	}
	for _, key := range []struct {
		name  string
		value string
	}{
		{"BACKEND", "same"},
		{"WORKER", "same"},
		{"REALTIME", "same"},
	} {
		t.Run("token-distinct-"+key.name, func(t *testing.T) {
			_, err := Load(func(k string) string {
				switch k {
				case "RELAYHUB_ADDR":
					return "127.0.0.1:8080"
				case "RELAYHUB_BACKEND_TOKEN":
					return "same"
				case "RELAYHUB_WORKER_TOKEN":
					return "same"
				case "RELAYHUB_REALTIME_TOKEN":
					return "same"
				}
				return ""
			})
			if err == nil {
				t.Fatal("accepted overlapping tokens")
			}
		})
	}

	_, err := Load(func(s string) string {
		switch s {
		case "RELAYHUB_ADDR":
			return "127.0.0.1:0"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("ephemeral port: %v", err)
	}
}
