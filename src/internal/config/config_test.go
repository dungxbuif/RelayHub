package config

import "testing"

func TestDefaultLoopback(t *testing.T) {
	c, err := Load(func(string) string { return "" })
	if err != nil || c.Addr != "127.0.0.1:8080" {
		t.Fatalf("config=%+v err=%v", c, err)
	}
}
func TestAddressValidation(t *testing.T) {
	for _, addr := range []string{"localhost", ":8080", "127.0.0.1:-1", "127.0.0.1:99999", "127.0.0.1:http"} {
		t.Run(addr, func(t *testing.T) {
			_, err := Load(func(string) string { return addr })
			if err == nil {
				t.Fatalf("accepted invalid addr %q", addr)
			}
		})
	}
	c, err := Load(func(string) string { return "127.0.0.1:0" })
	if err != nil || c.Addr != "127.0.0.1:0" {
		t.Fatalf("ephemeral port: %+v %v", c, err)
	}
}
