package natsbroker

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConnectRejectsInvalidOptionsWithoutLeakingCredentials(t *testing.T) {
	secretURL := "https://operator:private-password@example.test:4222"
	_, err := Connect(Options{URL: secretURL})
	if err == nil {
		t.Fatal("Connect() error = nil, want invalid URL rejection")
	}
	if strings.Contains(err.Error(), "operator") || strings.Contains(err.Error(), "private-password") {
		t.Fatalf("Connect() error leaked credentials: %q", err)
	}
}

func TestConnectBoundsInitialFailure(t *testing.T) {
	started := time.Now()
	_, err := Connect(Options{URL: "nats://127.0.0.1:1", ConnectTimeout: 50 * time.Millisecond, MaxReconnects: 0})
	if err == nil {
		t.Fatal("Connect() error = nil, want unavailable server error")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("Connect() took %s, want bounded initial failure", time.Since(started))
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Connect() error = %v, want ErrUnavailable", err)
	}
}
