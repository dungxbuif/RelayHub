package auth

import (
	"errors"
	"testing"
	"time"
)

func TestSignMatchesHandDerivedCanonicalLiteral(t *testing.T) {
	body := []byte(`{"scopes":["ws:connect"],"ttl_seconds":600}`)
	got := Sign([]byte("test-secret"), "1770000000", "POST", "/api/v1/socket/token?audience=browser", body)
	const want = "4b6a6ada471e93240169f8878e877be6aa8d8cbf764e48f4a9d1b3c2ffef82ca"
	if got != want {
		t.Fatalf("Sign() = %q, want hand-derived %q", got, want)
	}
}

func TestVerifyRejectsChangesToEverySignedComponent(t *testing.T) {
	secret := []byte("test-secret")
	timestamp := "1770000000"
	method := "POST"
	target := "/api/v1/socket/token?audience=browser"
	body := []byte(`{"scopes":["ws:connect"],"ttl_seconds":600}`)
	signature := Sign(secret, timestamp, method, target, body)
	now := time.Unix(1770000000, 0)

	tests := []struct {
		name   string
		method string
		target string
		body   []byte
	}{
		{name: "method", method: "GET", target: target, body: body},
		{name: "request target", method: method, target: "/api/v1/socket/token", body: body},
		{name: "body", method: method, target: target, body: []byte(`{"scopes":["ws:read"],"ttl_seconds":600}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify(secret, timestamp, tt.method, tt.target, signature, tt.body, now, 5*time.Minute)
			if !errors.Is(err, ErrInvalidSignature) {
				t.Fatalf("Verify() error = %v, want ErrInvalidSignature", err)
			}
		})
	}
}

func TestVerifyAcceptsInclusiveTimestampBoundary(t *testing.T) {
	now := time.Unix(1770000000, 900_000_000)
	secret := []byte("boundary-secret")

	for _, unix := range []int64{now.Add(-300 * time.Second).Unix(), now.Add(300 * time.Second).Unix()} {
		timestamp := time.Unix(unix, 0).Format("150405")
		// Format the known Unix second without sharing parsing logic with Verify.
		if unix == 1769999700 {
			timestamp = "1769999700"
		} else {
			timestamp = "1770000300"
		}
		signature := Sign(secret, timestamp, "GET", "/api/v1/apps/app_example", nil)
		if err := Verify(secret, timestamp, "GET", "/api/v1/apps/app_example", signature, nil, now, 300*time.Second); err != nil {
			t.Fatalf("Verify() at timestamp %s error = %v, want nil", timestamp, err)
		}
	}
}

func TestVerifyRejectsExpiredReplayTimestamp(t *testing.T) {
	now := time.Unix(1770000000, 0)
	secret := []byte("boundary-secret")
	timestamp := "1769999699"
	signature := Sign(secret, timestamp, "GET", "/api/v1/apps/app_example", nil)

	err := Verify(secret, timestamp, "GET", "/api/v1/apps/app_example", signature, nil, now, 300*time.Second)
	if !errors.Is(err, ErrTimestampExpired) {
		t.Fatalf("Verify() error = %v, want ErrTimestampExpired", err)
	}
}

func TestVerifyRejectsMalformedHeaders(t *testing.T) {
	now := time.Unix(1770000000, 0)
	tests := []struct {
		name      string
		timestamp string
		signature string
	}{
		{name: "missing timestamp", signature: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "non decimal timestamp", timestamp: "today", signature: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "missing signature", timestamp: "1770000000"},
		{name: "short signature", timestamp: "1770000000", signature: "abcd"},
		{name: "non hex signature", timestamp: "1770000000", signature: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
		{name: "uppercase signature", timestamp: "1770000000", signature: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify([]byte("secret"), tt.timestamp, "GET", "/api/v1/apps/app_example", tt.signature, nil, now, 5*time.Minute)
			if !errors.Is(err, ErrMalformedAuthentication) {
				t.Fatalf("Verify() error = %v, want ErrMalformedAuthentication", err)
			}
		})
	}
}
