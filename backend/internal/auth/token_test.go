package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTokenIssuerRoundTripsAppAndExplicitScopes(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	issuer := NewTokenIssuer([]byte("socket-signing-secret"), func() time.Time { return now })

	token, err := issuer.Issue("app_orders", []string{"ws:connect", "ws:read"}, 10*time.Minute)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if strings.Count(token, ".") != 2 || strings.ContainsAny(token, "+/=") {
		t.Fatalf("Issue() token = %q, want three URL-safe unpadded segments", token)
	}

	claims, err := issuer.Verify(token, "ws:connect")
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if claims.Version != 1 || claims.AppID != "app_orders" {
		t.Fatalf("Verify() claims = %#v, want version 1 app_orders", claims)
	}
	if !claims.IssuedAt.Equal(now) || !claims.ExpiresAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("Verify() times = %v to %v, want %v to %v", claims.IssuedAt, claims.ExpiresAt, now, now.Add(10*time.Minute))
	}
}

func TestTokenIssuerRejectsTampering(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	issuer := NewTokenIssuer([]byte("socket-signing-secret"), func() time.Time { return now })
	token, err := issuer.Issue("app_orders", []string{"ws:connect"}, time.Minute)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	last := token[len(token)-1]
	replacement := byte('A')
	if last == replacement {
		replacement = 'B'
	}
	tampered := token[:len(token)-1] + string(replacement)

	_, err = issuer.Verify(tampered, "ws:connect")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("Verify(tampered) error = %v, want ErrInvalidToken", err)
	}
}

func TestTokenIssuerRejectsExpiredToken(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	current := now
	issuer := NewTokenIssuer([]byte("socket-signing-secret"), func() time.Time { return current })
	token, err := issuer.Issue("app_orders", []string{"ws:connect"}, time.Minute)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	current = now.Add(time.Minute)

	_, err = issuer.Verify(token, "ws:connect")
	if !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("Verify(expired) error = %v, want ErrTokenExpired", err)
	}
}

func TestTokenIssuerRequiresRequestedScope(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	issuer := NewTokenIssuer([]byte("socket-signing-secret"), func() time.Time { return now })
	token, err := issuer.Issue("app_orders", []string{"ws:connect"}, time.Minute)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}

	_, err = issuer.Verify(token, "ws:read")
	if !errors.Is(err, ErrMissingScope) {
		t.Fatalf("Verify(missing scope) error = %v, want ErrMissingScope", err)
	}
}

func TestTokenIssuerValidatesScopes(t *testing.T) {
	issuer := NewTokenIssuer([]byte("socket-signing-secret"), func() time.Time {
		return time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	})

	tests := []struct {
		name   string
		scopes []string
	}{
		{name: "missing", scopes: nil},
		{name: "empty", scopes: []string{""}},
		{name: "malformed", scopes: []string{"ws connect"}},
		{name: "duplicate", scopes: []string{"ws:connect", "ws:connect"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := issuer.Issue("app_orders", tt.scopes, time.Minute)
			if !errors.Is(err, ErrInvalidScope) {
				t.Fatalf("Issue() error = %v, want ErrInvalidScope", err)
			}
		})
	}
}

func TestTokenIssuerEnforcesMaximumTTLInclusively(t *testing.T) {
	issuer := NewTokenIssuer([]byte("socket-signing-secret"), func() time.Time {
		return time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	})

	if _, err := issuer.Issue("app_orders", []string{"ws:connect"}, 15*time.Minute); err != nil {
		t.Fatalf("Issue(15m) error = %v, want nil", err)
	}
	for _, ttl := range []time.Duration{0, -time.Second, 15*time.Minute + time.Second} {
		if _, err := issuer.Issue("app_orders", []string{"ws:connect"}, ttl); !errors.Is(err, ErrInvalidTTL) {
			t.Fatalf("Issue(%v) error = %v, want ErrInvalidTTL", ttl, err)
		}
	}
}

func TestTokenIssuerRoundTripsRealtimeV2IdentityAndCapabilities(t *testing.T) {
	now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	issuer := NewTokenIssuer([]byte("realtime-v2-secret"), func() time.Time { return now })
	token, err := issuer.IssueRealtime("app_orders", "client_42", map[string][]string{"support.room_42": {"subscribe", "publish", "presence"}}, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := issuer.Verify(token, "ws:connect")
	if err != nil || claims.ClientID != "client_42" || len(claims.Channels["support.room_42"]) != 3 {
		t.Fatalf("claims=%#v error=%v", claims, err)
	}
	for _, invalid := range []struct {
		client   string
		channels map[string][]string
	}{
		{"", map[string][]string{"room": {"subscribe"}}},
		{"client", map[string][]string{"*": {"subscribe"}}},
		{"client", map[string][]string{"Bad": {"subscribe"}}},
		{"client", map[string][]string{"room": {"admin"}}},
	} {
		if _, err := issuer.IssueRealtime("app_orders", invalid.client, invalid.channels, time.Minute); err == nil {
			t.Fatalf("accepted invalid capability %#v", invalid)
		}
	}
}

func TestTokenIssuerRoundTripsBoundedRealtimeNamespaceGrant(t *testing.T) {
	now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	issuer := NewTokenIssuer([]byte("realtime-v2-secret"), func() time.Time { return now })
	token, err := issuer.IssueRealtime("app_orders", "client_42", map[string][]string{"tenant:42:*": {"subscribe", "publish", "history"}}, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := issuer.Verify(token, "ws:connect")
	if err != nil || len(claims.Channels["tenant:42:*"]) != 3 {
		t.Fatalf("claims=%#v error=%v", claims, err)
	}
	for _, grant := range []string{"*", "tenant:*:orders", "tenant:42:*:*"} {
		if _, err := issuer.IssueRealtime("app_orders", "client_42", map[string][]string{grant: {"subscribe"}}, time.Minute); err == nil {
			t.Fatalf("IssueRealtime accepted unbounded grant %q", grant)
		}
	}
}
