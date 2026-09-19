package domain

import "testing"

func TestRealtimeChannelGrantAcceptsExactOrOneTerminalNamespaceSegment(t *testing.T) {
	valid := []string{"room", "tenant:42:orders", "tenant:42:*"}
	for _, grant := range valid {
		if !ValidRealtimeChannelGrant(grant) {
			t.Fatalf("ValidRealtimeChannelGrant(%q) = false", grant)
		}
	}
	invalid := []string{"*", "tenant:*:orders", "tenant:**", "tenant:", "tenant:42:*:*", "Tenant:42:*"}
	for _, grant := range invalid {
		if ValidRealtimeChannelGrant(grant) {
			t.Fatalf("ValidRealtimeChannelGrant(%q) = true", grant)
		}
	}
}

func TestRealtimeChannelGrantMatchesOnlyOneCompleteSegment(t *testing.T) {
	tests := []struct {
		grant, channel string
		want           bool
	}{
		{"room", "room", true},
		{"room", "room:1", false},
		{"tenant:42:*", "tenant:42:orders", true},
		{"tenant:42:*", "tenant:42:orders:created", false},
		{"tenant:42:*", "tenant:43:orders", false},
		{"tenant:42:*", "tenant:42", false},
	}
	for _, test := range tests {
		if got := RealtimeChannelGrantMatches(test.grant, test.channel); got != test.want {
			t.Fatalf("RealtimeChannelGrantMatches(%q, %q) = %v, want %v", test.grant, test.channel, got, test.want)
		}
	}
}
