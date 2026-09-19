package relayhub

import "testing"

func TestRealtimeV2ContractsRejectWildcardsAndInvalidAudiences(t *testing.T) {
	valid := RealtimeTokenRequest{ClientID: "client_1", Channels: map[string][]string{"support.room_1": {"subscribe", "publish", "presence"}}, TTLSeconds: 600}
	if !validRealtimeTokenRequest(valid) {
		t.Fatal("valid realtime token request rejected")
	}
	invalid := []RealtimeTokenRequest{
		{ClientID: "", Channels: valid.Channels, TTLSeconds: 600},
		{ClientID: "client", Channels: map[string][]string{"*": {"subscribe"}}, TTLSeconds: 600},
		{ClientID: "client", Channels: map[string][]string{"room": {"admin"}}, TTLSeconds: 600},
	}
	for _, request := range invalid {
		if validRealtimeTokenRequest(request) {
			t.Fatalf("invalid request accepted: %#v", request)
		}
	}
	if validAudience(RealtimeAudience{Type: "connection"}) || validAudience(RealtimeAudience{Type: "all", ClientID: "forged"}) {
		t.Fatal("invalid audience accepted")
	}
}
