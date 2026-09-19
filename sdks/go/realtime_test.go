package relayhub

import (
	"encoding/json"
	"testing"
)

func TestRealtimeV2ContractsAllowBoundedNamespaceGrantAndRejectUnboundedWildcards(t *testing.T) {
	valid := RealtimeTokenRequest{ClientID: "client_1", Channels: map[string][]string{"support.room_1": {"subscribe", "publish", "presence"}}, TTLSeconds: 600}
	if !validRealtimeTokenRequest(valid) {
		t.Fatal("valid realtime token request rejected")
	}
	invalid := []RealtimeTokenRequest{
		{ClientID: "", Channels: valid.Channels, TTLSeconds: 600},
		{ClientID: "client", Channels: map[string][]string{"*": {"subscribe"}}, TTLSeconds: 600},
		{ClientID: "client", Channels: map[string][]string{"tenant:*:orders": {"subscribe"}}, TTLSeconds: 600},
		{ClientID: "client", Channels: map[string][]string{"tenant:42:*:*": {"subscribe"}}, TTLSeconds: 600},
		{ClientID: "client", Channels: map[string][]string{"room": {"admin"}}, TTLSeconds: 600},
	}
	namespace := RealtimeTokenRequest{ClientID: "client", Channels: map[string][]string{"tenant:42:*": {"subscribe", "publish", "history"}}, TTLSeconds: 600}
	if !validRealtimeTokenRequest(namespace) {
		t.Fatal("bounded namespace grant rejected")
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

func TestRealtimeV2HistoryAndBatchValidation(t *testing.T) {
	if !validRealtimeHistoryRequest(RealtimeHistoryRequest{Limit: 100, Cursor: "MTIzNC0w"}) {
		t.Fatal("valid history request rejected")
	}
	for _, request := range []RealtimeHistoryRequest{{Limit: 0}, {Limit: 101}, {Limit: 10, Cursor: "bad cursor"}} {
		if validRealtimeHistoryRequest(request) {
			t.Fatalf("invalid history request accepted: %#v", request)
		}
	}
	valid := []RealtimePublishItem{{ID: "one", Channel: "room", Data: map[string]any{"n": 1}}}
	if !validRealtimePublishBatch(valid) {
		t.Fatal("valid batch rejected")
	}
	if validRealtimePublishBatch([]RealtimePublishItem{{ID: "same", Channel: "room", Data: map[string]any{}}, {ID: "same", Channel: "room", Data: map[string]any{}}}) {
		t.Fatal("duplicate batch item ID accepted")
	}
}

func TestRealtimeV2HistoryFrameDecodesContinuityCursor(t *testing.T) {
	var frame RealtimeFrame
	if err := json.Unmarshal([]byte(`{"type":"history.result","channel":"room","continuity_cursor":"MTIzNC0w","items":[]}`), &frame); err != nil || frame.ContinuityCursor != "MTIzNC0w" {
		t.Fatalf("frame=%#v error=%v", frame, err)
	}
}

func TestRealtimeV2RewindCapsResolvedChannels(t *testing.T) {
	channels := make([]string, 11)
	for index := range channels {
		channels[index] = "room:" + string(rune('a'+index))
	}
	if validRealtimeRewindChannels(channels) {
		t.Fatal("rewind accepted more than 10 resolved channels")
	}
}
