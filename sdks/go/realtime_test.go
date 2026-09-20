package relayhub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func realtimeSocketPair(t *testing.T) (*RealtimeConn, *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(response, request, nil)
		if err == nil {
			accepted <- connection
		}
	}))
	t.Cleanup(server.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &RealtimeConn{connection: client}, <-accepted
}

type realtimeTestKeys struct{ key []byte }

func (keys realtimeTestKeys) EncryptionKey(context.Context, string) (string, []byte, error) {
	return "key-1", append([]byte(nil), keys.key...), nil
}
func (keys realtimeTestKeys) DecryptionKey(context.Context, string, string) ([]byte, error) {
	return append([]byte(nil), keys.key...), nil
}

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

func TestRealtimeV2EncryptionRoundTripUsesExplicitKeyProvider(t *testing.T) {
	provider := realtimeTestKeys{key: []byte("0123456789abcdef0123456789abcdef")}
	envelope, err := EncryptRealtimePayload(context.Background(), provider, "private:room", map[string]any{"secret": "hello", "n": 7})
	if err != nil || envelope.Algorithm != "aes-256-gcm" || envelope.KeyID != "key-1" || envelope.Ciphertext == "" {
		t.Fatalf("envelope=%#v error=%v", envelope, err)
	}
	var output struct {
		Secret string `json:"secret"`
		N      int    `json:"n"`
	}
	frame := RealtimeFrame{Type: "channel.message", Channel: "private:room", Encryption: &envelope}
	if err := DecryptRealtimeFrame(context.Background(), provider, frame, &output); err != nil || output.Secret != "hello" || output.N != 7 {
		t.Fatalf("output=%#v error=%v", output, err)
	}
	frame.Channel = "private:other"
	if err := DecryptRealtimeFrame(context.Background(), provider, frame, &output); err == nil {
		t.Fatal("ciphertext replayed into another channel")
	}
}

func TestRealtimeV2EncryptionRejectsPublicChannelAndWrongKeySize(t *testing.T) {
	provider := realtimeTestKeys{key: []byte("short")}
	if _, err := EncryptRealtimePayload(context.Background(), provider, "private:room", map[string]any{}); err == nil {
		t.Fatal("short key accepted")
	}
	provider.key = []byte("0123456789abcdef0123456789abcdef")
	if _, err := EncryptRealtimePayload(context.Background(), provider, "public:room", map[string]any{}); err == nil {
		t.Fatal("public encrypted channel accepted")
	}
}

func TestRealtimeV2MessageActionsUseTypedBoundedFrames(t *testing.T) {
	connection, peer := realtimeSocketPair(t)
	defer peer.Close()
	done := make(chan []map[string]any, 1)
	go func() {
		frames := make([]map[string]any, 0, 3)
		for range 3 {
			var frame map[string]any
			_ = peer.ReadJSON(&frame)
			frames = append(frames, frame)
		}
		done <- frames
	}()
	if err := connection.PutMessageAction("room", "msg_1", "reaction", "idem_1", map[string]any{"emoji": "👍"}); err != nil {
		t.Fatal(err)
	}
	if err := connection.ListMessageActions("room", "msg_1"); err != nil {
		t.Fatal(err)
	}
	if err := connection.RemoveMessageAction("room", "msg_1", "action_1"); err != nil {
		t.Fatal(err)
	}
	frames := <-done
	if frames[0]["type"] != "message.action.put" || frames[1]["type"] != "message.actions.get" || frames[2]["type"] != "message.action.remove" {
		t.Fatalf("frames=%#v", frames)
	}
	if err := connection.PutMessageAction("room", "msg_1", "moderate", "idem_2", map[string]any{}); err != ErrInvalidInput {
		t.Fatalf("invalid action error=%v", err)
	}
}
