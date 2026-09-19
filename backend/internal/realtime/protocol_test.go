package realtime

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProtocolDecode(t *testing.T) {
	for _, raw := range []string{`{"type":"subscribe","topics":["events","jobs"]}`, `{"type":"ping"}`, `{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":{"value":42}}`, `{"type":"rpc.result","invocation_id":"inv_1","ok":false,"error":{"code":"failed","message":"Try later"}}`, `{"type":"subscribe","topics":["functions"]}`} {
		f, err := DecodeClientFrame([]byte(raw))
		if err != nil || f.Type == "" {
			t.Fatalf("decode %s: %#v %v", raw, f, err)
		}
	}
}
func TestProtocolErrors(t *testing.T) {
	tests := []struct{ raw, code string }{
		{`{`, "invalid_json"}, {`null`, "invalid_frame"}, {`{"type":"unknown"}`, "unknown_type"},
		{`{"type":"subscribe"}`, "invalid_topics"}, {`{"type":"subscribe","topics":[]}`, "invalid_topics"},
		{`{"type":"subscribe","topics":["events","events"]}`, "invalid_topics"},
		{`{"type":"subscribe","topics":["secret"]}`, "invalid_topics"},
		{`{"type":"subscribe","topics":["events"],"app_id":"victim"}`, "invalid_frame"},
		{`{"type":"rpc.result","ok":true,"result":{}}`, "invalid_rpc_result"},
		{`{"type":"rpc.result","invocation_id":"inv_1","result":{}}`, "invalid_rpc_result"},
		{`{"type":"rpc.result","invocation_id":"inv_1","ok":true}`, "invalid_rpc_result"},
		{`{"type":"rpc.result","invocation_id":"inv_1","ok":false}`, "invalid_rpc_result"},
		{`{"type":"ping"} {}`, "invalid_json"},
		{strings.Repeat("x", 65537), "frame_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.code+tt.raw[:1], func(t *testing.T) {
			_, err := DecodeClientFrame([]byte(tt.raw))
			if err == nil || err.Code != tt.code {
				t.Fatalf("got %v want %s", err, tt.code)
			}
			b, _ := json.Marshal(ErrorFrame(err))
			var got map[string]any
			_ = json.Unmarshal(b, &got)
			if len(got) != 3 || got["type"] != "error" || got["code"] != tt.code || got["message"] == "" {
				t.Fatalf("error envelope %s", b)
			}
		})
	}
	if MaxInboundBytes != 65536 {
		t.Fatal("limit changed")
	}
	raw := `{"type":"ping"}`
	_, err := DecodeClientFrame([]byte(raw + strings.Repeat(" ", 65536-len(raw))))
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCFrameContract(t *testing.T) {
	for _, raw := range []string{
		`{"type":"rpc.result","invocation_id":"inv_1","ok":false,"error":{}}`,
		`{"type":"rpc.result","invocation_id":"inv_1","ok":false,"error":"failed"}`,
		`{"type":"rpc.result","invocation_id":"inv_1","ok":false,"error":{"code":"oops","message":"Failed","secret":"no"}}`,
		`{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":{},"error":{}}`,
		`{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":{},"app_id":"owner"}`,
	} {
		if _, e := DecodeClientFrame([]byte(raw)); e == nil {
			t.Fatalf("invalid result accepted %s", raw)
		}
	}
	prefix := `{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":"`
	wire := prefix + strings.Repeat("x", 65536-len(prefix)-2) + `"}`
	if _, e := DecodeClientFrame([]byte(wire)); e != nil {
		t.Fatalf("exact boundary rejected %v", e)
	}
	if _, e := DecodeClientFrame([]byte(wire + " ")); e == nil || e.Code != "frame_too_large" {
		t.Fatalf("oversize %v", e)
	}
	frame := ServerFrame{Type: "rpc.invoke", InvocationID: "inv_1", Function: "calculate", Input: json.RawMessage(`{"n":9007199254740993}`), Deadline: "2026-09-11T10:00:01Z"}
	raw, e := json.Marshal(frame)
	if e != nil || string(raw) != `{"type":"rpc.invoke","invocation_id":"inv_1","function":"calculate","input":{"n":9007199254740993},"deadline":"2026-09-11T10:00:01Z"}` {
		t.Fatalf("wire %s %v", raw, e)
	}
}

func TestRealtimeV2ProtocolFrames(t *testing.T) {
	valid := []string{
		`{"type":"subscribe","channels":["support.room_42"]}`,
		`{"type":"unsubscribe","channels":["support.room_42"]}`,
		`{"type":"channel.publish","channel":"support.room_42","data":{"text":"hello"}}`,
		`{"type":"channel.publish","channel":"support.room_42","audience":{"type":"others"},"data":{"text":"hello"}}`,
		`{"type":"channel.publish","channel":"support.room_42","audience":{"type":"connection","connection_id":"conn_1"},"data":{"text":"hello"}}`,
		`{"type":"channel.publish","channel":"support.room_42","audience":{"type":"client","client_id":"client_2"},"data":{"text":"hello"}}`,
		`{"type":"ping"}`,
		`{"type":"presence.update","channel":"support.room_42","data":{"status":"online"}}`,
	}
	for _, raw := range valid {
		if _, err := DecodeClientFrameV2([]byte(raw)); err != nil {
			t.Fatalf("DecodeClientFrameV2(%s) error=%v", raw, err)
		}
	}

	invalid := []string{
		`{"type":"subscribe","channels":["*"]}`,
		`{"type":"subscribe","channels":["room","room"]}`,
		`{"type":"unsubscribe","channels":[]}`,
		`{"type":"channel.publish","channel":"room","data":[]}`,
		`{"type":"channel.publish","channel":"room","audience":{"type":"connection"},"data":{}}`,
		`{"type":"channel.publish","channel":"room","audience":{"type":"client","client_id":"bad id"},"data":{}}`,
		`{"type":"channel.publish","channel":"room","audience":{"type":"all","client_id":"forged"},"data":{}}`,
		`{"type":"channel.publish","channel":"room","publisher_client_id":"forged","data":{}}`,
		`{"type":"presence.update","channel":"*","data":{}}`,
		`{"type":"presence.update","channel":"room","data":[]}`,
	}
	for _, raw := range invalid {
		if _, err := DecodeClientFrameV2([]byte(raw)); err == nil {
			t.Fatalf("DecodeClientFrameV2 accepted %s", raw)
		}
	}
}

func TestRealtimeV2HistoryAndRewindFramesAreBounded(t *testing.T) {
	valid := []string{
		`{"type":"history.get","channel":"tenant:42:orders","limit":25}`,
		`{"type":"history.get","channel":"tenant:42:orders","limit":100,"cursor":"MTIzNC0w"}`,
		`{"type":"subscribe","channels":["tenant:42:orders"],"rewind":{"limit":10}}`,
	}
	for _, raw := range valid {
		if _, err := DecodeClientFrameV2([]byte(raw)); err != nil {
			t.Fatalf("DecodeClientFrameV2(%s) error=%v", raw, err)
		}
	}
	invalid := []string{
		`{"type":"history.get","channel":"room","limit":0}`,
		`{"type":"history.get","channel":"room","limit":101}`,
		`{"type":"history.get","channel":"room","limit":10,"cursor":"not a cursor!"}`,
		`{"type":"subscribe","channels":["room"],"rewind":{"limit":101}}`,
		`{"type":"unsubscribe","channels":["room"],"rewind":{"limit":10}}`,
	}
	for _, raw := range invalid {
		if _, err := DecodeClientFrameV2([]byte(raw)); err == nil {
			t.Fatalf("DecodeClientFrameV2 accepted %s", raw)
		}
	}
}

func TestRealtimeV2RewindCapsResolvedChannels(t *testing.T) {
	channels := make([]string, 11)
	for index := range channels {
		channels[index] = `"room:` + string(rune('a'+index)) + `"`
	}
	raw := `{"type":"subscribe","channels":[` + strings.Join(channels, ",") + `],"rewind":{"limit":10}}`
	if _, err := DecodeClientFrameV2([]byte(raw)); err == nil || err.Code != "invalid_history" {
		t.Fatalf("DecodeClientFrameV2 rewind error=%v", err)
	}
}

func TestRealtimeV2BatchPublishRequiresUniqueBoundedItems(t *testing.T) {
	valid := `{"type":"channel.publish.batch","items":[{"id":"one","channel":"tenant:42:orders","data":{"n":1}},{"id":"two","channel":"tenant:42:updates","audience":{"type":"others"},"data":{"n":2}}]}`
	frame, err := DecodeClientFrameV2([]byte(valid))
	if err != nil || len(frame.Items) != 2 || frame.Items[1].ID != "two" {
		t.Fatalf("frame=%#v error=%v", frame, err)
	}
	invalid := []string{
		`{"type":"channel.publish.batch","items":[]}`,
		`{"type":"channel.publish.batch","items":[{"id":"same","channel":"room","data":{}},{"id":"same","channel":"room","data":{}}]}`,
		`{"type":"channel.publish.batch","items":[{"id":"bad id","channel":"room","data":{}}]}`,
		`{"type":"channel.publish.batch","items":[{"id":"one","channel":"room","data":[]}]}`,
	}
	for _, raw := range invalid {
		if _, err := DecodeClientFrameV2([]byte(raw)); err == nil {
			t.Fatalf("DecodeClientFrameV2 accepted %s", raw)
		}
	}
}
