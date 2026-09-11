package realtime

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProtocolDecode(t *testing.T) {
	for _, raw := range []string{`{"type":"subscribe","topics":["events","jobs"]}`, `{"type":"ping"}`, `{"type":"rpc.result","invocation_id":"inv_1","ok":true,"result":{"value":42}}`, `{"type":"rpc.result","invocation_id":"inv_1","ok":false,"error":"failed"}`} {
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
		{`{"type":"subscribe","topics":["functions"]}`, "unauthorized_topic"},
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
