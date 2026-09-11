package streamprotocol

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type invalidFixture struct {
	Name         string          `json:"name"`
	Frame        json.RawMessage `json:"frame"`
	Wire         string          `json:"wire"`
	WireBase64   string          `json:"wire_base64"`
	Generate     string          `json:"generate"`
	Semantic     string          `json:"semantic"`
	AppID        string          `json:"app_id"`
	ConnectionID string          `json:"connection_id"`
	ErrorCode    string          `json:"error_code"`
}

func fixture(t *testing.T, name string, target any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("fixtures", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}

func TestValidClientFixtures(t *testing.T) {
	var frames []json.RawMessage
	fixture(t, "client.valid.json", &frames)
	if len(frames) != 7 {
		t.Fatalf("expected every client frame, got %d", len(frames))
	}
	for _, raw := range frames {
		frame, err := DecodeClientFrame(raw)
		if err != nil || frame.Type == "" {
			t.Fatalf("%s: %#v", raw, err)
		}
	}
}

func TestInvalidClientFixtures(t *testing.T) {
	var cases []invalidFixture
	fixture(t, "client.invalid.json", &cases)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			_, got := DecodeClientFrame(tc.Frame)
			if got == nil || got.Code != tc.ErrorCode {
				t.Fatalf("got %#v, want %s", got, tc.ErrorCode)
			}
		})
	}
}

func TestValidAndInvalidServerFixtures(t *testing.T) {
	var valid []json.RawMessage
	fixture(t, "server.valid.json", &valid)
	if len(valid) != 7 {
		t.Fatalf("expected every server frame, got %d", len(valid))
	}
	for _, raw := range valid {
		if err := ValidateServerFrame(raw); err != nil {
			t.Fatalf("%s: %#v", raw, err)
		}
	}
	var invalid []invalidFixture
	fixture(t, "server.invalid.json", &invalid)
	for _, tc := range invalid {
		if err := ValidateServerFrame(tc.Frame); err == nil {
			t.Fatalf("accepted %s", tc.Name)
		}
	}
}

func TestWireAndOwnershipFixtures(t *testing.T) {
	var cases []invalidFixture
	fixture(t, "wire.invalid.json", &cases)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Semantic == "assignment" {
				frame, err := DecodeClientFrame(tc.Frame)
				if err != nil {
					t.Fatal(err)
				}
				got := ValidateAssignment(frame, tc.AppID, tc.ConnectionID, map[string]Assignment{})
				if got == nil || got.Code != tc.ErrorCode {
					t.Fatalf("got %#v", got)
				}
				return
			}
			var raw []byte
			switch {
			case tc.Wire != "":
				raw = []byte(tc.Wire)
			case tc.WireBase64 != "":
				raw, _ = base64.StdEncoding.DecodeString(tc.WireBase64)
			case tc.Generate == "oversize_ping":
				raw = []byte(`{"type":"ping","padding":"` + strings.Repeat("x", MaxMessageBytes) + `"}`)
			default:
				t.Fatal("fixture has no wire")
			}
			_, got := DecodeClientFrame(raw)
			if got == nil || got.Code != tc.ErrorCode {
				t.Fatalf("got %#v, want %s", got, tc.ErrorCode)
			}
		})
	}
}

func TestProtocolConstants(t *testing.T) {
	if Version != 1 || Subprotocol != "relayhub.stream.v1" || DefaultConsumer != "default" || MaxMessageBytes != 65536 {
		t.Fatal("public constants changed")
	}
	for _, code := range []string{"invalid_utf8", "frame_too_large", "invalid_json", "duplicate_key", "invalid_frame", "unknown_type", "unsupported_version", "delivery_not_assigned", "stale_delivery", "consumer_already_started", "consumer_unavailable", "backpressure", "function_not_assigned", "internal_error"} {
		if !StableErrorCode(code) {
			t.Errorf("missing stable error %q", code)
		}
	}
	if CloseProtocolViolation != 4400 || CloseAuthenticationFailed != 4401 || CloseForbidden != 4403 || CloseUnsupportedVersion != 4406 || CloseTimeout != 4408 || CloseBackpressure != 4429 || CloseDependencyUnavailable != 4503 {
		t.Fatal("public close codes changed")
	}
	if CloseCodeFor(&ProtocolError{Code: "frame_too_large"}) != 1009 || CloseCodeFor(&ProtocolError{Code: "unsupported_version"}) != CloseUnsupportedVersion || CloseCodeFor(&ProtocolError{Code: "consumer_unavailable"}) != CloseDependencyUnavailable {
		t.Fatal("protocol errors map to unexpected close codes")
	}
}

func TestMessageByteBoundaries(t *testing.T) {
	raw := []byte(`{"type":"ping"}`)
	raw = append(raw, []byte(strings.Repeat(" ", MaxMessageBytes-len(raw)))...)
	if _, err := DecodeClientFrame(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeClientFrame(append(raw, ' ')); err == nil || err.Code != "frame_too_large" {
		t.Fatalf("got %#v", err)
	}
}
