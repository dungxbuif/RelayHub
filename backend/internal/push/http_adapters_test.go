package push

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

func TestHTTPAdaptersSendProviderSpecificRequestsWithoutLeakingCredentials(t *testing.T) {
	tests := []struct {
		name, provider, path, authorization string
		newAdapter                          func(string, *http.Client) (*HTTPAdapter, error)
	}{
		{name: "APNS", provider: "apns", path: "/3/device/device-token", authorization: "bearer apns-private", newAdapter: func(endpoint string, client *http.Client) (*HTTPAdapter, error) {
			return NewAPNSAdapter(endpoint, "bearer apns-private", "com.example.app", client)
		}},
		{name: "FCM", provider: "fcm", path: "/v1/projects/project-one/messages:send", authorization: "Bearer fcm-private", newAdapter: func(endpoint string, client *http.Client) (*HTTPAdapter, error) {
			return NewFCMAdapter(endpoint, "Bearer fcm-private", "project-one", client)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				body, _ := io.ReadAll(request.Body)
				if request.URL.RequestURI() != test.path || request.Header.Get("Authorization") != test.authorization || strings.Contains(string(body), test.authorization) {
					t.Fatalf("request path=%q authorization=%q body=%s", request.URL.RequestURI(), request.Header.Get("Authorization"), body)
				}
				var payload map[string]any
				if json.Unmarshal(body, &payload) != nil {
					t.Fatal("invalid provider JSON")
				}
				if test.provider == "apns" {
					response.Header().Set("apns-id", "apns-message")
					response.WriteHeader(http.StatusOK)
				} else {
					_, _ = io.WriteString(response, `{"name":"fcm-message"}`)
				}
			}))
			defer server.Close()
			adapter, err := test.newAdapter(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			id, err := adapter.Send(context.Background(), domain.PushDevice{Provider: test.provider, Token: []byte("device-token")}, domain.PushNotification{Title: "Ready", Data: json.RawMessage(`{"order_id":"o1"}`)})
			if err != nil || id != strings.ToLower(test.name)+"-message" {
				t.Fatalf("id=%q error=%v", id, err)
			}
		})
	}
}

func TestHTTPAdapterRejectsInsecureOrPartialConfiguration(t *testing.T) {
	for _, endpoint := range []string{"http://push.example", "https://user:pass@push.example", "not-a-url"} {
		if _, err := NewAPNSAdapter(endpoint, "secret", "topic", nil); err == nil {
			t.Fatalf("accepted endpoint %q", endpoint)
		}
	}
}
