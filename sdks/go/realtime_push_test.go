package relayhub

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRealtimePushHelpersUseSignedAppScopedRoutes(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		if request.Header.Get("X-RelayHub-Signature") != sign("secret", request.Header.Get("X-RelayHub-Timestamp"), request.Method, request.URL.RequestURI(), body) {
			t.Fatal("invalid request signature")
		}
		calls++
		switch request.Method + " " + request.URL.RequestURI() {
		case "POST /api/v2/realtime/push/devices":
			response.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(response, `{"id":"device_1","app_id":"app_1","provider":"fcm"}`)
		case "PUT /api/v2/realtime/push/channels/private%3Aroom/devices/device_1", "DELETE /api/v2/realtime/push/channels/private%3Aroom/devices/device_1", "DELETE /api/v2/realtime/push/devices/device_1":
			response.WriteHeader(http.StatusNoContent)
		case "POST /api/v2/realtime/push/channels/private%3Aroom/notifications":
			response.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(response, `{"outcomes":[{"id":"push_1","device_id":"device_1","channel":"private:room","provider":"fcm","status":"delivered"}]}`)
		default:
			t.Fatalf("unexpected route %s %s", request.Method, request.URL.RequestURI())
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, APIKey: "key", HMACSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	device, err := client.RegisterPushDevice(context.Background(), "fcm", "provider-device-token")
	if err != nil || device.ID != "device_1" {
		t.Fatalf("device=%#v error=%v", device, err)
	}
	if err := client.BindPushDevice(context.Background(), "private:room", device.ID, true); err != nil {
		t.Fatal(err)
	}
	outcomes, err := client.PublishPush(context.Background(), "private:room", PushNotification{Title: "Ready"})
	if err != nil || len(outcomes) != 1 || outcomes[0].Status != "delivered" {
		t.Fatalf("outcomes=%#v error=%v", outcomes, err)
	}
	if err := client.BindPushDevice(context.Background(), "private:room", device.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := client.DeletePushDevice(context.Background(), device.ID); err != nil || calls != 5 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

func TestVerifyCallbackSignatureChecksExactBytesTargetAndSkew(t *testing.T) {
	body := []byte(`{"event":{"id":"evt_1"}}`)
	timestamp := "1770000000"
	signature := sign("secret", timestamp, "POST", "/callbacks/realtime", body)
	now := time.Unix(1770000000, 0)
	if !VerifyCallbackSignature("secret", timestamp, "/callbacks/realtime", body, signature, now, 5*time.Minute) {
		t.Fatal("valid callback signature rejected")
	}
	if VerifyCallbackSignature("secret", timestamp, "/callbacks/realtime", []byte(`{}`), signature, now, 5*time.Minute) || VerifyCallbackSignature("secret", timestamp, "/wrong", body, signature, now, 5*time.Minute) || VerifyCallbackSignature("secret", timestamp, "/callbacks/realtime", body, signature, now.Add(10*time.Minute), 5*time.Minute) {
		t.Fatal("invalid callback signature accepted")
	}
}
