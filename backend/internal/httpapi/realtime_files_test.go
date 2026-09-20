package httpapi

import (
	"net/http/httptest"
	"testing"
)

func TestRealtimeFilesFailClosedWhenObjectStorageIsDisabled(t *testing.T) {
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/api/v2/realtime/files", nil)
	realtimeFileHandlers{}.create(response, request)
	if response.Code != 503 || response.Body.String() != `{"error":{"code":"file_messaging_disabled","message":"File messaging is not configured."}}`+"\n" {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
