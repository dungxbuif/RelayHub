package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type realtimeAdminFake struct {
	connections  []redisstate.RealtimeConnection
	appID        string
	connectionID string
	err          error
}

func (fake *realtimeAdminFake) List(_ context.Context, appID string, _ int64) ([]redisstate.RealtimeConnection, error) {
	fake.appID = appID
	return fake.connections, fake.err
}
func (fake *realtimeAdminFake) Disconnect(_ context.Context, appID, connectionID string) error {
	fake.appID, fake.connectionID = appID, connectionID
	return fake.err
}

func TestAdminRealtimeListsBoundedAppConnections(t *testing.T) {
	fake := &realtimeAdminFake{connections: []redisstate.RealtimeConnection{{AppID: "app_a", ClientID: "client_1", ConnectionID: "conn_1", ConnectedAt: time.Now()}}}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/connections?app_id=app_a&limit=50", nil)
	adminRealtimeHandlers{control: fake}.list(response, request)
	if response.Code != http.StatusOK || fake.appID != "app_a" || !containsBody(response.Body.String(), `"connection_id":"conn_1"`) {
		t.Fatalf("status=%d app=%q body=%s", response.Code, fake.appID, response.Body.String())
	}
}

func TestAdminRealtimeDisconnectIsAppScoped(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{{nil, http.StatusNoContent}, {realtime.ErrConnectionNotFound, http.StatusNotFound}, {errors.New("NATS unavailable"), http.StatusServiceUnavailable}} {
		fake := &realtimeAdminFake{err: test.err}
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/connections/conn_1?app_id=app_a", nil)
		routeContext := chi.NewRouteContext()
		routeContext.URLParams.Add("connectionID", "conn_1")
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
		adminRealtimeHandlers{control: fake}.disconnect(response, request)
		if response.Code != test.status || fake.appID != "app_a" || fake.connectionID != "conn_1" {
			t.Fatalf("status=%d target=%s/%s", response.Code, fake.appID, fake.connectionID)
		}
	}
}

func TestAdminRealtimeHistoryIsAppScopedAndBounded(t *testing.T) {
	hub := realtime.NewHub()
	defer hub.Close()
	hub.SetHistoryStore(&websocketHistory{entries: []redisstate.RealtimeHistoryEntry{{Cursor: "MTIzNC0w", Payload: []byte(`{"type":"channel.message","app_id":"app_a","channel":"room","audience":{"type":"all"},"data":{}}`)}}})
	apps := newHTTPMemoryStore()
	apps.apps["app_a"] = domain.App{ID: "app_a", Enabled: true}
	handlers := adminRealtimeResourceHandlers{apps: service.NewAppService(apps, service.AppOptions{}), history: hub}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/apps/app_a/realtime/channels/room/history?limit=1", nil)
	request = withRealtimeRouteParams(request, map[string]string{"appID": "app_a", "channel": "room"})
	response := httptest.NewRecorder()
	handlers.historyList(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"cursor":"MTIzNC0w"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	tooLarge := httptest.NewRequest(http.MethodGet, "/api/v1/admin/apps/app_a/realtime/channels/room/history?limit=101", nil)
	tooLarge = withRealtimeRouteParams(tooLarge, map[string]string{"appID": "app_a", "channel": "room"})
	response = httptest.NewRecorder()
	handlers.historyList(response, tooLarge)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("large history status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdminRealtimeResourceRoutesFailClosedWithoutDependencies(t *testing.T) {
	handlers := adminRealtimeResourceHandlers{apps: service.NewAppService(nil, service.AppOptions{})}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/apps/app_a/realtime/files/file_1", nil)
	request = withRealtimeRouteParams(request, map[string]string{"appID": "app_a", "fileID": "file_1"})
	response := httptest.NewRecorder()
	handlers.fileMetadata(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("file status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "presign") || strings.Contains(response.Body.String(), "download_url") {
		t.Fatalf("file error leaked URL details: %s", response.Body.String())
	}
}

func withRealtimeRouteParams(request *http.Request, values map[string]string) *http.Request {
	routeContext := chi.NewRouteContext()
	for key, value := range values {
		routeContext.URLParams.Add(key, value)
	}
	return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
}

func containsBody(body, part string) bool {
	return len(body) >= len(part) && stringContains(body, part)
}
func stringContains(body, part string) bool {
	for i := 0; i+len(part) <= len(body); i++ {
		if body[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
