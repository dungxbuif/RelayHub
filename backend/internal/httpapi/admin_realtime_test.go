package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
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
