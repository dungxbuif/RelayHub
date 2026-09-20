package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/service"
)

func testAdminSessions(t *testing.T) *service.AdminSessionService {
	t.Helper()
	sessions, err := service.NewAdminSessionService(&httpSessionStore{}, &adminUserMemoryStore{password: "admin-bootstrap"}, time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	return sessions
}

func adminSessionHeaders(t *testing.T, router http.Handler) map[string]string {
	t.Helper()
	response := requestJSON(t, router, http.MethodPost, "/api/v1/admin/session", []byte(`{"email":"dungbui.dungbui.00@gmail.com","password":"admin-bootstrap"}`), nil)
	var payload adminSessionResponse
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &payload) != nil || len(response.Result().Cookies()) != 1 {
		t.Fatalf("admin fixture login: status=%d", response.Code)
	}
	cookie := response.Result().Cookies()[0]
	return map[string]string{"Cookie": cookie.Name + "=" + cookie.Value, "X-RelayHub-CSRF": payload.CSRFToken}
}
