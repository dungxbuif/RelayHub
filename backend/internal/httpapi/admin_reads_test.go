package httpapi

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/service"
)

type httpAdminReadStore struct{ eventQuery adminread.EventListQuery }

func (store *httpAdminReadStore) ListAdminEvents(_ context.Context, query adminread.EventListQuery) (adminread.Page[adminread.EventSummary], error) {
	store.eventQuery = query
	return adminread.Page[adminread.EventSummary]{Items: []adminread.EventSummary{{ID: "evt_1", Type: "invoice.created", CreatedAt: time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)}}}, nil
}
func (*httpAdminReadStore) GetAdminEvent(context.Context, string) (adminread.EventDetail, error) {
	return adminread.EventDetail{EventSummary: adminread.EventSummary{ID: "evt_1"}, Data: json.RawMessage(`{"safe":true}`)}, nil
}
func (*httpAdminReadStore) ListAdminDeadLetters(context.Context, adminread.DeadLetterListQuery) (adminread.Page[adminread.DeadLetterSummary], error) {
	return adminread.Page[adminread.DeadLetterSummary]{Items: []adminread.DeadLetterSummary{}}, nil
}
func (*httpAdminReadStore) GetAdminDeadLetter(context.Context, string) (adminread.DeadLetterDetail, error) {
	return adminread.DeadLetterDetail{DeadLetterSummary: adminread.DeadLetterSummary{DeliveryID: "dlv_1"}}, nil
}
func (*httpAdminReadStore) ListAdminAudit(context.Context, adminread.AuditListQuery) (adminread.Page[adminread.AuditSummary], error) {
	return adminread.Page[adminread.AuditSummary]{Items: []adminread.AuditSummary{}}, nil
}
func (*httpAdminReadStore) AdminDurableCounts(context.Context) (adminread.DurableCounts, error) {
	return adminread.DurableCounts{Pending: 2, Retrying: 1, DeadLetter: 3}, nil
}

func TestAdminReadRoutesRequireAdminAndReturnBoundedData(t *testing.T) {
	router, store := adminReadTestRouter(t)
	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/admin/dashboard", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	for _, path := range []string{"/api/v1/admin/dashboard", "/api/v1/admin/metrics", "/api/v1/admin/events?limit=10&type=invoice.created", "/api/v1/admin/events/evt_1", "/api/v1/admin/dlq", "/api/v1/admin/dlq/dlv_1", "/api/v1/admin/audit"} {
		response := adminReadRequest(router, path)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", path, response.Code, response.Body.String())
		}
		if strings.Contains(strings.ToLower(response.Body.String()), "authorization") || strings.Contains(response.Body.String(), "admin-bootstrap") {
			t.Fatalf("GET %s leaked credential: %s", path, response.Body.String())
		}
	}
	if store.eventQuery.Options.Limit != 10 || store.eventQuery.Filters.Type != "invoice.created" {
		t.Fatalf("event query = %#v", store.eventQuery)
	}
}

func TestAdminReadRoutesRejectUnknownDuplicateInvalidAndReboundQueries(t *testing.T) {
	router, _ := adminReadTestRouter(t)
	fingerprint := (adminread.EventFilters{Type: "invoice.created"}).Fingerprint()
	cursor, err := adminread.EncodeCursor(adminread.Cursor{Timestamp: time.Now(), ID: "evt_1", FilterFingerprint: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{
		"/api/v1/admin/events?limit=101",
		"/api/v1/admin/events?limit=1&limit=2",
		"/api/v1/admin/events?unknown=value",
		"/api/v1/admin/events?from=not-a-time",
		"/api/v1/admin/events?type=other&cursor=" + cursor,
		"/api/v1/admin/dashboard?window=24h&step=1m",
	}
	for _, path := range paths {
		response := adminReadRequest(router, path)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_request"`) {
			t.Fatalf("GET %s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func adminReadTestRouter(t *testing.T) (http.Handler, *httpAdminReadStore) {
	t.Helper()
	store := &httpAdminReadStore{}
	reads, err := service.NewAdminReadService(store, nil, service.AdminReadOptions{Now: func() time.Time { return time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	adminFS := fs.FS(fstest.MapFS{"index.html": {Data: []byte("admin")}})
	return NewRouter(Dependencies{AdminToken: "admin-bootstrap", AdminReads: reads, Admin: adminFS, Metrics: http.NotFoundHandler()}), store
}

func adminReadRequest(router http.Handler, path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set("Authorization", "Bearer admin-bootstrap")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}
