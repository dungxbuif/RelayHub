package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestBootstrapRoutes(t *testing.T) {
	cases := []struct {
		method, path string
		status       int
		code         string
	}{
		{"GET", "/healthz", 200, ""},
		{"POST", "/healthz", 405, "METHOD_NOT_ALLOWED"},
		{"GET", "/readyz", 503, "NOT_READY"},
		{"POST", "/api/v1/jobs", 501, "NOT_IMPLEMENTED"},
		{"GET", "/api/v1/jobs", 501, "NOT_IMPLEMENTED"},
		{"POST", "/api/v1/realtime/sessions", 501, "NOT_IMPLEMENTED"},
		{"GET", "/api/v1/missing", 404, "NOT_FOUND"},
		{"GET", "/api/v1/admin/projects", 404, "NOT_FOUND"},
		{"GET", "/hooks/test", 404, "NOT_FOUND"},
		{"GET", "/connection/websocket", 501, "NOT_IMPLEMENTED"},
		{"GET", "/", 404, "NOT_FOUND"},
	}
	h := NewHandler()
	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("X-Request-ID", "untrusted-client-id")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tc.status {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
			if rr.Header().Get("Content-Type") != "application/json" {
				t.Fatal("not JSON")
			}
			id := rr.Header().Get("X-Request-ID")
			if id == "" || id == "untrusted-client-id" || seen[id] {
				t.Fatalf("bad id %q", id)
			}
			seen[id] = true
			var body struct {
				RequestID string `json:"requestId"`
				Error     struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if tc.code != "" && (body.Error.Code != tc.code || body.RequestID != id) {
				t.Fatalf("bad error %+v", body)
			}
		})
	}
}
