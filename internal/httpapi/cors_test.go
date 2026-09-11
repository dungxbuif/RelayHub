package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCORSExactOriginPreflightAndAuthenticatedRequests(t *testing.T) {
	for _, allowed := range [][]string{nil, {"https://ui.example"}} {
		router := NewRouter(Dependencies{AllowedOrigins: allowed, Metrics: http.NotFoundHandler()})
		for _, origin := range []string{"https://ui.example", "https://evil.example", "https://ui.example.evil"} {
			t.Run(strings.Join(allowed, ",")+"/"+origin, func(t *testing.T) {
				r := httptest.NewRequest("GET", "/healthz", nil)
				r.Header.Set("Origin", origin)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				want := ""
				if len(allowed) > 0 && origin == allowed[0] {
					want = origin
				}
				if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") != want {
					t.Fatalf("origin policy: %d %v", w.Code, w.Header())
				}
				r = httptest.NewRequest("OPTIONS", "/api/v1/events", nil)
				r.Header.Set("Origin", origin)
				r.Header.Set("Access-Control-Request-Method", "POST")
				r.Header.Set("Access-Control-Request-Headers", "content-type,x-relayhub-api-key,x-relayhub-timestamp,x-relayhub-signature,idempotency-key")
				w = httptest.NewRecorder()
				router.ServeHTTP(w, r)
				if want != "" {
					if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != want || !strings.Contains(w.Header().Get("Access-Control-Allow-Headers"), "X-RelayHub-Signature") {
						t.Fatalf("preflight: %d %v", w.Code, w.Header())
					}
				} else if w.Header().Get("Access-Control-Allow-Origin") != "" {
					t.Fatal("unlisted origin permitted")
				}
			})
		}
	}
	// CORS headers do not authorize a request: the normal auth middleware runs.
	h, _ := eventRouter(t)
	h = cors([]string{"https://ui.example"})(h)
	r := httptest.NewRequest("POST", "/api/v1/events", strings.NewReader(`{}`))
	r.Header.Set("Origin", "https://ui.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 || w.Header().Get("Access-Control-Allow-Origin") != "https://ui.example" {
		t.Fatalf("CORS bypassed auth: %d", w.Code)
	}
}

func TestCORSRejectsUnapprovedPreflightCapabilities(t *testing.T) {
	h := NewRouter(Dependencies{AllowedOrigins: []string{"https://ui.example"}, Metrics: http.NotFoundHandler()})
	for _, tc := range [][2]string{{"TRACE", "content-type"}, {"POST", "x-unapproved"}} {
		r := httptest.NewRequest("OPTIONS", "/api/v1/events", nil)
		r.Header.Set("Origin", "https://ui.example")
		r.Header.Set("Access-Control-Request-Method", tc[0])
		r.Header.Set("Access-Control-Request-Headers", tc[1])
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 || w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("unapproved preflight accepted: %d %v", w.Code, w.Header())
		}
	}
}
