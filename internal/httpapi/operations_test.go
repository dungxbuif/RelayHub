package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type healthCheckerFunc func(context.Context) error

func (fn healthCheckerFunc) Ping(ctx context.Context) error {
	return fn(ctx)
}

func TestHealthDoesNotDependOnRedis(t *testing.T) {
	router := newTestRouter(healthCheckerFunc(func(context.Context) error {
		panic("healthz must not ping Redis")
	}))

	response := performRequest(t, router, http.MethodGet, "/healthz")

	if response.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
	assertJSONResponse(t, response, `{"status":"ok"}`)
}

func TestReadyReflectsRedisReachability(t *testing.T) {
	tests := []struct {
		name       string
		pingError  error
		wantStatus int
		wantBody   string
	}{
		{
			name:       "reachable",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "unreachable",
			pingError:  errors.New("connection refused with internal details"),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"not_ready","message":"Redis is unavailable."}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newTestRouter(healthCheckerFunc(func(context.Context) error {
				return tt.pingError
			}))

			response := performRequest(t, router, http.MethodGet, "/readyz")

			if response.Code != tt.wantStatus {
				t.Fatalf("GET /readyz status = %d, want %d; body = %s", response.Code, tt.wantStatus, response.Body.String())
			}
			assertJSONResponse(t, response, tt.wantBody)
			if strings.Contains(response.Body.String(), "internal details") {
				t.Fatalf("GET /readyz exposed internal error: %s", response.Body.String())
			}
		})
	}
}

func TestMetricsServesPrometheusText(t *testing.T) {
	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayhub_test_ready", Help: "Test metric."})
	gauge.Set(1)
	registry.MustRegister(gauge)
	router := newTestRouterWithMetrics(
		healthCheckerFunc(func(context.Context) error { return nil }),
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	)

	response := performRequest(t, router, http.MethodGet, "/metrics")

	if response.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", response.Code, http.StatusOK)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Fatalf("GET /metrics Content-Type = %q, want text/plain", contentType)
	}
	if body := response.Body.String(); !strings.Contains(body, "relayhub_test_ready 1") {
		t.Fatalf("GET /metrics body = %q, want registered metric", body)
	}
}

func TestDocsServesStaticFilesWithCorrectContentTypes(t *testing.T) {
	router := newTestRouter(healthCheckerFunc(func(context.Context) error { return nil }))

	tests := []struct {
		name         string
		path         string
		wantStatus   int
		wantType     string
		wantBody     string
		wantLocation string
	}{
		{
			name:         "redirect",
			path:         "/docs",
			wantStatus:   http.StatusPermanentRedirect,
			wantLocation: "/docs/",
		},
		{
			name:       "index",
			path:       "/docs/",
			wantStatus: http.StatusOK,
			wantType:   "text/html",
			wantBody:   "<h1>RelayHub docs</h1>",
		},
		{
			name:       "llms",
			path:       "/docs/llms.txt",
			wantStatus: http.StatusOK,
			wantType:   "text/plain",
			wantBody:   "# RelayHub agent index",
		},
		{
			name:       "Markdown descendant",
			path:       "/docs/developer/guide.md",
			wantStatus: http.StatusOK,
			wantType:   "text/markdown",
			wantBody:   "# Developer guide",
		},
		{
			name:       "CSS descendant",
			path:       "/docs/assets/app.css",
			wantStatus: http.StatusOK,
			wantType:   "text/css",
			wantBody:   "body { color: navy; }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := performRequest(t, router, http.MethodGet, tt.path)
			if response.Code != tt.wantStatus {
				t.Fatalf("GET %s status = %d, want %d; body = %s", tt.path, response.Code, tt.wantStatus, response.Body.String())
			}
			if tt.wantLocation != "" && response.Header().Get("Location") != tt.wantLocation {
				t.Fatalf("GET %s Location = %q, want %q", tt.path, response.Header().Get("Location"), tt.wantLocation)
			}
			if tt.wantType != "" && !strings.HasPrefix(response.Header().Get("Content-Type"), tt.wantType) {
				t.Fatalf("GET %s Content-Type = %q, want prefix %q", tt.path, response.Header().Get("Content-Type"), tt.wantType)
			}
			if tt.wantBody != "" && !strings.Contains(response.Body.String(), tt.wantBody) {
				t.Fatalf("GET %s body = %q, want content %q", tt.path, response.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestNotFoundUsesStandardJSONError(t *testing.T) {
	router := newTestRouter(healthCheckerFunc(func(context.Context) error { return nil }))

	response := performRequest(t, router, http.MethodGet, "/missing")

	if response.Code != http.StatusNotFound {
		t.Fatalf("GET /missing status = %d, want %d", response.Code, http.StatusNotFound)
	}
	assertJSONResponse(t, response, `{"error":{"code":"not_found","message":"The requested resource was not found."}}`)
}

func TestMethodNotAllowedUsesStandardJSONError(t *testing.T) {
	router := newTestRouter(healthCheckerFunc(func(context.Context) error { return nil }))

	response := performRequest(t, router, http.MethodPost, "/healthz")

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	assertJSONResponse(t, response, `{"error":{"code":"method_not_allowed","message":"The requested method is not allowed."}}`)
}

func TestRequestBodyLimitRejectsOversizedRequests(t *testing.T) {
	router := newTestRouter(healthCheckerFunc(func(context.Context) error { return nil }))
	request := httptest.NewRequest(http.MethodPost, "/missing", strings.NewReader(strings.Repeat("x", 1<<20+1)))
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	assertJSONResponse(t, response, `{"error":{"code":"request_too_large","message":"Request body exceeds the 1 MiB limit."}}`)
}

func newTestRouter(health healthCheckerFunc) http.Handler {
	return newTestRouterWithMetrics(health, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(response, "relayhub_test 1\n")
	}))
}

func newTestRouterWithMetrics(health healthCheckerFunc, metrics http.Handler) http.Handler {
	docs := fstest.MapFS{
		"index.html":         {Data: []byte("<h1>RelayHub docs</h1>")},
		"llms.txt":           {Data: []byte("# RelayHub agent index")},
		"developer/guide.md": {Data: []byte("# Developer guide")},
		"assets/app.css":     {Data: []byte("body { color: navy; }")},
	}
	return NewRouter(Dependencies{Health: health, Docs: docs, Metrics: metrics})
}

func performRequest(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertJSONResponse(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	if got := strings.TrimSpace(response.Body.String()); got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}
