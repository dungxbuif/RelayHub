package httpapi

import (
	"github.com/go-chi/chi/v5"
	"net/http"
	"sort"
)

// Route describes one concrete router registration. Static Admin routes are
// intentionally outside the versioned API contract.
type Route struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Auth   string `json:"auth"`
}

// RouteManifest derives the contract inventory from the actual registered
// router. It requires no source parsing and adds no public discovery endpoint.
func RouteManifest(handler http.Handler) []Route {
	router, ok := handler.(chi.Routes)
	if !ok {
		return nil
	}
	routes := []Route{}
	_ = chi.Walk(router, func(method, path string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		routes = append(routes, Route{Method: method, Path: path, Auth: routeAuth[method+" "+path]})
		return nil
	})
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Path == routes[j].Path {
			return routes[i].Method < routes[j].Method
		}
		return routes[i].Path < routes[j].Path
	})
	return routes
}

// Explicit security metadata is intentionally independent of OpenAPI. Walking
// registrations catches missing routes; HTTP probes catch wrong middleware.
var routeAuth = map[string]string{
	"GET /healthz": "public", "GET /readyz": "public", "GET /metrics": "public",
	"GET /admin": "public", "GET /admin/*": "public", "GET /ws": "ws_token",
	"GET /api/v1/stream":         "ws_token",
	"POST /api/v1/admin/session": "admin_bootstrap", "DELETE /api/v1/admin/session": "admin_session",
	"GET /api/v1/admin/dashboard": "admin", "GET /api/v1/admin/metrics": "admin",
	"GET /api/v1/admin/events": "admin", "GET /api/v1/admin/events/{eventID}": "admin",
	"GET /api/v1/admin/events/{eventID}/timeline": "admin",
	"GET /api/v1/admin/dlq":                       "admin", "GET /api/v1/admin/dlq/{deliveryID}": "admin",
	"POST /api/v1/admin/dlq/replay": "admin", "POST /api/v1/admin/dlq/{deliveryID}/replay": "admin",
	"GET /api/v1/admin/audit": "admin",
	"POST /api/v1/apps":       "admin", "GET /api/v1/apps": "admin",
	"GET /api/v1/apps/{appID}": "app", "PATCH /api/v1/apps/{appID}": "app",
	"DELETE /api/v1/apps/{appID}": "admin", "POST /api/v1/apps/{appID}/rotate-secret": "admin",
	"POST /api/v1/socket/token": "app", "POST /api/v1/events": "app",
	"GET /api/v1/events/{eventID}": "app", "GET /api/v1/jobs/{jobID}": "app",
	"POST /api/v1/functions": "app", "GET /api/v1/functions": "app",
	"DELETE /api/v1/functions/{functionID}": "app", "POST /api/v1/functions/{functionID}/invoke": "app",
	"POST /api/v1/routing/rules": "admin", "GET /api/v1/routing/rules": "admin",
	"PATCH /api/v1/routing/rules/{ruleID}": "admin", "DELETE /api/v1/routing/rules/{ruleID}": "admin",
	"POST /api/v1/realtime/channels/{channel}/publish": "app",
}
