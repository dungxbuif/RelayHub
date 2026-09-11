package httpapi

import (
	"encoding/json"
	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/web"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Catches an undocumented route, a phantom operation, or accidental removal of
// authentication on a registered API boundary. It does not parse Go source.
func TestRouteManifestMatchesContractAndAuthentication(t *testing.T) {
	hub := realtime.NewHub()
	defer hub.Close()
	router := NewRouter(Dependencies{Apps: service.NewAppService(nil, service.AppOptions{}), Events: service.NewEventService(nil, nil, service.EventOptions{}), Functions: service.NewFunctionService(nil, service.FunctionOptions{}), Realtime: hub, Stream: &recordingStream{}, TokenIssuer: auth.NewTokenIssuer([]byte("contract-test-secret"), nil), Docs: web.Public, Metrics: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })})
	raw, err := os.ReadFile("../../public-docs/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string                `json:"operationId"`
			Security    []map[string][]string `json:"security"`
		} `json:"paths"`
	}
	if err = json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("RELAYHUB_ROUTE_MANIFEST_OUTPUT"); output != "" {
		data, err := json.Marshal(RouteManifest(router))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for _, route := range RouteManifest(router) {
		path := route.Path
		if path == "/docs/*" {
			path = "/docs/{resource}"
		}
		operation, ok := spec.Paths[path][strings.ToLower(route.Method)]
		if !ok {
			t.Errorf("undocumented %s %s", route.Method, path)
			continue
		}
		seen[strings.ToLower(route.Method)+" "+path] = true
		categories := map[string][]map[string][]string{"public": {}, "admin": {{"AdminBearer": {}}}, "app": {{"AppApiKey": {}, "AppSignature": {}}}, "ws_token": {{"SocketToken": {}}}}
		expected, known := categories[route.Auth]
		if !known || !reflect.DeepEqual(operation.Security, expected) {
			t.Errorf("auth category %s for %s %s: got %v want %v", route.Auth, route.Method, path, operation.Security, expected)
		}
		if route.Path == "/ws" || strings.HasPrefix(route.Path, "/api/") {
			if len(operation.Security) == 0 {
				t.Errorf("missing security %s", path)
			}
			target := strings.NewReplacer("{appID}", "missing", "{eventID}", "missing", "{jobID}", "missing", "{functionID}", "missing").Replace(route.Path)
			response := performRequest(t, router, route.Method, target)
			if response.Code != 401 {
				t.Errorf("%s %s missing auth boundary: %d", route.Method, path, response.Code)
			}
		}
	}
	for path, item := range spec.Paths {
		for method, op := range item {
			if op.OperationID != "" && !seen[method+" "+path] {
				t.Errorf("phantom OpenAPI operation %s %s", method, path)
			}
		}
	}
}
