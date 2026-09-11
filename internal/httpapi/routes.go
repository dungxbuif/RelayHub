package httpapi

import (
	"github.com/go-chi/chi/v5"
	"net/http"
	"sort"
)

// Route describes one concrete router registration. Wildcard docs resources use
// /docs/* here and /docs/{resource} in OpenAPI, with nested paths documented there.
type Route struct {
	Method string `json:"method"`
	Path   string `json:"path"`
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
		routes = append(routes, Route{method, path})
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
