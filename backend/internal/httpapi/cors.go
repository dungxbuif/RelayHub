package httpapi

import (
	"net/http"
	"strings"
)

// cors shares the explicit browser origin allowlist with WebSocket upgrades.
// Preflight discovers capabilities; every actual request still authenticates.
func cors(origins []string) func(http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, origin := range origins {
		if origin != "" && origin != "*" {
			allowed[origin] = true
		}
	}
	const methods = "GET, POST, PATCH, DELETE, OPTIONS"
	const headers = "Authorization, Content-Type, Idempotency-Key, X-RelayHub-Api-Key, X-RelayHub-Timestamp, X-RelayHub-Signature"
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(allowed) == 0 {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Add("Vary", "Origin")
			origin := r.Header.Get("Origin")
			preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
			if preflight {
				w.Header().Add("Vary", "Access-Control-Request-Method")
				w.Header().Add("Vary", "Access-Control-Request-Headers")
			}
			if !allowed[origin] || len(r.Header.Values("Origin")) != 1 {
				next.ServeHTTP(w, r)
				return
			}
			if preflight {
				valid := false
				for _, method := range strings.Split(methods, ", ") {
					if r.Header.Get("Access-Control-Request-Method") == method {
						valid = true
					}
				}
				for _, name := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
					name = strings.TrimSpace(name)
					if name == "" {
						continue
					}
					found := false
					for _, candidate := range strings.Split(headers, ", ") {
						if strings.EqualFold(name, candidate) {
							found = true
						}
					}
					valid = valid && found
				}
				if !valid {
					writeError(w, http.StatusForbidden, "forbidden", "CORS preflight is not allowed.")
					return
				}
				w.Header().Set("Access-Control-Allow-Methods", methods)
				w.Header().Set("Access-Control-Allow-Headers", headers)
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Expose-Headers", "X-Request-Id, Idempotent-Replayed")
			if preflight {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
