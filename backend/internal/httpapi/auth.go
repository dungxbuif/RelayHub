package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/service"
)

type authenticatedAppContextKey struct{}

func adminAuthentication(adminToken string, sessions *service.AdminSessionService) func(http.Handler) http.Handler {
	want := sha256.Sum256([]byte(adminToken))
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			authorization := request.Header.Get("Authorization")
			if authorization != "" {
				token, ok := bearerToken(authorization)
				got := sha256.Sum256([]byte(token))
				if !ok || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
					writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
					return
				}
				next.ServeHTTP(response, request)
				return
			}
			cookie, err := request.Cookie(AdminCookieName)
			if err != nil || sessions == nil {
				writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
				return
			}
			mutate := request.Method != http.MethodGet && request.Method != http.MethodHead && request.Method != http.MethodOptions
			if err := sessions.Authenticate(request.Context(), cookie.Value, request.Header.Get("X-RelayHub-CSRF"), mutate); err != nil {
				writeAdminSessionError(response, err)
				return
			}
			next.ServeHTTP(response, request)
		})
	}
}

func signedAuthentication(apps *service.AppService, now func() time.Time, maxSkew time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if apps == nil {
				writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
				return
			}
			authenticated, err := apps.AuthenticateAPIKey(request.Context(), request.Header.Get("X-RelayHub-Api-Key"))
			if err != nil {
				writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
				return
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					writeError(response, http.StatusRequestEntityTooLarge, "request_too_large", "Request body exceeds the 1 MiB limit.")
					return
				}
				writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			if err := auth.Verify(
				authenticated.HMACSecret,
				request.Header.Get("X-RelayHub-Timestamp"),
				request.Method,
				request.URL.RequestURI(),
				request.Header.Get("X-RelayHub-Signature"),
				body,
				now(),
				maxSkew,
			); err != nil {
				writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
				return
			}
			ctx := context.WithValue(request.Context(), authenticatedAppContextKey{}, authenticated)
			next.ServeHTTP(response, request.WithContext(ctx))
		})
	}
}

func authenticatedApp(request *http.Request) service.AuthenticatedApp {
	authenticated, _ := request.Context().Value(authenticatedAppContextKey{}).(service.AuthenticatedApp)
	return authenticated
}
