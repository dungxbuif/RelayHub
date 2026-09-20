package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/service"
)

const AdminCookieName = "__Host-relayhub_admin"

type adminSessionHandlers struct {
	sessions *service.AdminSessionService
}

type adminSessionResponse struct {
	CSRFToken string    `json:"csrf_token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (h adminSessionHandlers) exchange(response http.ResponseWriter, request *http.Request) {
	token, ok := bearerToken(request.Header.Get("Authorization"))
	if !ok || h.sessions == nil {
		writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
		return
	}
	sessionID, csrfToken, expiresAt, err := h.sessions.Exchange(request.Context(), token)
	if err != nil {
		writeAdminSessionError(response, err)
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	http.SetCookie(response, adminCookie(sessionID, expiresAt))
	writeJSON(response, http.StatusOK, adminSessionResponse{CSRFToken: csrfToken, ExpiresAt: expiresAt})
}

func (h adminSessionHandlers) logout(response http.ResponseWriter, request *http.Request) {
	cookie, err := request.Cookie(AdminCookieName)
	if err != nil || h.sessions == nil {
		writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
		return
	}
	if err := h.sessions.Logout(request.Context(), cookie.Value); err != nil {
		writeAdminSessionError(response, err)
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	http.SetCookie(response, adminCookie("", time.Unix(1, 0).UTC()))
	response.WriteHeader(http.StatusNoContent)
}

func adminCookie(value string, expiresAt time.Time) *http.Cookie {
	maxAge := int(time.Until(expiresAt).Seconds())
	if value == "" {
		maxAge = -1
	}
	return &http.Cookie{
		Name: AdminCookieName, Value: value, Path: "/", Expires: expiresAt,
		MaxAge: maxAge, HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
	}
}

func bearerToken(value string) (string, bool) {
	if strings.Count(value, " ") != 1 {
		return "", false
	}
	scheme, token, ok := strings.Cut(value, " ")
	return token, ok && scheme == "Bearer" && token != ""
}

func writeAdminSessionError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrUnauthorized), errors.Is(err, redisstate.ErrNotFound):
		writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
	case errors.Is(err, service.ErrForbidden):
		writeError(response, http.StatusForbidden, "forbidden", "The request is not allowed.")
	default:
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
	}
}
