package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type adminUserHandlers struct {
	users    store.AdminUserStore
	sessions *service.AdminSessionService
	now      func() time.Time
}

type createAdminUserRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

type updateAdminUserRequest struct {
	Role    *string `json:"role"`
	Enabled *bool   `json:"enabled"`
}

type rotateAdminUserPasswordRequest struct {
	Password string `json:"password"`
}

func (h adminUserHandlers) list(response http.ResponseWriter, request *http.Request) {
	lifecycle, ok := h.users.(store.AdminUserLifecycleStore)
	if !ok {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	users, err := lifecycle.ListAdminUsers(request.Context())
	if err != nil {
		writeAdminUserStoreError(response, err)
		return
	}
	result := make([]adminUserResponse, 0, len(users))
	for _, user := range users {
		result = append(result, newAdminUserResponse(user))
	}
	writeJSON(response, http.StatusOK, result)
}

func (h adminUserHandlers) create(response http.ResponseWriter, request *http.Request) {
	if h.users == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	var body createAdminUserRequest
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Email == "" || body.Password == "" {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	if body.Role == "" {
		body.Role = "admin"
	}
	now := time.Now
	if h.now != nil {
		now = h.now
	}
	timestamp := now().UTC()
	user := store.AdminUser{ID: "adm_" + uuid.NewString(), Email: body.Email, Role: body.Role, Enabled: true, CreatedAt: timestamp, UpdatedAt: timestamp}
	if err := h.users.CreateAdminUser(request.Context(), user, body.Password); err != nil {
		writeAdminUserStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, newAdminUserResponse(user))
}

func (h adminUserHandlers) update(response http.ResponseWriter, request *http.Request) {
	lifecycle, ok := h.users.(store.AdminUserLifecycleStore)
	if !ok {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	var body updateAdminUserRequest
	if err := decodeJSON(request, &body); err != nil || (body.Role == nil && body.Enabled == nil) {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	user, err := lifecycle.GetAdminUser(request.Context(), chi.URLParam(request, "userID"))
	if err != nil {
		writeAdminUserStoreError(response, err)
		return
	}
	if body.Role != nil {
		user.Role = *body.Role
	}
	if body.Enabled != nil {
		if !*body.Enabled && authenticatedAdminPrincipal(request).ID == user.ID {
			writeError(response, http.StatusConflict, "conflict", "The request conflicts with an existing resource.")
			return
		}
		user.Enabled = *body.Enabled
	}
	user.UpdatedAt = h.timestamp()
	updated, err := lifecycle.UpdateAdminUser(request.Context(), user)
	if err != nil {
		writeAdminUserStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, newAdminUserResponse(updated))
}

func (h adminUserHandlers) disable(response http.ResponseWriter, request *http.Request) {
	lifecycle, ok := h.users.(store.AdminUserLifecycleStore)
	if !ok {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	userID := chi.URLParam(request, "userID")
	if authenticatedAdminPrincipal(request).ID == userID {
		writeError(response, http.StatusConflict, "conflict", "The request conflicts with an existing resource.")
		return
	}
	user, err := lifecycle.DisableAdminUser(request.Context(), userID, h.timestamp())
	if err != nil {
		writeAdminUserStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, newAdminUserResponse(user))
}

func (h adminUserHandlers) rotatePassword(response http.ResponseWriter, request *http.Request) {
	lifecycle, ok := h.users.(store.AdminUserLifecycleStore)
	if !ok || h.sessions == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	var body rotateAdminUserPasswordRequest
	if err := decodeJSON(request, &body); err != nil || body.Password == "" {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	userID := chi.URLParam(request, "userID")
	user, err := lifecycle.RotateAdminUserPassword(request.Context(), userID, body.Password, h.timestamp())
	if err != nil {
		writeAdminUserStoreError(response, err)
		return
	}
	if err := h.sessions.RevokeAdminUserSessions(request.Context(), userID); err != nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	writeJSON(response, http.StatusOK, newAdminUserResponse(user))
}

type adminUserResponse struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func newAdminUserResponse(user store.AdminUser) adminUserResponse {
	return adminUserResponse{ID: user.ID, Email: user.Email, Role: user.Role, Enabled: user.Enabled, CreatedAt: user.CreatedAt, UpdatedAt: user.UpdatedAt}
}

func (h adminUserHandlers) timestamp() time.Time {
	if h.now != nil {
		return h.now().UTC()
	}
	return time.Now().UTC()
}

func writeAdminUserStoreError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidTarget):
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
	case errors.Is(err, store.ErrConflict):
		writeError(response, http.StatusConflict, "conflict", "The request conflicts with an existing resource.")
	case errors.Is(err, store.ErrNotFound):
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
	default:
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
	}
}
