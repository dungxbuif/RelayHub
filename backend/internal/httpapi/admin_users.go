package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

type adminUserHandlers struct {
	users store.AdminUserStore
	now   func() time.Time
}

type createAdminUserRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
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
		switch {
		case errors.Is(err, store.ErrInvalidTarget):
			writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		case errors.Is(err, store.ErrConflict):
			writeError(response, http.StatusConflict, "conflict", "The request conflicts with an existing resource.")
		default:
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		}
		return
	}
	writeJSON(response, http.StatusCreated, map[string]any{"id": user.ID, "email": user.Email, "role": user.Role, "enabled": true, "created_at": user.CreatedAt, "updated_at": user.UpdatedAt})
}
