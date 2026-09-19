package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type appHandlers struct {
	apps        *service.AppService
	tokenIssuer *auth.TokenIssuer
}

type appRequest struct {
	Name         string              `json:"name"`
	CallbackURL  *string             `json:"callback_url"`
	DeliveryMode domain.DeliveryMode `json:"delivery_mode"`
}

type socketTokenRequest struct {
	Scopes     []string `json:"scopes"`
	TTLSeconds int64    `json:"ttl_seconds"`
}

type socketTokenResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (handlers appHandlers) create(response http.ResponseWriter, request *http.Request) {
	var input appRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	_, credentials, err := handlers.apps.Create(request.Context(), service.CreateApp{
		Name: input.Name, CallbackURL: input.CallbackURL, DeliveryMode: input.DeliveryMode,
	})
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, credentials)
}

func (handlers appHandlers) list(response http.ResponseWriter, request *http.Request) {
	apps, err := handlers.apps.List(request.Context())
	if err != nil {
		writeServiceError(response, err)
		return
	}
	if apps == nil {
		apps = []domain.App{}
	}
	writeJSON(response, http.StatusOK, apps)
}

func (handlers appHandlers) get(response http.ResponseWriter, request *http.Request) {
	appID := chi.URLParam(request, "appID")
	if authenticatedApp(request).App.ID != appID {
		writeError(response, http.StatusForbidden, "forbidden", "The authenticated application cannot access this resource.")
		return
	}
	app, err := handlers.apps.Get(request.Context(), appID)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, app)
}

func (handlers appHandlers) update(response http.ResponseWriter, request *http.Request) {
	appID := chi.URLParam(request, "appID")
	if authenticatedApp(request).App.ID != appID {
		writeError(response, http.StatusForbidden, "forbidden", "The authenticated application cannot access this resource.")
		return
	}
	input, err := decodeUpdateApp(request)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	app, err := handlers.apps.Update(request.Context(), appID, input)
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, app)
}

func (handlers appHandlers) disable(response http.ResponseWriter, request *http.Request) {
	app, err := handlers.apps.Disable(request.Context(), chi.URLParam(request, "appID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, app)
}

func (handlers appHandlers) rotate(response http.ResponseWriter, request *http.Request) {
	credentials, err := handlers.apps.RotateSecret(request.Context(), chi.URLParam(request, "appID"))
	if err != nil {
		writeServiceError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, credentials)
}

func (handlers appHandlers) socketToken(response http.ResponseWriter, request *http.Request) {
	var input socketTokenRequest
	if err := decodeJSON(request, &input); err != nil || !allowedSocketScopes(input.Scopes) || input.TTLSeconds < 1 || input.TTLSeconds > 900 {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	if handlers.tokenIssuer == nil {
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
		return
	}
	token, err := handlers.tokenIssuer.Issue(authenticatedApp(request).App.ID, input.Scopes, time.Duration(input.TTLSeconds)*time.Second)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
		return
	}
	claims, err := handlers.tokenIssuer.Verify(token, "")
	if err != nil {
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
		return
	}
	writeJSON(response, http.StatusCreated, socketTokenResponse{Token: token, ExpiresAt: claims.ExpiresAt})
}

func decodeUpdateApp(request *http.Request) (service.UpdateApp, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSON(request, &raw); err != nil || len(raw) == 0 {
		return service.UpdateApp{}, service.ErrInvalidInput
	}
	var result service.UpdateApp
	for field, value := range raw {
		switch field {
		case "name":
			var name string
			if err := json.Unmarshal(value, &name); err != nil {
				return service.UpdateApp{}, service.ErrInvalidInput
			}
			result.Name = &name
		case "callback_url":
			result.CallbackURL.Set = true
			if string(value) != "null" {
				var callbackURL string
				if err := json.Unmarshal(value, &callbackURL); err != nil {
					return service.UpdateApp{}, service.ErrInvalidInput
				}
				result.CallbackURL.Value = &callbackURL
			}
		case "delivery_mode":
			var mode domain.DeliveryMode
			if err := json.Unmarshal(value, &mode); err != nil {
				return service.UpdateApp{}, service.ErrInvalidInput
			}
			result.DeliveryMode = &mode
		default:
			return service.UpdateApp{}, service.ErrInvalidInput
		}
	}
	return result, nil
}

func decodeJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON value")
	}
	return nil
}

func allowedSocketScopes(scopes []string) bool {
	allowed := map[string]struct{}{"ws:connect": {}, "ws:subscribe": {}, "ws:read": {}, "stream:connect": {}}
	if len(scopes) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if _, ok := allowed[scope]; !ok {
			return false
		}
		if _, duplicate := seen[scope]; duplicate {
			return false
		}
		seen[scope] = struct{}{}
	}
	return true
}

func writeServiceError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(response, http.StatusBadRequest, "invalid_request", "The request is invalid.")
	case errors.Is(err, service.ErrUnauthorized):
		writeError(response, http.StatusUnauthorized, "unauthorized", "Authentication failed.")
	case errors.Is(err, service.ErrNotFound):
		writeError(response, http.StatusNotFound, "app_not_found", "The application was not found.")
	case errors.Is(err, service.ErrConflict):
		writeError(response, http.StatusConflict, "conflict", "The application conflicts with an existing record.")
	default:
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
	}
}
