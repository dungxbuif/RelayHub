package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminFullControlHandlers struct {
	apps      *service.AppService
	functions *service.FunctionService
}

func (h adminFullControlHandlers) app(response http.ResponseWriter, request *http.Request) {
	if h.apps == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return
	}
	app, err := h.apps.Get(request.Context(), chi.URLParam(request, "appID"))
	if err != nil {
		writeAdminFullControlError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, app)
}

func (h adminFullControlHandlers) listFunctions(response http.ResponseWriter, request *http.Request) {
	app, ok := h.activeApp(response, request)
	if !ok || h.functions == nil {
		if ok {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		}
		return
	}
	functions, err := h.functions.List(request.Context(), app.ID)
	if err != nil {
		writeFunctionError(response, err)
		return
	}
	if functions == nil {
		functions = []domain.Function{}
	}
	writeJSON(response, http.StatusOK, functions)
}

func (h adminFullControlHandlers) registerFunction(response http.ResponseWriter, request *http.Request) {
	app, ok := h.activeApp(response, request)
	if !ok || h.functions == nil {
		if ok {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		}
		return
	}
	var input service.RegisterFunction
	if err := decodeJSON(request, &input); err != nil {
		writeFunctionError(response, service.ErrInvalidInput)
		return
	}
	function, err := h.functions.Register(request.Context(), app.ID, input)
	if err != nil {
		writeFunctionError(response, err)
		return
	}
	logOperation(request, operationFields{AppID: app.ID, FunctionID: function.ID, Operation: "admin.function.registered", Outcome: "success"})
	writeJSON(response, http.StatusCreated, function)
}

func (h adminFullControlHandlers) updateFunction(response http.ResponseWriter, request *http.Request) {
	app, ok := h.activeApp(response, request)
	if !ok || h.functions == nil {
		if ok {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		}
		return
	}
	input, err := decodeAdminFunctionUpdate(request)
	if err != nil {
		writeFunctionError(response, service.ErrInvalidInput)
		return
	}
	function, err := h.functions.Update(request.Context(), app.ID, chi.URLParam(request, "functionID"), input)
	if err != nil {
		writeFunctionError(response, err)
		return
	}
	logOperation(request, operationFields{AppID: app.ID, FunctionID: function.ID, Operation: "admin.function.updated", Outcome: "success"})
	writeJSON(response, http.StatusOK, function)
}

func (h adminFullControlHandlers) deleteFunction(response http.ResponseWriter, request *http.Request) {
	app, ok := h.activeApp(response, request)
	if !ok || h.functions == nil {
		if ok {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		}
		return
	}
	if err := h.functions.Delete(request.Context(), app.ID, chi.URLParam(request, "functionID")); err != nil {
		writeFunctionError(response, err)
		return
	}
	logOperation(request, operationFields{AppID: app.ID, FunctionID: chi.URLParam(request, "functionID"), Operation: "admin.function.deleted", Outcome: "success"})
	response.WriteHeader(http.StatusNoContent)
}

func (h adminFullControlHandlers) invokeFunction(response http.ResponseWriter, request *http.Request) {
	app, ok := h.activeApp(response, request)
	if !ok || h.functions == nil {
		if ok {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		}
		return
	}
	var input struct {
		Input json.RawMessage `json:"input"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeFunctionError(response, service.ErrInvalidInput)
		return
	}
	result, replay, err := h.functions.Invoke(request.Context(), app.ID, chi.URLParam(request, "functionID"), request.Header.Get("Idempotency-Key"), input.Input)
	if replay {
		response.Header().Set("Idempotent-Replayed", "true")
	}
	if err != nil {
		writeFunctionError(response, err)
		return
	}
	outcome := "success"
	if !result.OK {
		outcome = "handler_error"
	}
	if replay {
		outcome = "replayed"
	}
	logOperation(request, operationFields{AppID: app.ID, InvocationID: result.InvocationID, Operation: "admin.function.invoked", Outcome: outcome})
	writeJSON(response, http.StatusOK, result)
}

func (h adminFullControlHandlers) activeApp(response http.ResponseWriter, request *http.Request) (domain.App, bool) {
	if h.apps == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return domain.App{}, false
	}
	app, err := h.apps.Get(request.Context(), chi.URLParam(request, "appID"))
	if err != nil || !app.Enabled {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
		return domain.App{}, false
	}
	return app, true
}

func decodeAdminFunctionUpdate(request *http.Request) (service.UpdateFunction, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSON(request, &raw); err != nil || len(raw) == 0 {
		return service.UpdateFunction{}, service.ErrInvalidInput
	}
	var input service.UpdateFunction
	for field, value := range raw {
		switch field {
		case "name":
			var name string
			if err := json.Unmarshal(value, &name); err != nil {
				return service.UpdateFunction{}, service.ErrInvalidInput
			}
			input.Name = &name
		case "timeout_seconds":
			var timeout int
			if err := json.Unmarshal(value, &timeout); err != nil {
				return service.UpdateFunction{}, service.ErrInvalidInput
			}
			input.TimeoutSeconds = &timeout
		case "enabled":
			var enabled bool
			if err := json.Unmarshal(value, &enabled); err != nil {
				return service.UpdateFunction{}, service.ErrInvalidInput
			}
			input.Enabled = &enabled
		default:
			return service.UpdateFunction{}, service.ErrInvalidInput
		}
	}
	return input, nil
}

func writeAdminFullControlError(response http.ResponseWriter, err error) {
	if errors.Is(err, service.ErrNotFound) {
		writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
		return
	}
	writeServiceError(response, err)
}
