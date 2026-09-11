package httpapi

import (
	"encoding/json"
	"errors"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
	"net/http"
)

type functionHandlers struct{ functions *service.FunctionService }

func (h functionHandlers) register(w http.ResponseWriter, r *http.Request) {
	var in service.RegisterFunction
	if decodeJSON(r, &in) != nil {
		writeFunctionError(w, service.ErrInvalidInput)
		return
	}
	f, e := h.functions.Register(r.Context(), authenticatedApp(r).App.ID, in)
	if e != nil {
		writeFunctionError(w, e)
		return
	}
	writeJSON(w, http.StatusCreated, f)
}
func (h functionHandlers) list(w http.ResponseWriter, r *http.Request) {
	items, e := h.functions.List(r.Context(), authenticatedApp(r).App.ID)
	if e != nil {
		writeFunctionError(w, e)
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (h functionHandlers) delete(w http.ResponseWriter, r *http.Request) {
	if e := h.functions.Delete(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "functionID")); e != nil {
		writeFunctionError(w, e)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h functionHandlers) invoke(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Input json.RawMessage `json:"input"`
	}
	if decodeJSON(r, &in) != nil {
		writeFunctionError(w, service.ErrInvalidInput)
		return
	}
	result, replay, e := h.functions.Invoke(r.Context(), authenticatedApp(r).App.ID, chi.URLParam(r, "functionID"), r.Header.Get("Idempotency-Key"), in.Input)
	if replay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	if e != nil {
		writeFunctionError(w, e)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func writeFunctionError(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, service.ErrFunctionUnavailable):
		writeError(w, http.StatusServiceUnavailable, "function_unavailable", "No function handler claimed this invocation.")
	case errors.Is(e, service.ErrFunctionTimeout):
		writeError(w, http.StatusGatewayTimeout, "function_timeout", "The function handler did not respond before its deadline.")
	case errors.Is(e, service.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "The requested resource was not found.")
	case errors.Is(e, service.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "A function with this name already exists.")
	default:
		writeServiceError(w, e)
	}
}
