package httpapi

import (
	"errors"
	"net/http"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type realtimeFileHandlers struct{ files *service.RealtimeFileService }

func (handlers realtimeFileHandlers) create(response http.ResponseWriter, request *http.Request) {
	if handlers.files == nil {
		writeError(response, http.StatusServiceUnavailable, "file_messaging_disabled", "File messaging is not configured.")
		return
	}
	var input service.CreateRealtimeFileInput
	if decodeJSON(request, &input) != nil {
		writeFileError(response, service.ErrInvalidInput)
		return
	}
	result, err := handlers.files.Create(request.Context(), authenticatedApp(request).App.ID, input)
	if err != nil {
		writeFileError(response, err)
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	writeJSON(response, http.StatusCreated, result)
}

func (handlers realtimeFileHandlers) complete(response http.ResponseWriter, request *http.Request) {
	if handlers.files == nil {
		writeError(response, http.StatusServiceUnavailable, "file_messaging_disabled", "File messaging is not configured.")
		return
	}
	file, err := handlers.files.Complete(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "fileID"))
	if err != nil {
		writeFileError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, file)
}

func (handlers realtimeFileHandlers) download(response http.ResponseWriter, request *http.Request) {
	if handlers.files == nil {
		writeError(response, http.StatusServiceUnavailable, "file_messaging_disabled", "File messaging is not configured.")
		return
	}
	result, err := handlers.files.Download(request.Context(), authenticatedApp(request).App.ID, chi.URLParam(request, "fileID"))
	if err != nil {
		writeFileError(response, err)
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	writeJSON(response, http.StatusOK, result)
}

func writeFileError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(response, http.StatusBadRequest, "invalid_request", "The file metadata is invalid.")
	case errors.Is(err, service.ErrNotFound):
		writeError(response, http.StatusNotFound, "file_not_found", "The file was not found.")
	case errors.Is(err, service.ErrConflict):
		writeError(response, http.StatusConflict, "file_not_ready", "The file is not ready or failed verification.")
	case errors.Is(err, service.ErrInvalidDependency):
		writeError(response, http.StatusServiceUnavailable, "file_messaging_unavailable", "File messaging is temporarily unavailable.")
	default:
		writeError(response, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
	}
}
