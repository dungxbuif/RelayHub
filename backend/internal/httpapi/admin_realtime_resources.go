package httpapi

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/go-chi/chi/v5"
)

type adminRealtimeResourceHandlers struct {
	apps    *service.AppService
	history *realtime.Hub
	files   *service.RealtimeFileService
	push    *service.RealtimePushService
	control realtimeAdmin
}

func (handlers adminRealtimeResourceHandlers) activeApp(response http.ResponseWriter, request *http.Request) (domain.App, bool) {
	if handlers.apps == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		return domain.App{}, false
	}
	app, err := handlers.apps.Get(request.Context(), chi.URLParam(request, "appID"))
	if err != nil || !app.Enabled {
		if errors.Is(err, service.ErrInvalidDependency) {
			writeError(response, http.StatusServiceUnavailable, "not_ready", "A required dependency is unavailable.")
		} else {
			writeError(response, http.StatusNotFound, "not_found", "The requested resource was not found.")
		}
		return domain.App{}, false
	}
	return app, true
}

func (handlers adminRealtimeResourceHandlers) historyList(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.history == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "Realtime history is unavailable.")
		return
	}
	limit := 50
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(response, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100.")
			return
		}
		limit = parsed
	}
	if limit < 1 || limit > 100 {
		writeError(response, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100.")
		return
	}
	result, protocolErr := handlers.history.HistoryForApp(chi.URLParam(request, "appID"), chi.URLParam(request, "channel"), request.URL.Query().Get("cursor"), limit)
	if protocolErr != nil {
		status := http.StatusServiceUnavailable
		if protocolErr.Code == "invalid_history" {
			status = http.StatusBadRequest
		}
		writeError(response, status, protocolErr.Code, protocolErr.Message)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (handlers adminRealtimeResourceHandlers) connectionsList(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.control == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime connection registry is unavailable.")
		return
	}
	limit := int64(100)
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(response, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 500.")
			return
		}
		limit = parsed
	}
	if limit < 1 || limit > 500 {
		writeError(response, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 500.")
		return
	}
	connections, err := handlers.control.List(request.Context(), chi.URLParam(request, "appID"), limit)
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime connection registry is unavailable.")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"connections": connections})
}

func (handlers adminRealtimeResourceHandlers) connectionDisconnect(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.control == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime command route is unavailable.")
		return
	}
	err := handlers.control.Disconnect(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "connectionID"))
	switch {
	case errors.Is(err, realtime.ErrConnectionNotFound):
		writeError(response, http.StatusNotFound, "not_found", "The realtime connection was not found.")
	case err != nil:
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime command route is unavailable.")
	default:
		response.WriteHeader(http.StatusNoContent)
	}
}

func (handlers adminRealtimeResourceHandlers) connectionGet(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.control == nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime connection registry is unavailable.")
		return
	}
	connections, err := handlers.control.List(request.Context(), chi.URLParam(request, "appID"), 500)
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "The realtime connection registry is unavailable.")
		return
	}
	for _, connection := range connections {
		if connection.ConnectionID == chi.URLParam(request, "connectionID") {
			writeJSON(response, http.StatusOK, connection)
			return
		}
	}
	writeError(response, http.StatusNotFound, "not_found", "The realtime connection was not found.")
}

func (handlers adminRealtimeResourceHandlers) fileMetadata(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.files == nil {
		writeError(response, http.StatusServiceUnavailable, "file_messaging_unavailable", "File messaging is temporarily unavailable.")
		return
	}
	file, err := handlers.files.AdminMetadata(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "fileID"))
	if err != nil {
		writeFileError(response, err)
		return
	}
	response.Header().Set("Cache-Control", "no-store")
	writeJSON(response, http.StatusOK, file)
}

func (handlers adminRealtimeResourceHandlers) fileDownload(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.files == nil {
		writeError(response, http.StatusServiceUnavailable, "file_messaging_unavailable", "File messaging is temporarily unavailable.")
		return
	}
	file, stream, err := handlers.files.AdminDownload(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "fileID"))
	if err != nil {
		writeFileError(response, err)
		return
	}
	defer stream.Close()
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Content-Type", file.MIMEType)
	response.Header().Set("Content-Length", strconv.FormatInt(file.SizeBytes, 10))
	response.Header().Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(file.Name))
	response.WriteHeader(http.StatusOK)
	_, _ = io.Copy(response, stream)
}

func (handlers adminRealtimeResourceHandlers) pushList(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.push == nil {
		writeError(response, http.StatusServiceUnavailable, "push_unavailable", "Push is temporarily unavailable.")
		return
	}
	devices, err := handlers.push.List(request.Context(), chi.URLParam(request, "appID"), request.URL.Query().Get("channel"))
	if err != nil {
		writePushError(response, err)
		return
	}
	if devices == nil {
		devices = []domain.PushDevice{}
	}
	writeJSON(response, http.StatusOK, map[string]any{"devices": devices})
}

func (handlers adminRealtimeResourceHandlers) pushRegister(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.push == nil {
		writeError(response, http.StatusServiceUnavailable, "push_unavailable", "Push is temporarily unavailable.")
		return
	}
	var input service.RegisterPushDeviceInput
	if decodeJSON(request, &input) != nil {
		writePushError(response, service.ErrInvalidInput)
		return
	}
	device, err := handlers.push.Register(request.Context(), chi.URLParam(request, "appID"), input)
	if err != nil {
		writePushError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, device)
}

func (handlers adminRealtimeResourceHandlers) pushDelete(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.push == nil {
		writeError(response, http.StatusServiceUnavailable, "push_unavailable", "Push is temporarily unavailable.")
		return
	}
	if err := handlers.push.Delete(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "deviceID")); err != nil {
		writePushError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (handlers adminRealtimeResourceHandlers) pushBind(bind bool) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if _, ok := handlers.activeApp(response, request); !ok {
			return
		}
		if handlers.push == nil {
			writeError(response, http.StatusServiceUnavailable, "push_unavailable", "Push is temporarily unavailable.")
			return
		}
		err := handlers.push.Bind(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "channel"), chi.URLParam(request, "deviceID"), bind)
		if err != nil {
			writePushError(response, err)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}
}

func (handlers adminRealtimeResourceHandlers) pushPublish(response http.ResponseWriter, request *http.Request) {
	if _, ok := handlers.activeApp(response, request); !ok {
		return
	}
	if handlers.push == nil {
		writeError(response, http.StatusServiceUnavailable, "push_unavailable", "Push is temporarily unavailable.")
		return
	}
	var input domain.PushNotification
	if decodeJSON(request, &input) != nil {
		writePushError(response, service.ErrInvalidInput)
		return
	}
	outcomes, err := handlers.push.Publish(request.Context(), chi.URLParam(request, "appID"), chi.URLParam(request, "channel"), input)
	if err != nil {
		writePushError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"outcomes": outcomes})
}
