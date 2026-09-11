package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"relayhub.local/relayhub/internal/config"
)

const (
	scopeJobsEnqueue   = "jobs:enqueue"
	scopeJobsRead      = "jobs:read"
	scopeWorkersClaim  = "workers:claim"
	scopeAttempts      = "attempts:update"
	scopeRealtimePub   = "realtime:publish"
	scopeRealtimeGrant = "realtime:grant"

	statusAccepted  = "accepted"
	statusQueued    = "queued"
	statusRunning   = "running"
	statusSucceeded = "succeeded"
	statusFailed    = "failed"

	leaseSeconds = 60 * time.Second
)

type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type errorResponse struct {
	Error     apiError `json:"error"`
	RequestID string   `json:"requestId"`
}

type tokenScopes map[string]struct{}

type project struct {
	ID              string
	Name            string
	AllowedQueues   map[string]struct{}
	RequestIDPrefix string
}

type credential struct {
	ProjectID string
	Scopes    tokenScopes
}

type job struct {
	ID              string          `json:"jobId"`
	ProjectID       string          `json:"-"`
	Queue           string          `json:"queue"`
	HandlerVersion  string          `json:"handlerVersion"`
	Status          string          `json:"status"`
	ProgressChannel string          `json:"progressChannel"`
	Data            json.RawMessage `json:"data"`
	AttemptCount    int             `json:"attemptCount"`
	Result          json.RawMessage `json:"result,omitempty"`
	Error           string          `json:"error,omitempty"`
	CreatedAt       string          `json:"createdAt"`
	UpdatedAt       string          `json:"updatedAt"`
}

type attempt struct {
	ID             string
	ProjectID      string
	JobID          string
	WorkerID       string
	LeaseToken     string
	LeaseExpiresAt time.Time
	Status         string
	ResultHash     string
}

type idempotencyRecord struct {
	JobID    string
	BodyHash string
}

type attemptProgressReq struct {
	LeaseToken string          `json:"leaseToken"`
	EventID    string          `json:"eventId"`
	Type       string          `json:"type"`
	Data       json.RawMessage `json:"data"`
}

type attemptHeartbeatReq struct {
	LeaseToken string `json:"leaseToken"`
}

type attemptCompleteReq struct {
	LeaseToken string          `json:"leaseToken"`
	Result     json.RawMessage `json:"result"`
}

type attemptFailReq struct {
	LeaseToken string `json:"leaseToken"`
	Error      struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type enqueueReq struct {
	Queue           string          `json:"queue"`
	HandlerVersion  string          `json:"handlerVersion"`
	Data            json.RawMessage `json:"data"`
	ProgressChannel string          `json:"progressChannel"`
}

type claimReq struct {
	Queue          string `json:"queue"`
	HandlerVersion string `json:"handlerVersion"`
	WorkerID       string `json:"workerId"`
	WaitSeconds    int    `json:"waitSeconds"`
}

type sessionReq struct {
	UserID string `json:"userId"`
}

type grantReq struct {
	UserID  string `json:"userId"`
	Channel string `json:"channel"`
}

type publishReq struct {
	Channel string          `json:"channel"`
	EventID string          `json:"eventId"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
}

type apiStore struct {
	cfg         config.Config
	mu          sync.Mutex
	projects    map[string]*project
	credentials map[string]*credential
	jobs        map[string]*job
	queueIndex  map[string]map[string][]string // project -> queue -> jobIDs in insertion order
	attempts    map[string]*attempt
	idempotent  map[string]idempotencyRecord
	queueByID   map[string]map[string]bool // project -> queue allowlist
}

func NewHandler(cfg config.Config) http.Handler {
	store := &apiStore{
		cfg:         cfg,
		projects:    map[string]*project{},
		credentials: map[string]*credential{},
		jobs:        map[string]*job{},
		queueIndex:  map[string]map[string][]string{},
		attempts:    map[string]*attempt{},
		idempotent:  map[string]idempotencyRecord{},
		queueByID:   map[string]map[string]bool{},
	}

	projectID := "project_default"
	store.projects[projectID] = &project{
		ID:   projectID,
		Name: "Default Project",
		AllowedQueues: map[string]struct{}{
			"demo-process":    {},
			"demo-process-v1": {},
			"demo":            {},
			"demo-v1":         {},
		},
		RequestIDPrefix: projectID,
	}
	store.queueByID[projectID] = map[string]bool{"demo-process": true, "demo-process-v1": true, "demo": true, "demo-v1": true}

	backendScopes := tokenScopes{
		scopeJobsEnqueue:   {},
		scopeJobsRead:      {},
		scopeRealtimePub:   {},
		scopeRealtimeGrant: {},
		"admin:all":        {},
	}
	workerScopes := tokenScopes{
		scopeWorkersClaim: {},
		scopeAttempts:     {},
		scopeJobsRead:     {},
	}
	realtimeScopes := tokenScopes{
		scopeRealtimePub:   {},
		scopeRealtimeGrant: {},
	}

	if cfg.BackendToken != "" {
		store.credentials[cfg.BackendToken] = &credential{ProjectID: projectID, Scopes: backendScopes}
	}
	if cfg.WorkerToken != "" {
		store.credentials[cfg.WorkerToken] = &credential{ProjectID: projectID, Scopes: workerScopes}
	}
	if cfg.RealtimeToken != "" {
		store.credentials[cfg.RealtimeToken] = &credential{ProjectID: projectID, Scopes: realtimeScopes}
	}

	return store
}

func (s *apiStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID, _ := newRequestID()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("Cache-Control", "no-store")

	fail := func(status int, code, message string, retryable bool) {
		writeError(w, status, requestID, code, message, retryable)
	}

	path := r.URL.Path
	switch {
	case path == "/healthz" || path == "/readyz":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use GET for health checks", false)
			return
		}
		if path == "/healthz" {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "provider": "in-memory"})
		return

	case path == "/api/v1/jobs":
		s.handleJobs(w, r, fail)
		return

	case strings.HasPrefix(path, "/api/v1/jobs/"):
		s.handleJobByID(w, r, fail)
		return

	case path == "/api/v1/workers/claim":
		s.handleWorkerClaim(w, r, fail)
		return

	case strings.HasPrefix(path, "/api/v1/attempts/"):
		s.handleAttempt(w, r, fail)
		return

	case path == "/api/v1/realtime/sessions":
		s.handleRealtimeSessions(w, r, fail)
		return

	case path == "/api/v1/realtime/grants":
		s.handleRealtimeGrants(w, r, fail)
		return

	case path == "/api/v1/realtime/publish":
		s.handleRealtimePublish(w, r, fail)
		return

	case path == "/connection/websocket":
		fail(http.StatusNotImplemented, "NOT_IMPLEMENTED", "WebSocket engine is not embedded in this runtime", false)
		return
	}

	fail(http.StatusNotFound, "NOT_FOUND", "Resource not found", false)
}

func (s *apiStore) handleJobs(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if _, ok := cred.Scopes[scopeJobsEnqueue]; !ok {
			fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing jobs:enqueue scope", false)
			return
		}
		var req enqueueReq
		raw, err := decodeJSON(r)
		if err != nil {
			fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
			return
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			fail(http.StatusBadRequest, "INVALID_JSON", "Invalid request body", false)
			return
		}
		if req.Queue == "" {
			fail(http.StatusBadRequest, "INVALID_REQUEST", "queue is required", false)
			return
		}
		if !s.isQueueAllowed(cred.ProjectID, req.Queue) {
			fail(http.StatusBadRequest, "INVALID_QUEUE", "Queue not allowed for this credential", false)
			return
		}
		idempotency := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idempotency == "" {
			fail(http.StatusBadRequest, "IDEMPOTENCY_REQUIRED", "Idempotency-Key is required", false)
			return
		}
		if len(idempotency) > 128 {
			fail(http.StatusBadRequest, "IDEMPOTENCY_INVALID", "Idempotency-Key must be <=128 chars", false)
			return
		}

		hash := sha256Hex(raw)
		key := fmt.Sprintf("%s|%s|%s", cred.ProjectID, req.Queue, idempotency)
		s.mu.Lock()
		if rec, exists := s.idempotent[key]; exists {
			job := s.jobs[rec.JobID]
			if job != nil {
				s.mu.Unlock()
				if rec.BodyHash == hash {
					writeJSON(w, http.StatusAccepted, map[string]any{"jobId": job.ID, "status": job.Status})
					return
				}
				fail(http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Different body for same key", false)
				return
			}
		}
		job := &job{
			ID:              newID("job_"),
			ProjectID:       cred.ProjectID,
			Queue:           req.Queue,
			HandlerVersion:  req.HandlerVersion,
			ProgressChannel: req.ProgressChannel,
			Status:          statusAccepted,
			Data:            req.Data,
			AttemptCount:    0,
			CreatedAt:       nowRFC3339(),
			UpdatedAt:       nowRFC3339(),
		}
		s.jobs[job.ID] = job
		if _, ok := s.queueIndex[cred.ProjectID]; !ok {
			s.queueIndex[cred.ProjectID] = map[string][]string{}
		}
		if q, ok := s.queueIndex[cred.ProjectID][req.Queue]; ok {
			s.queueIndex[cred.ProjectID][req.Queue] = append(q, job.ID)
		} else {
			s.queueIndex[cred.ProjectID][req.Queue] = []string{job.ID}
		}
		s.idempotent[key] = idempotencyRecord{JobID: job.ID, BodyHash: hash}
		s.mu.Unlock()
		writeJSON(w, http.StatusAccepted, map[string]any{"jobId": job.ID, "status": statusQueued})
		return
	case http.MethodGet:
		fail(http.StatusNotImplemented, "NOT_IMPLEMENTED", "List jobs is not implemented in this phase", false)
	default:
		w.Header().Set("Allow", http.MethodPost)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use POST on /api/v1/jobs", false)
	}
}

func (s *apiStore) handleJobByID(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}
	if _, ok := cred.Scopes[scopeJobsRead]; !ok {
		if r.Method == http.MethodGet {
			fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing jobs:read scope", false)
			return
		}
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/jobs/"), "/")
	if len(parts) != 1 || parts[0] == "" {
		fail(http.StatusNotFound, "NOT_FOUND", "Job id missing", false)
		return
	}
	jobID := parts[0]

	s.mu.Lock()
	job, ok := s.jobs[jobID]
	s.mu.Unlock()
	if !ok {
		fail(http.StatusNotFound, "NOT_FOUND", "Job not found", false)
		return
	}
	if job.ProjectID != cred.ProjectID {
		fail(http.StatusNotFound, "NOT_FOUND", "Job not found", false)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if r.Method == http.MethodGet {
			if _, ok := cred.Scopes[scopeJobsRead]; !ok {
				fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing jobs:read scope", false)
				return
			}
			writeJSON(w, http.StatusOK, job)
			return
		}
	default:
		w.Header().Set("Allow", http.MethodGet)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use GET for /api/v1/jobs/{id}", false)
	}
}

func (s *apiStore) handleWorkerClaim(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}
	if _, ok := cred.Scopes[scopeWorkersClaim]; !ok {
		fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing workers:claim scope", false)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use POST for /api/v1/workers/claim", false)
		return
	}
	var req claimReq
	if err := decodeJSONInto(r, &req); err != nil {
		fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
		return
	}
	if req.Queue == "" {
		fail(http.StatusBadRequest, "INVALID_REQUEST", "queue is required", false)
		return
	}
	if !s.isQueueAllowed(cred.ProjectID, req.Queue) {
		fail(http.StatusBadRequest, "INVALID_QUEUE", "Queue not allowed for this credential", false)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.queueIndex[cred.ProjectID][req.Queue]
	for _, jid := range queue {
		job := s.jobs[jid]
		if job == nil {
			continue
		}
		if job.ProjectID != cred.ProjectID || job.Queue != req.Queue {
			continue
		}
		if req.HandlerVersion != "" && job.HandlerVersion != "" && req.HandlerVersion != job.HandlerVersion {
			continue
		}
		if job.Status != statusAccepted && job.Status != statusQueued {
			continue
		}

		leaseToken := newID("lease_")
		attemptID := newID("attempt_")
		now := time.Now().UTC()
		attempt := &attempt{
			ID:             attemptID,
			ProjectID:      cred.ProjectID,
			JobID:          jid,
			WorkerID:       req.WorkerID,
			LeaseToken:     leaseToken,
			LeaseExpiresAt: now.Add(leaseSeconds),
			Status:         "running",
		}
		job.Status = statusRunning
		job.AttemptCount++
		job.UpdatedAt = nowRFC3339At(now)
		s.attempts[attemptID] = attempt

		writeJSON(w, http.StatusOK, map[string]any{
			"job": map[string]any{
				"jobId":          job.ID,
				"status":         job.Status,
				"attemptCount":   job.AttemptCount,
				"handlerVersion": job.HandlerVersion,
				"queue":          job.Queue,
			},
			"attemptId":      attempt.ID,
			"leaseToken":     attempt.LeaseToken,
			"leaseExpiresAt": attempt.LeaseExpiresAt.Format(time.RFC3339),
		})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiStore) handleAttempt(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}
	if _, ok := cred.Scopes[scopeAttempts]; !ok {
		fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing attempts:update scope", false)
		return
	}

	segments := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/attempts/"), "/")
	if len(segments) != 2 || segments[0] == "" || segments[1] == "" {
		fail(http.StatusNotFound, "NOT_FOUND", "Invalid attempts path", false)
		return
	}
	attemptID := segments[0]
	action := segments[1]

	s.mu.Lock()
	attempt, ok := s.attempts[attemptID]
	s.mu.Unlock()
	if !ok {
		fail(http.StatusNotFound, "NOT_FOUND", "Attempt not found", false)
		return
	}
	if attempt.ProjectID != cred.ProjectID {
		fail(http.StatusNotFound, "NOT_FOUND", "Attempt not found", false)
		return
	}

	s.mu.Lock()
	job := s.jobs[attempt.JobID]
	s.mu.Unlock()
	if job == nil {
		fail(http.StatusNotFound, "NOT_FOUND", "Job for attempt not found", false)
		return
	}

	switch r.Method {
	case http.MethodPost:
		switch action {
		case "heartbeat":
			var req attemptHeartbeatReq
			if err := decodeJSONInto(r, &req); err != nil {
				fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if err := s.assertActiveLeaseLocked(attempt, req.LeaseToken, nowRFC3339()); err != nil {
				fail(http.StatusConflict, "LEASE_LOST", err.Error(), false)
				return
			}
			attempt.LeaseExpiresAt = time.Now().UTC().Add(leaseSeconds)
			writeJSON(w, http.StatusOK, map[string]string{"leaseExpiresAt": attempt.LeaseExpiresAt.Format(time.RFC3339)})
		case "progress":
			var req attemptProgressReq
			if err := decodeJSONInto(r, &req); err != nil {
				fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if err := s.assertActiveLeaseLocked(attempt, req.LeaseToken, nowRFC3339()); err != nil {
				fail(http.StatusConflict, "LEASE_LOST", err.Error(), false)
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]string{"eventId": req.EventID})
		case "complete":
			var req attemptCompleteReq
			if err := decodeJSONInto(r, &req); err != nil {
				fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if err := s.assertActiveLeaseLocked(attempt, req.LeaseToken, nowRFC3339()); err != nil {
				fail(http.StatusConflict, "LEASE_LOST", err.Error(), false)
				return
			}
			if attempt.Status == statusSucceeded {
				if req.Result == nil {
					writeJSON(w, http.StatusOK, map[string]string{"attemptId": attempt.ID, "status": attempt.Status})
					return
				}
				if reqHash := sha256Hex(req.Result); reqHash == attempt.ResultHash {
					writeJSON(w, http.StatusOK, map[string]string{"attemptId": attempt.ID, "status": attempt.Status})
					return
				}
				fail(http.StatusConflict, "RESULT_CONFLICT", "Attempt already completed with different result", false)
				return
			}
			if job == nil {
				fail(http.StatusNotFound, "NOT_FOUND", "Job for attempt not found", false)
				return
			}
			attempt.Status = statusSucceeded
			attempt.ResultHash = sha256Hex(req.Result)
			job.Status = statusSucceeded
			job.Result = req.Result
			job.UpdatedAt = nowRFC3339()
			writeJSON(w, http.StatusOK, map[string]string{"attemptId": attempt.ID, "status": statusSucceeded})
		case "fail":
			var req attemptFailReq
			if err := decodeJSONInto(r, &req); err != nil {
				fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if err := s.assertActiveLeaseLocked(attempt, req.LeaseToken, nowRFC3339()); err != nil {
				fail(http.StatusConflict, "LEASE_LOST", err.Error(), false)
				return
			}
			if req.Error.Code == "" {
				req.Error.Code = "WORKER_ERROR"
			}
			job.Status = statusFailed
			job.Error = req.Error.Message
			job.UpdatedAt = nowRFC3339()
			attempt.Status = statusFailed
			writeJSON(w, http.StatusOK, map[string]string{"attemptId": attempt.ID, "status": statusFailed})
		default:
			fail(http.StatusNotFound, "NOT_FOUND", "Unknown attempt action", false)
		}
	default:
		w.Header().Set("Allow", http.MethodPost)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use POST for attempt endpoints", false)
	}
}

func (s *apiStore) handleRealtimeSessions(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}
	if _, ok := cred.Scopes[scopeRealtimeGrant]; !ok {
		if _, ok = cred.Scopes[scopeRealtimePub]; !ok {
			fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing realtime scope", false)
			return
		}
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use POST for /api/v1/realtime/sessions", false)
		return
	}
	var req sessionReq
	if err := decodeJSONInto(r, &req); err != nil {
		fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
		return
	}
	if req.UserID == "" {
		fail(http.StatusBadRequest, "INVALID_REQUEST", "userId is required", false)
		return
	}
	expiresAt := time.Now().UTC().Add(5 * time.Minute)
	writeJSON(w, http.StatusCreated, map[string]any{
		"url":       "wss://relayhub.dungxbuif.com/connection/websocket",
		"token":     newID("session_"),
		"expiresAt": expiresAt.Format(time.RFC3339),
		"userId":    req.UserID,
		"projectId": cred.ProjectID,
	})
}

func (s *apiStore) handleRealtimeGrants(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}
	if _, ok := cred.Scopes[scopeRealtimeGrant]; !ok {
		fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing realtime:grant scope", false)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use POST for /api/v1/realtime/grants", false)
		return
	}
	var req grantReq
	if err := decodeJSONInto(r, &req); err != nil {
		fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
		return
	}
	if req.UserID == "" || req.Channel == "" {
		fail(http.StatusBadRequest, "INVALID_REQUEST", "userId and channel are required", false)
		return
	}
	expiresAt := time.Now().UTC().Add(5 * time.Minute)
	wireChannel := fmt.Sprintf("p:%s:ch:%s", cred.ProjectID, req.Channel)
	writeJSON(w, http.StatusCreated, map[string]any{
		"channel":     req.Channel,
		"wireChannel": wireChannel,
		"token":       newID("grant_"),
		"expiresAt":   expiresAt.Format(time.RFC3339),
		"userId":      req.UserID,
	})
}

func (s *apiStore) handleRealtimePublish(w http.ResponseWriter, r *http.Request, fail func(int, string, string, bool)) {
	cred, ok := s.authenticate(r)
	if !ok {
		fail(http.StatusUnauthorized, "AUTH_REQUIRED", "Missing or invalid Authorization Bearer token", false)
		return
	}
	if _, ok := cred.Scopes[scopeRealtimePub]; !ok {
		fail(http.StatusForbidden, "SCOPE_FORBIDDEN", "Missing realtime:publish scope", false)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		fail(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Use POST for /api/v1/realtime/publish", false)
		return
	}
	var req publishReq
	if err := decodeJSONInto(r, &req); err != nil {
		fail(http.StatusBadRequest, "INVALID_JSON", err.Error(), false)
		return
	}
	if req.Channel == "" || req.EventID == "" || req.Type == "" {
		fail(http.StatusBadRequest, "INVALID_REQUEST", "channel, eventId, type are required", false)
		return
	}
	_ = cred
	writeJSON(w, http.StatusAccepted, map[string]any{"eventId": req.EventID, "acceptedByProject": cred.ProjectID})
}

func (s *apiStore) authenticate(r *http.Request) (*credential, bool) {
	authed := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if authed == "" || authed == r.Header.Get("Authorization") {
		return nil, false
	}
	cred, ok := s.credentials[authed]
	if !ok {
		return nil, false
	}
	return cred, true
}

func (s *apiStore) isQueueAllowed(projectID, queue string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	allowed, ok := s.queueByID[projectID]
	if !ok {
		return false
	}
	_, exists := allowed[queue]
	return exists
}

func (s *apiStore) assertActiveLeaseLocked(a *attempt, leaseToken string, requestID string) error {
	if a.LeaseToken != leaseToken {
		return errors.New("lease token does not match")
	}
	if time.Now().UTC().After(a.LeaseExpiresAt) {
		return fmt.Errorf("lease expired")
	}
	if requestID == "" {
		return nil
	}
	return nil
}

func decodeJSON(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, errors.New("request body is required")
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, errors.New("request body is required")
	}
	if !json.Valid(body) {
		return nil, errors.New("request body must be valid JSON")
	}
	return body, nil
}

func decodeJSONInto(r *http.Request, v any) error {
	raw, err := decodeJSON(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return errors.New("invalid request payload")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, requestID, code, message string, retryable bool) {
	writeJSON(w, status, errorResponse{Error: apiError{Code: code, Message: message, Retryable: retryable}, RequestID: requestID})
}

func newRequestID() (string, error) {
	return newID("req_"), nil
}

func newID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func sha256Hex(v []byte) string {
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:])
}

func nowRFC3339() string {
	return nowRFC3339At(time.Now().UTC())
}

func nowRFC3339At(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}
