package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"relayhub.local/relayhub/internal/config"
)

func TestBootstrapHealthAndReadiness(t *testing.T) {
	h := NewHandler(config.Config{BackendToken: "b", WorkerToken: "w", RealtimeToken: "r"})
	cases := []struct {
		method, path string
		status       int
	}{
		{"GET", "/healthz", http.StatusOK},
		{"POST", "/healthz", http.StatusMethodNotAllowed},
		{"GET", "/readyz", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tc.status {
				t.Fatalf("want %d, got %d", tc.status, rr.Code)
			}
		})
	}
}

func TestJobsAndAttemptsLifecycle(t *testing.T) {
	cfg := config.Config{BackendToken: "backend-key", WorkerToken: "worker-key", RealtimeToken: "realtime-key"}
	h := NewHandler(cfg)

	backend := "Bearer backend-key"
	worker := "Bearer worker-key"

	create := func(path, body string, token string, idem string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, ioBody(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", token)
		r.Header.Set("Idempotency-Key", idem)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr
	}

	r1 := create("/api/v1/jobs", `{"queue":"demo-process","handlerVersion":"v1","data":{"appJobId":"1"}}`, backend, "idem-1")
	if r1.Code != http.StatusAccepted {
		t.Fatalf("create1 %d body=%s", r1.Code, r1.Body.String())
	}
	type jobResp struct {
		JobID  string `json:"jobId"`
		Status string `json:"status"`
	}
	var j1 jobResp
	if err := json.Unmarshal(r1.Body.Bytes(), &j1); err != nil {
		t.Fatal(err)
	}
	if j1.JobID == "" || j1.Status == "" {
		t.Fatalf("unexpected create body %s", r1.Body.String())
	}

	r2 := create("/api/v1/jobs", `{"queue":"demo-process","handlerVersion":"v1","data":{"appJobId":"1"}}`, backend, "idem-1")
	var j2 jobResp
	if r2.Code != http.StatusAccepted {
		t.Fatalf("create2 %d body=%s", r2.Code, r2.Body.String())
	}
	if err := json.Unmarshal(r2.Body.Bytes(), &j2); err != nil {
		t.Fatal(err)
	}
	if j2.JobID != j1.JobID {
		t.Fatalf("idempotent mismatch %s %s", j1.JobID, j2.JobID)
	}

	r3 := create("/api/v1/jobs", `{"queue":"demo-process","handlerVersion":"v1","data":{"appJobId":"2"}}`, backend, "idem-1")
	if r3.Code != http.StatusConflict {
		t.Fatalf("idempotency conflict expected 409, got %d body=%s", r3.Code, r3.Body.String())
	}

	claimReq := httptest.NewRequest(http.MethodPost, "/api/v1/workers/claim", ioBody(`{"queue":"demo-process","handlerVersion":"v1","workerId":"worker-1"}`))
	claimReq.Header.Set("Content-Type", "application/json")
	claimReq.Header.Set("Authorization", worker)
	claimRR := httptest.NewRecorder()
	h.ServeHTTP(claimRR, claimReq)
	if claimRR.Code != http.StatusOK {
		t.Fatalf("claim %d body=%s", claimRR.Code, claimRR.Body.String())
	}
	var claimResp struct {
		AttemptID  string         `json:"attemptId"`
		LeaseToken string         `json:"leaseToken"`
		Job        map[string]any `json:"job"`
	}
	if err := json.Unmarshal(claimRR.Body.Bytes(), &claimResp); err != nil {
		t.Fatal(err)
	}
	if claimResp.AttemptID == "" || claimResp.LeaseToken == "" {
		t.Fatalf("invalid claim payload %s", claimRR.Body.String())
	}

	heartbeatReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/attempts/%s/heartbeat", claimResp.AttemptID), ioBody(fmt.Sprintf(`{"leaseToken":"%s"}`, claimResp.LeaseToken)))
	heartbeatReq.Header.Set("Content-Type", "application/json")
	heartbeatReq.Header.Set("Authorization", worker)
	heartbeatRR := httptest.NewRecorder()
	h.ServeHTTP(heartbeatRR, heartbeatReq)
	if heartbeatRR.Code != http.StatusOK {
		t.Fatalf("heartbeat %d body=%s", heartbeatRR.Code, heartbeatRR.Body.String())
	}

	completeReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/attempts/%s/complete", claimResp.AttemptID), ioBody(`{"leaseToken":"`+claimResp.LeaseToken+`","result":{"ok":true}}`))
	completeReq.Header.Set("Content-Type", "application/json")
	completeReq.Header.Set("Authorization", worker)
	completeRR := httptest.NewRecorder()
	h.ServeHTTP(completeRR, completeReq)
	if completeRR.Code != http.StatusOK {
		t.Fatalf("complete %d body=%s", completeRR.Code, completeRR.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+j1.JobID, nil)
	getReq.Header.Set("Authorization", backend)
	getRR := httptest.NewRecorder()
	h.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("get job %d body=%s", getRR.Code, getRR.Body.String())
	}
	var got job
	if err := json.Unmarshal(getRR.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "succeeded" {
		t.Fatalf("job status %q", got.Status)
	}
}

func TestRealtimeEndpoints(t *testing.T) {
	h := NewHandler(config.Config{BackendToken: "backend-key", WorkerToken: "worker-key", RealtimeToken: "realtime-key"})
	backend := "Bearer backend-key"

	sessReq := httptest.NewRequest(http.MethodPost, "/api/v1/realtime/sessions", ioBody(`{"userId":"user-1"}`))
	sessReq.Header.Set("Content-Type", "application/json")
	sessReq.Header.Set("Authorization", backend)
	sessRR := httptest.NewRecorder()
	h.ServeHTTP(sessRR, sessReq)
	if sessRR.Code != http.StatusCreated {
		t.Fatalf("session %d body=%s", sessRR.Code, sessRR.Body.String())
	}

	grantReq := httptest.NewRequest(http.MethodPost, "/api/v1/realtime/grants", ioBody(`{"userId":"user-1","channel":"jobs/1"}`))
	grantReq.Header.Set("Content-Type", "application/json")
	grantReq.Header.Set("Authorization", backend)
	grantRR := httptest.NewRecorder()
	h.ServeHTTP(grantRR, grantReq)
	if grantRR.Code != http.StatusCreated {
		t.Fatalf("grant %d body=%s", grantRR.Code, grantRR.Body.String())
	}

	publishReq := httptest.NewRequest(http.MethodPost, "/api/v1/realtime/publish", ioBody(`{"channel":"jobs/1","eventId":"evt-1","type":"tick","data":{"n":1}}`))
	publishReq.Header.Set("Content-Type", "application/json")
	publishReq.Header.Set("Authorization", backend)
	publishRR := httptest.NewRecorder()
	h.ServeHTTP(publishRR, publishReq)
	if publishRR.Code != http.StatusAccepted {
		t.Fatalf("publish %d body=%s", publishRR.Code, publishRR.Body.String())
	}
}

func TestAuthError(t *testing.T) {
	h := NewHandler(config.Config{BackendToken: "backend-key", WorkerToken: "worker-key", RealtimeToken: "realtime-key"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", ioBody(`{"queue":"demo-process","handlerVersion":"v1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "x")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("auth %d body=%s", rr.Code, rr.Body.String())
	}
}

func ioBody(s string) *strings.Reader { return strings.NewReader(s) }
