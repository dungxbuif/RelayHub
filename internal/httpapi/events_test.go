package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"
)

func eventRouter(t *testing.T, wrappers ...func(store.EventStore) store.EventStore) (http.Handler, []service.AppCredentials) {
	t.Helper()
	apps := newHTTPMemoryStore()
	as := service.NewAppService(apps, service.AppOptions{})
	creds := []service.AppCredentials{}
	for _, name := range []string{"producer", "target", "outsider"} {
		_, c, err := as.Create(context.Background(), service.CreateApp{Name: name, DeliveryMode: domain.DeliveryQueue})
		if err != nil {
			t.Fatal(err)
		}
		creds = append(creds, c)
	}
	m := &httpEventMemory{events: map[string]domain.Event{}, jobs: map[string]domain.Job{}, idem: map[string]store.Publication{}}
	var repository store.EventStore = m
	for _, wrap := range wrappers {
		repository = wrap(repository)
	}
	return NewRouter(Dependencies{Apps: as, Events: service.NewEventService(repository, apps, service.EventOptions{}), AdminToken: "admin-test-token", Now: func() time.Time { return time.Unix(1789120800, 0) }, Docs: fstest.MapFS{}, Health: apps, Metrics: http.NotFoundHandler()}), creds
}
func signedEventRequest(t *testing.T, h http.Handler, c service.AppCredentials, method, path string, body []byte, key string) *httptest.ResponseRecorder {
	return requestJSON(t, h, method, path, body, map[string]string{"Idempotency-Key": key, "X-RelayHub-Api-Key": c.APIKey, "X-RelayHub-Timestamp": "1789120800", "X-RelayHub-Signature": auth.Sign([]byte(c.HMACSecret), "1789120800", method, path, body)})
}
func TestEventHTTPRoutes(t *testing.T) {
	h, c := eventRouter(t)
	body := []byte(fmt.Sprintf(`{"type":"order.created","target_app_ids":[%q],"data":{"n":9007199254740993}}`, c[1].AppID))
	for _, key := range []string{"k", "k"} {
		res := signedEventRequest(t, h, c[0], "POST", "/api/v1/events", body, key)
		if res.Code != 202 {
			t.Fatalf("publish %d %s", res.Code, res.Body.String())
		}
	}
	replay := signedEventRequest(t, h, c[0], "POST", "/api/v1/events", body, "k")
	if replay.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("missing replay header")
	}
	var p store.Publication
	if err := json.Unmarshal(replay.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Event.SourceAppID != c[0].AppID || len(p.Jobs) != 1 || !strings.Contains(string(p.Event.Data), "9007199254740993") {
		t.Fatalf("publish response %s", replay.Body.String())
	}
	for _, path := range []string{"/api/v1/events/" + p.Event.ID, "/api/v1/jobs/" + p.Jobs[0].ID} {
		for _, actor := range []int{0, 1, 2} {
			want := 200
			if actor == 2 {
				want = 404
			}
			res := signedEventRequest(t, h, c[actor], "GET", path, nil, "")
			if res.Code != want {
				t.Fatalf("GET %s actor %d: %d %s", path, actor, res.Code, res.Body.String())
			}
		}
	}
	queue := signedEventRequest(t, h, c[1], "GET", "/api/v1/queue?limit=20&wait=0", nil, "")
	var leased []store.LeasedEvent
	if err := json.Unmarshal(queue.Body.Bytes(), &leased); err != nil || queue.Code != 200 || len(leased) != 1 || leased[0].Job.Status != domain.JobLeased {
		t.Fatalf("queue %d %s %v", queue.Code, queue.Body.String(), err)
	}
	empty := signedEventRequest(t, h, c[1], "GET", "/api/v1/queue", nil, "")
	assertStatusAndJSON(t, empty, 200, `[]`)
	ackPath := "/api/v1/events/" + p.Event.ID + "/ack"
	denied := signedEventRequest(t, h, c[2], "POST", ackPath, nil, "")
	if denied.Code != 404 {
		t.Fatalf("cross ack %d", denied.Code)
	}
	for range 2 {
		res := signedEventRequest(t, h, c[1], "POST", ackPath, nil, "")
		if res.Code != 204 || res.Body.Len() != 0 {
			t.Fatalf("ack %d %s", res.Code, res.Body.String())
		}
	}
	control := "/api/v1/jobs/" + p.Jobs[0].ID + "/requeue"
	if res := signedEventRequest(t, h, c[1], "POST", control, nil, ""); res.Code != 401 {
		t.Fatalf("nonadmin %d", res.Code)
	}
	if res := requestJSON(t, h, "POST", control, nil, map[string]string{"Authorization": "Bearer admin-test-token"}); res.Code != 409 {
		t.Fatalf("illegal control %d", res.Code)
	}
	fresh := signedEventRequest(t, h, c[0], "POST", "/api/v1/events", body, "fresh")
	if err := json.Unmarshal(fresh.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"dead-letter", "dead-letter", "requeue"} {
		res := requestJSON(t, h, "POST", "/api/v1/jobs/"+p.Jobs[0].ID+"/"+action, nil, map[string]string{"Authorization": "Bearer admin-test-token"})
		if res.Code != 200 {
			t.Fatalf("%s %d %s", action, res.Code, res.Body.String())
		}
	}
}
func TestEventHTTPErrorsAndSignedBody(t *testing.T) {
	h, c := eventRouter(t)
	tests := []struct {
		method, path, body, key string
		status                  int
	}{
		{"POST", "/api/v1/events", `{"type":"x","target_app_ids":[],"data":{}}`, "k", 400},
		{"POST", "/api/v1/events", `{`, "k", 400},
		{"POST", "/api/v1/events", `{"source_app_id":"evil"}`, "k", 400},
		{"POST", "/api/v1/events", `{}`, "", 400},
		{"GET", "/api/v1/queue?limit=0", "", "", 400},
		{"GET", "/api/v1/queue?limit=101", "", "", 400},
		{"GET", "/api/v1/queue?limit=bad", "", "", 400},
		{"GET", "/api/v1/queue?wait=31", "", "", 400},
		{"GET", "/api/v1/queue?wait=-1", "", "", 400},
		{"GET", "/api/v1/queue?wait=x", "", "", 400},
		{"GET", "/api/v1/events/missing", "", "", 404},
		{"GET", "/api/v1/jobs/missing", "", "", 404},
		{"POST", "/api/v1/events/missing/ack", "", "", 404},
	}
	for _, tt := range tests {
		res := signedEventRequest(t, h, c[0], tt.method, tt.path, []byte(tt.body), tt.key)
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		code := map[int]string{400: "invalid_request", 404: "not_found"}[tt.status]
		if err := json.Unmarshal(res.Body.Bytes(), &envelope); err != nil || res.Code != tt.status || envelope.Error.Code != code || envelope.Error.Message == "" {
			t.Fatalf("%s: %d %s", tt.path, res.Code, res.Body.String())
		}
	}
	for _, path := range []string{"/api/v1/events", "/api/v1/queue", "/api/v1/events/missing", "/api/v1/jobs/missing", "/api/v1/events/missing/ack"} {
		method := "GET"
		if path == "/api/v1/events" || strings.HasSuffix(path, "/ack") {
			method = "POST"
		}
		res := requestJSON(t, h, method, path, nil, nil)
		if res.Code != 401 {
			t.Fatalf("unsigned %s: %d", path, res.Code)
		}
	}
	big := signedEventRequest(t, h, c[0], "POST", "/api/v1/events", []byte(strings.Repeat("x", (1<<20)+1)), "k")
	if big.Code != 413 {
		t.Fatalf("big %d", big.Code)
	}
	tampered := requestJSON(t, h, "POST", "/api/v1/events", []byte(`{}`), map[string]string{"X-RelayHub-Api-Key": c[0].APIKey, "X-RelayHub-Timestamp": "1789120800", "X-RelayHub-Signature": auth.Sign([]byte(c[0].HMACSecret), "1789120800", "POST", "/api/v1/events", []byte(`{"data":{}}`))})
	if tampered.Code != 401 {
		t.Fatal("body tamper passed")
	}
}

// In-memory boundary for service policy tests; concurrency/atomicity are separately tested against Redis.
type httpEventMemory struct {
	mu     sync.Mutex
	events map[string]domain.Event
	jobs   map[string]domain.Job
	idem   map[string]store.Publication
}

func (m *httpEventMemory) PublishEvent(_ context.Context, p store.Publication, key string, _ store.EventRetention) (store.Publication, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := p.Event.SourceAppID + ":" + key
	if old, ok := m.idem[k]; ok {
		return old, true, nil
	}
	m.idem[k] = p
	m.events[p.Event.ID] = p.Event
	for _, j := range p.Jobs {
		m.jobs[j.ID] = j
	}
	return p, false, nil
}
func (m *httpEventMemory) GetEvent(_ context.Context, id string) (domain.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok {
		return e, store.ErrNotFound
	}
	return e, nil
}
func (m *httpEventMemory) GetJob(_ context.Context, id string) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return j, store.ErrNotFound
	}
	return j, nil
}
func (m *httpEventMemory) LeaseJobs(ctx context.Context, target string, limit int, now time.Time, lease time.Duration) ([]store.LeasedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []store.LeasedEvent{}
	for id, j := range m.jobs {
		if j.TargetAppID == target && (j.Status == domain.JobPending || (j.Status == domain.JobLeased && j.LeaseUntil != nil && !j.LeaseUntil.After(now))) && len(out) < limit {
			j.Status = domain.JobLeased
			end := now.Add(lease)
			j.LeaseUntil = &end
			j.UpdatedAt = now
			j.Attempts++
			m.jobs[id] = j
			out = append(out, store.LeasedEvent{Event: m.events[j.EventID], Job: j})
		}
	}
	return out, nil
}
func (m *httpEventMemory) AckEvent(ctx context.Context, target, event string, now time.Time, retention time.Duration) error {
	m.mu.Lock()
	var id string
	for _, j := range m.jobs {
		if j.TargetAppID == target && j.EventID == event {
			id = j.ID
		}
	}
	m.mu.Unlock()
	if id == "" {
		return store.ErrNotFound
	}
	_, err := m.TransitionJob(ctx, id, domain.JobAcked, now, retention)
	return err
}
func (m *httpEventMemory) TransitionJob(_ context.Context, id string, status domain.JobStatus, now time.Time, _ time.Duration) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return j, store.ErrNotFound
	}
	if !j.Status.CanTransition(status) {
		return j, store.ErrConflict
	}
	j.Status = status
	j.UpdatedAt = now
	j.LeaseUntil = nil
	m.jobs[id] = j
	return j, nil
}

func (m *httpEventMemory) FindPublication(_ context.Context, source, key string) (store.Publication, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.idem[source+":"+key]
	if !ok {
		return p, store.ErrNotFound
	}
	return p, nil
}
