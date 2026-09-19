package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/config"
	"github.com/dungxbuif/RelayHub/internal/platform"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"gopkg.in/yaml.v3"
)

type fakeSharedRedis struct {
	pingErr      error
	pingCalls    atomic.Int64
	heartbeats   atomic.Int64
	releases     atomic.Int64
	closed       atomic.Bool
	lastInstance platform.Instance
	mu           sync.Mutex
}

func (f *fakeSharedRedis) Ping(context.Context) error {
	f.pingCalls.Add(1)
	return f.pingErr
}
func (f *fakeSharedRedis) Heartbeat(_ context.Context, instance platform.Instance, _ time.Duration) error {
	f.mu.Lock()
	f.lastInstance = instance
	f.mu.Unlock()
	f.heartbeats.Add(1)
	return nil
}
func (f *fakeSharedRedis) ReleaseInstance(context.Context, platform.Instance) error {
	f.releases.Add(1)
	return nil
}
func (f *fakeSharedRedis) Close() error { f.closed.Store(true); return nil }

type fakeHealth struct {
	err   error
	calls atomic.Int64
}

func (f *fakeHealth) Ping(context.Context) error { f.calls.Add(1); return f.err }

func TestRuntimeConstructsRedisBeforeServing(t *testing.T) {
	cfg := config.Config{Redis: config.RedisConfig{
		Mode: "standalone", Addrs: []string{"redis.internal:6379"}, Password: "private",
		KeyPrefix: "rh", ConnectTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 8,
	}}
	fake := &fakeSharedRedis{}
	called := false
	runtime, err := prepareSharedRuntime(context.Background(), "api", cfg, time.Now(), func(_ context.Context, got redisstate.Config) (sharedRedis, error) {
		called = true
		if got.Mode != redisstate.ModeStandalone || got.Password != "private" || len(got.Addrs) != 1 {
			t.Fatalf("Redis config = %+v", got)
		}
		return fake, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || fake.heartbeats.Load() != 1 {
		t.Fatalf("factory called=%v heartbeats=%d", called, fake.heartbeats.Load())
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.releases.Load() != 1 || !fake.closed.Load() {
		t.Fatalf("releases=%d closed=%v", fake.releases.Load(), fake.closed.Load())
	}
}

func TestRuntimeReadinessIncludesRedis(t *testing.T) {
	nats, postgres, redis := &fakeHealth{}, &fakeHealth{}, &fakeHealth{err: errors.New("redis unavailable")}
	health := requiredHealth(nats, postgres, redis)
	if err := health.Ping(context.Background()); err == nil {
		t.Fatal("readiness succeeded while Redis was unavailable")
	}
	if nats.calls.Load() != 1 || postgres.calls.Load() != 1 || redis.calls.Load() != 1 {
		t.Fatalf("health calls nats=%d postgres=%d redis=%d", nats.calls.Load(), postgres.calls.Load(), redis.calls.Load())
	}
}

func TestRuntimeUsesDistinctInstanceGenerationAfterRestart(t *testing.T) {
	now := time.Now()
	first, err := platform.NewInstance("api", "api_fixed", now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := platform.NewInstance("api", "api_fixed", now.Add(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.Generation == second.Generation || first.Generation == 0 || second.Generation == 0 {
		t.Fatalf("restart instances = %+v, %+v", first, second)
	}
}

type blockingRuntimeWorker struct {
	started chan struct{}
}

func (w *blockingRuntimeWorker) Run(ctx context.Context) error {
	close(w.started)
	<-ctx.Done()
	return nil
}

func TestWorkerKeepsDurableLoopRunningWhenRedisReadinessFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	worker := &blockingRuntimeWorker{started: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- runWorker(ctx, worker, &fakeHealth{err: errors.New("redis unavailable")}, config.Config{WorkerHTTPAddr: address, ShutdownTimeout: time.Second})
	}()
	select {
	case <-worker.started:
	case <-time.After(time.Second):
		t.Fatal("durable worker did not start")
	}

	eventuallyHTTPStatus(t, "http://"+address+"/readyz", http.StatusServiceUnavailable)
	select {
	case err := <-done:
		t.Fatalf("worker stopped after readiness failure: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runWorker() error = %v", err)
	}
}

func eventuallyHTTPStatus(t *testing.T, target string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(target)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not return %d", target, want)
}

type orderedNATSShutdown struct {
	drainStarted chan struct{}
	releaseDrain chan struct{}
	closed       chan struct{}
}

func (fake *orderedNATSShutdown) Drain() error {
	close(fake.drainStarted)
	<-fake.releaseDrain
	return nil
}
func (fake *orderedNATSShutdown) Close() { close(fake.closed) }

func TestShutdownNATSWaitsForDrainBeforeClose(t *testing.T) {
	fake := &orderedNATSShutdown{drainStarted: make(chan struct{}), releaseDrain: make(chan struct{}), closed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		shutdownNATS(fake, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		close(done)
	}()
	<-fake.drainStarted
	select {
	case <-fake.closed:
		t.Fatal("Close called before Drain completed")
	case <-time.After(25 * time.Millisecond):
	}
	close(fake.releaseDrain)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not complete after drain")
	}
}

// A probe that loads runtime credentials, follows redirects, or accepts a 503
// must fail these tests: Docker health means this local endpoint returned 200.
func TestHealthcheckCommand(t *testing.T) {
	for _, status := range []int{200, 204, 302, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/readyz")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "sensitive-response")
			}))
			defer server.Close()
			old := os.Args
			defer func() { os.Args = old }()
			os.Args = []string{"relayhub", "healthcheck", server.URL + "/readyz"}
			t.Setenv("RELAYHUB_ADMIN_TOKEN", "")
			t.Setenv("RELAYHUB_SIGNING_SECRET", "")
			var logs bytes.Buffer
			err := run(slog.New(slog.NewJSONHandler(&logs, nil)))
			if (err == nil) != (status == 200) {
				t.Fatalf("status %d: error=%v", status, err)
			}
			if logs.Len() != 0 || (err != nil && strings.Contains(err.Error(), "sensitive-response")) {
				t.Fatal("probe leaked response")
			}
		})
	}
}
func TestHealthcheckRejectsInvalidTargets(t *testing.T) {
	for _, target := range []string{"http://example.com/readyz", "file:///etc/passwd", "http://user:secret@127.0.0.1/readyz", "not-a-url", "http://127.0.0.1:1/readyz"} {
		old := os.Args
		os.Args = []string{"relayhub", "healthcheck", target}
		err := run(slog.New(slog.NewJSONHandler(io.Discard, nil)))
		os.Args = old
		if err == nil {
			t.Fatal("accepted invalid or unreachable target")
		}
		if strings.Contains(err.Error(), target) || strings.Contains(err.Error(), "secret") {
			t.Fatal("probe error leaks target")
		}
	}
}

func TestDeploymentContract(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	read := func(path string) []byte {
		t.Helper()
		b, e := os.ReadFile(filepath.Join(root, path))
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	raw := read("compose.yaml")
	public := read("web/docs/static/deploy/docker-compose.relayhub.yml")
	if !bytes.Equal(raw, public) {
		t.Fatal("root/public Compose drift")
	}
	var c struct {
		Services map[string]map[string]any `yaml:"services"`
		Networks map[string]any            `yaml:"networks"`
		Volumes  map[string]any            `yaml:"volumes"`
	}
	if e := yaml.Unmarshal(raw, &c); e != nil {
		t.Fatal(e)
	}
	if len(c.Services) != 5 || len(c.Networks) != 1 || c.Networks["relayhub"] == nil {
		t.Fatal("expected API, worker, PostgreSQL, NATS, Redis and project network")
	}
	if _, ok := c.Volumes["relayhub-data"]; ok {
		t.Fatal("Redis volume must not be in the release deployment")
	}
	if _, ok := c.Volumes["relayhub-postgres-data"]; !ok {
		t.Fatal("missing PostgreSQL volume")
	}
	if _, ok := c.Volumes["relayhub-nats-data"]; !ok {
		t.Fatal("missing JetStream volume")
	}
	for _, name := range []string{"relayhub-api", "relayhub-worker", "relayhub-postgres", "relayhub-nats", "relayhub-redis"} {
		s, ok := c.Services[name]
		if !ok {
			t.Fatalf("missing %s", name)
		}
		if _, ok := s["container_name"]; ok {
			t.Fatal("fixed container name")
		}
		if _, ok := s["expose"]; ok {
			t.Fatal("explicit exposed port")
		}
		ports, _ := s["ports"].([]any)
		if name == "relayhub-api" {
			if len(ports) != 1 || ports[0] != "${RELAYHUB_PORT:-8080}:8080" {
				t.Fatal("API publication")
			}
		} else if len(ports) > 0 {
			t.Fatal("private service publishes port")
		}
		if s["stop_grace_period"] == nil || s["healthcheck"] == nil {
			t.Fatal("missing lifecycle configuration")
		}
		if name == "relayhub-api" || name == "relayhub-worker" {
			if s["user"] == nil || s["user"] == "0:0" || s["read_only"] != true {
				t.Fatalf("%s must be nonroot readonly", name)
			}
			if !strings.Contains(string(mustYAML(t, s["cap_drop"])), "ALL") || !strings.Contains(string(mustYAML(t, s["security_opt"])), "no-new-privileges") {
				t.Fatal("missing application container security restrictions")
			}
			if !strings.Contains(string(mustYAML(t, s["depends_on"])), "service_healthy") {
				t.Fatal("dependency not healthy")
			}
			if !strings.Contains(string(mustYAML(t, s["healthcheck"])), "healthcheck") {
				t.Fatal("probe must be binary")
			}
		} else {
			if s["user"] != nil || s["read_only"] != nil || s["cap_drop"] != nil || s["security_opt"] != nil {
				t.Fatalf("%s must use official image defaults for volume initialization", name)
			}
		}
	}
	natsService := string(mustYAML(t, c.Services["relayhub-nats"]))
	for _, want := range []string{"nats:2.14.5-alpine", "deploy/nats/nats.conf", "relayhub-nats-data:/data", "RELAYHUB_NATS_USERNAME:?", "RELAYHUB_NATS_PASSWORD:?"} {
		if !strings.Contains(natsService, want) {
			t.Fatalf("NATS missing %s", want)
		}
	}
	api, worker := c.Services["relayhub-api"], c.Services["relayhub-worker"]
	if api["image"] != worker["image"] {
		t.Fatal("image mismatch")
	}
	for name, command := range map[string]string{"relayhub-api": "api", "relayhub-worker": "worker"} {
		v, _ := c.Services[name]["command"].([]any)
		if len(v) != 1 || v[0] != command {
			t.Fatal("wrong runtime command")
		}
	}
	postgres := c.Services["relayhub-postgres"]
	pg := string(mustYAML(t, postgres))
	for _, want := range []string{"postgres:17-alpine", "POSTGRES_DB", "POSTGRES_USER", "RELAYHUB_POSTGRES_PASSWORD:?", "relayhub-postgres-data:/var/lib/postgresql/data"} {
		if !strings.Contains(pg, want) {
			t.Fatalf("PostgreSQL missing %s", want)
		}
	}
	redis := string(mustYAML(t, c.Services["relayhub-redis"]))
	for _, want := range []string{"redis:7.4-alpine", "sh", "exec redis-server", "--appendonly", "no", "--requirepass", "REDIS_PASSWORD", "RELAYHUB_REDIS_PASSWORD:?", "CMD-SHELL", "redis-cli", "PONG"} {
		if !strings.Contains(redis, want) {
			t.Fatalf("Redis missing %s", want)
		}
	}
	if strings.Contains(redis, "ports:") || strings.Contains(redis, "volumes:") {
		t.Fatal("Redis must stay private and ephemeral")
	}
	for _, runtime := range []map[string]any{api, worker} {
		dependencies := string(mustYAML(t, runtime["depends_on"]))
		if !strings.Contains(dependencies, "relayhub-redis") || !strings.Contains(dependencies, "service_healthy") {
			t.Fatal("runtime must wait for healthy Redis")
		}
	}
	env := string(read(".env.example"))
	config := string(read("backend/internal/config/config.go"))
	// Every supported config setting has an operator-visible example.
	for _, key := range regexp.MustCompile(`RELAYHUB_[A-Z_]+`).FindAllString(config, -1) {
		if !strings.Contains(env, key+"=") {
			t.Fatalf("missing %s", key)
		}
	}

	for _, key := range []string{"RELAYHUB_ADMIN_TOKEN", "RELAYHUB_SIGNING_SECRET", "RELAYHUB_POSTGRES_PASSWORD", "RELAYHUB_SECRET_ENCRYPTION_KEY", "RELAYHUB_NATS_USERNAME", "RELAYHUB_NATS_PASSWORD", "RELAYHUB_REDIS_PASSWORD"} {
		if !strings.Contains(env, key+"=\n") {
			t.Fatal("example must have empty required credentials")
		}
	}
	dockerignore := string(read(".dockerignore"))
	for _, want := range []string{"*", "!.dockerignore", "!backend/**", "!web/admin/**", "!web/docs/**", "!docs/developer/streaming-protocol.md", ".git/**", "**/node_modules/**", "**/dist/**"} {
		if !strings.Contains(dockerignore, want) {
			t.Fatalf("dockerignore missing %s", want)
		}
	}
	docker := string(read("Dockerfile"))
	if strings.Contains(docker, "COPY . .") || strings.Contains(docker, ".github") {
		t.Fatal("build must not copy private .env, GitHub workflow or workspace files")
	}
	for _, want := range []string{"TARGETARCH", "TARGETOS", "CGO_ENABLED=0", "distroless/static", "USER 65532:65532", "check-contracts.sh", "docs/developer/streaming-protocol.md"} {
		if !strings.Contains(docker, want) {
			t.Fatalf("image missing %s", want)
		}
	}
}
func mustYAML(t *testing.T, v any) []byte {
	t.Helper()
	b, e := yaml.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
