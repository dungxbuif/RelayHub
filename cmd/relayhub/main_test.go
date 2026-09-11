package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

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
	root := filepath.Join("..", "..")
	read := func(path string) []byte {
		t.Helper()
		b, e := os.ReadFile(filepath.Join(root, path))
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	raw := read("compose.yaml")
	public := read("public-docs/deploy/docker-compose.relayhub.yml")
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
	if len(c.Services) != 4 || len(c.Networks) != 1 || c.Networks["relayhub"] == nil {
		t.Fatal("expected API, worker, Redis, NATS and project network")
	}
	if _, ok := c.Volumes["relayhub-data"]; !ok {
		t.Fatal("missing AOF volume")
	}
	if _, ok := c.Volumes["relayhub-nats-data"]; !ok {
		t.Fatal("missing JetStream volume")
	}
	for _, name := range []string{"relayhub-api", "relayhub-worker", "relayhub-redis", "relayhub-nats"} {
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
		if s["user"] == nil || s["user"] == "0:0" || s["read_only"] != true {
			t.Fatalf("%s must be nonroot readonly", name)
		}
		if !strings.Contains(string(mustYAML(t, s["cap_drop"])), "ALL") || !strings.Contains(string(mustYAML(t, s["security_opt"])), "no-new-privileges") {
			t.Fatal("missing security restrictions")
		}
		if s["stop_grace_period"] == nil || s["healthcheck"] == nil {
			t.Fatal("missing lifecycle configuration")
		}
		if name == "relayhub-api" || name == "relayhub-worker" {
			if !strings.Contains(string(mustYAML(t, s["depends_on"])), "service_healthy") {
				t.Fatal("dependency not healthy")
			}
			if !strings.Contains(string(mustYAML(t, s["healthcheck"])), "healthcheck") {
				t.Fatal("probe must be binary")
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
	redis := c.Services["relayhub-redis"]
	r := string(mustYAML(t, redis))
	for _, want := range []string{"redis:7", "appendonly", "appendfsync", "requirepass", "RELAYHUB_REDIS_PASSWORD:?", "relayhub-data:/data"} {
		if !strings.Contains(r, want) {
			t.Fatalf("Redis missing %s", want)
		}
	}
	env := string(read(".env.example"))
	config := string(read("internal/config/config.go"))
	// Every supported config setting has an operator-visible example.
	for _, key := range regexp.MustCompile(`RELAYHUB_[A-Z_]+`).FindAllString(config, -1) {
		if !strings.Contains(env, key+"=") {
			t.Fatalf("missing %s", key)
		}
	}

	for _, key := range []string{"RELAYHUB_ADMIN_TOKEN", "RELAYHUB_SIGNING_SECRET", "RELAYHUB_REDIS_PASSWORD", "RELAYHUB_NATS_USERNAME", "RELAYHUB_NATS_PASSWORD"} {
		if !strings.Contains(env, key+"=\n") {
			t.Fatal("example must have empty required credentials")
		}
	}
	docker := string(read("Dockerfile"))
	if strings.Contains(docker, "COPY . .") {
		t.Fatal("build must not copy private .env or workspace files")
	}
	for _, want := range []string{"TARGETARCH", "TARGETOS", "CGO_ENABLED=0", "distroless/static", "USER 65532:65532", "check-contracts.sh"} {
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

func TestCIContract(t *testing.T) {
	b, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow map[string]any
	if err := yaml.Unmarshal(b, &workflow); err != nil {
		t.Fatal(err)
	}
	raw := string(b)
	for _, gate := range []string{"python3 scripts/test-docs-runtime.py", "go test -race ./scripts/e2e-client.go ./scripts/e2e-client_test.go", "./scripts/e2e.sh --backup-rehearsal", "RELAYHUB_DOCS_TEST_REDIS_URL:", "pull_request:", "go vet ./...", "go test ./...", "go test -race ./...", "-tags=integration", "RELAYHUB_TEST_REDIS_URL:", "redis:7-alpine", "check-contracts.sh --self-test", "docker build", "docker compose config --quiet", "./scripts/e2e.sh", "timeout-minutes:", "gofmt -l ."} {
		if !strings.Contains(raw, gate) {
			t.Fatalf("CI misses %s", gate)
		}
	}
	if strings.Contains(raw, "secrets.") || strings.Contains(raw, "continue-on-error: true") {
		t.Fatal("PR gates cannot depend on secrets or skip failures")
	}
}
