package relayhub

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var fixture struct {
	Frames   []json.RawMessage `json:"frames"`
	Input    EventInput        `json:"event_input"`
	Response HTTPResponse      `json:"http_response"`
}
var root string
var integrationOnce sync.Once
var integrationErr error
var stopIntegration = func() {}
var server struct {
	URL          string `json:"url"`
	Endpoint     string `json:"endpoint"`
	AgentID      string `json:"agent_id"`
	AppToken     string `json:"app_token"`
	AgentToken   string `json:"agent_token"`
	OtherToken   string `json:"other_token"`
	SiblingToken string `json:"sibling_token"`
}

func TestMain(m *testing.M) {
	root, _ = filepath.Abs("../../..")
	b, err := os.ReadFile(filepath.Join(root, "contracts/v1/fixtures.json"))
	if err == nil {
		err = json.Unmarshal(b, &fixture)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "fixture setup:", err)
		os.Exit(1)
	}
	code := m.Run()
	stopIntegration()
	os.Exit(code)
}
func testID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func startIntegration() error {
	temp, err := os.MkdirTemp("", "relayhub-go-test-")
	if err != nil {
		return err
	}
	namespace := "relayhub_go_test_" + strings.ReplaceAll(testID(), "-", "")
	compose := filepath.Join(root, "rust/tests/compose.yaml")
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "docker", "compose", "--env-file", "/dev/null", "-f", compose, "-p", namespace, "down", "--volumes", "--remove-orphans", "--timeout", "1").Run()
		_ = os.RemoveAll(temp)
	}
	stopIntegration = cleanup
	read := func(path string) (string, error) { b, e := os.ReadFile(filepath.Join(root, path)); return string(b), e }
	node, err := read("sdks/remake/node/test/acceptance.test.ts")
	if err != nil {
		return err
	}
	_, bridge, ok := strings.Cut(node, "const bridge = String.raw`\n")
	if !ok {
		return errors.New("Node Rust adapter missing")
	}
	bridge, _, ok = strings.Cut(bridge, "\n`;\n")
	if !ok {
		return errors.New("Node Rust adapter terminator missing")
	}
	manifest, err := read("rust/Cargo.toml")
	if err != nil {
		return err
	}
	manifest = strings.Replace(manifest, `name = "relayhub"`, `name = "relayhub-go-contract-adapter"`, 1)
	manifest = strings.Replace(manifest, "[dependencies]", fmt.Sprintf("[dependencies]\nrelayhub = { path = %q }", filepath.Join(root, "rust")), 1)
	support, err := read("rust/tests/support/mod.rs")
	if err != nil {
		return err
	}
	support = strings.ReplaceAll(support, `concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml")`, fmt.Sprintf("%q", compose))
	support = strings.Replace(support, `format!("relayhub_rust_test_{}", uuid::Uuid::new_v4().simple())`, fmt.Sprintf("%q.to_owned()", namespace), 1)
	bridge = strings.NewReplacer("__SUPPORT__", filepath.Join(temp, "support.rs"), "__MIGRATION__", filepath.Join(root, "rust/migrations/0004_endpoints.sql"), "__COMPOSE__", compose).Replace(bridge)
	// Observe heartbeat renewal in PostgreSQL without implementing lease policy here.
	bridge = strings.Replace(bridge, "SELECT d.state,d.attempt_count", "SELECT EXTRACT(EPOCH FROM (d.lease_expires_at-clock_timestamp()))::double precision AS remaining,d.state,d.attempt_count", 1)
	bridge = strings.Replace(bridge, `{"state":row.get`, `{"remaining":row.get::<Option<f64>,_>("remaining"),"state":row.get`, 1)
	lock, err := read("rust/Cargo.lock")
	if err != nil {
		return err
	}
	if err = os.Mkdir(filepath.Join(temp, "src"), 0700); err != nil {
		return err
	}
	for p, s := range map[string]string{"Cargo.toml": manifest, "Cargo.lock": lock, "support.rs": support, "src/main.rs": bridge} {
		if err = os.WriteFile(filepath.Join(temp, p), []byte(s), 0600); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	target := filepath.Join(root, "rust/target")
	build := exec.CommandContext(ctx, "cargo", "build", "--offline", "--manifest-path", filepath.Join(temp, "Cargo.toml"), "--target-dir", target)
	if out, e := build.CombinedOutput(); e != nil {
		return fmt.Errorf("Rust adapter build: %w\n%s", e, out)
	}
	child := exec.Command(filepath.Join(target, "debug/relayhub-go-contract-adapter"))
	stdout, err := child.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := os.Create(filepath.Join(temp, "adapter.stderr"))
	if err != nil {
		return err
	}
	child.Stderr = stderr
	if err = child.Start(); err != nil {
		stderr.Close()
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	stopIntegration = func() {
		_ = child.Process.Signal(os.Interrupt)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			_ = child.Process.Kill()
			<-exited
		}
		stderr.Close()
		cleanup()
	}
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if !scanner.Scan() {
			ready <- errors.New("Rust adapter failed before readiness")
			return
		}
		ready <- json.Unmarshal(scanner.Bytes(), &server)
	}()
	select {
	case err = <-ready:
		return err
	case <-time.After(90 * time.Second):
		return errors.New("Rust adapter readiness timeout")
	}
}
func integration(t *testing.T) {
	t.Helper()
	integrationOnce.Do(func() { integrationErr = startIntegration() })
	if integrationErr != nil {
		t.Fatal(integrationErr)
	}
}
func mustClient(t *testing.T, agent bool, suffix, token string) *Client {
	t.Helper()
	integration(t)
	o := Options{}
	if token == "" {
		token = server.AppToken
	}
	if agent {
		o.AgentID = server.AgentID
		if token == server.AppToken {
			token = server.AgentToken
		}
	}
	c, err := NewClient(server.URL+suffix, token, o)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func input() EventInput { v := fixture.Input; v.EndpointID = server.Endpoint; return v }

var testHTTP = &http.Client{Timeout: 5 * time.Second}

func control(t *testing.T, action, id string, out any) {
	t.Helper()
	route := server.URL + "/test/" + action
	if id != "" {
		route += "?id=" + id
	}
	r, err := testHTTP.Post(route, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("test control status %d", r.StatusCode)
	}
	if out != nil {
		if err = json.NewDecoder(r.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

type row struct {
	State      string        `json:"state"`
	Remaining  *float64      `json:"remaining"`
	Attempt    int           `json:"attempt"`
	Generation int           `json:"generation"`
	Terminal   bool          `json:"terminal"`
	Expired    int           `json:"expired"`
	Input      EventInput    `json:"input"`
	Response   *HTTPResponse `json:"response"`
}

func state(t *testing.T, id string) row { t.Helper(); var r row; control(t, "state", id, &r); return r }
func awaitState(t *testing.T, id, want string) row {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		r := state(t, id)
		if r.State == want {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("delivery did not reach %s: %+v", want, state(t, id))
	return row{}
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(12 * time.Second):
		t.Fatal("observable condition timed out")
		var zero T
		return zero
	}
}
func publish(t *testing.T) Accepted {
	t.Helper()
	c := mustClient(t, false, "", "")
	a, err := c.Publish(context.Background(), input(), testID())
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func runAgent(t *testing.T, c *Client, h func(context.Context, Delivery) (ProcessingResult, error)) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.RunAgent(ctx, h) }()
	t.Cleanup(cancel)
	return cancel, done
}
func stopAgent(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error: %v", err)
	}
}
func success(context.Context, Delivery) (ProcessingResult, error) { return ProcessingResult{}, nil }
func status(t *testing.T, err error, want int) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Status != want {
		t.Fatalf("error %v, want HTTP status %d", err, want)
	}
}
func equalJSON(t *testing.T, a, b []byte) {
	t.Helper()
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil || !reflect.DeepEqual(x, y) {
		t.Fatalf("JSON differs: %s / %s", a, b)
	}
}

func TestGolden(t *testing.T) {
	t.Run("golden_frames_match", func(t *testing.T) {
		for _, wire := range fixture.Frames {
			f, err := DecodeFrame(wire)
			if err != nil {
				t.Fatal(err)
			}
			b, err := EncodeFrame(f)
			if err != nil {
				t.Fatal(err)
			}
			equalJSON(t, b, wire)
			if d, ok := f.(*Delivery); ok {
				if !bytes.Equal(d.Input.Body, []byte{0, 255, 127, 128}) || !reflect.DeepEqual(d.Input.Headers, Headers{{"X-Tag", "one"}, {"X-Tag", "two"}}) {
					t.Fatal("binary/duplicate headers changed")
				}
			}
		}
	})
}
func TestAgentSuccess(t *testing.T) {
	t.Run("handler_success_acks", func(t *testing.T) {
		c := mustClient(t, true, "/actual", "")
		a := publish(t)
		seen := make(chan Delivery, 1)
		release := make(chan struct{})
		defer close(release)
		cancel, done := runAgent(t, c, func(ctx context.Context, d Delivery) (ProcessingResult, error) {
			seen <- d
			select {
			case <-release:
			case <-ctx.Done():
			}
			d.Lease.EventID = testID()
			return ProcessingResult{}, nil
		})
		d := receive(t, seen)
		if d.Lease.EventID != a.EventID || d.Lease.DeliveryID != a.DeliveryID || d.Lease.Generation != 1 || d.Lease.Attempt != 1 || !reflect.DeepEqual(d.Input, input()) {
			t.Fatal("delivery metadata/input changed")
		}
		if r := state(t, a.DeliveryID); r.State != "in_flight" || r.Terminal {
			t.Fatal("settled before handler completion")
		}
		release <- struct{}{}
		r := awaitState(t, a.DeliveryID, "succeeded")
		if !r.Terminal || r.Response != nil {
			t.Fatal("ACK did not settle exactly as success")
		}
		stopAgent(t, cancel, done)
	})
}
func TestAgentError(t *testing.T) {
	t.Run("handler_error_retries", func(t *testing.T) {
		c := mustClient(t, true, "", "")
		a := publish(t)
		called := make(chan struct{}, 1)
		cancel, done := runAgent(t, c, func(context.Context, Delivery) (ProcessingResult, error) {
			called <- struct{}{}
			return ProcessingResult{}, errors.New("private-handler-error")
		})
		receive(t, called)
		stopAgent(t, cancel, done)
		r := state(t, a.DeliveryID)
		if r.State != "in_flight" || r.Terminal || r.Response != nil {
			t.Fatal("handler error settled work")
		}
		control(t, "recover", a.DeliveryID, nil)
		r = state(t, a.DeliveryID)
		if r.State != "retry_wait" || r.Expired != 1 {
			t.Fatal("server retry state missing")
		}
		control(t, "due", a.DeliveryID, nil)
		cancel, done = runAgent(t, c, success)
		r = awaitState(t, a.DeliveryID, "succeeded")
		if r.Attempt != 2 {
			t.Fatal("retry attempt missing")
		}
		stopAgent(t, cancel, done)
	})
}
func TestAgentReconnect(t *testing.T) {
	t.Run("reconnect_redelivers_same_event_id", func(t *testing.T) {
		c := mustClient(t, true, "", "")
		a := publish(t)
		seen := make(chan Delivery, 2)
		release := make(chan struct{})
		defer close(release)
		canceled := make(chan struct{}, 1)
		cancel, done := runAgent(t, c, func(ctx context.Context, d Delivery) (ProcessingResult, error) {
			seen <- d
			if d.Lease.Attempt == 1 {
				<-ctx.Done()
				canceled <- struct{}{}
				<-release
			}
			return ProcessingResult{}, nil
		})
		first := receive(t, seen)
		control(t, "disconnect", "", nil)
		receive(t, canceled)
		control(t, "recover", a.DeliveryID, nil)
		control(t, "due", a.DeliveryID, nil)
		second := receive(t, seen)
		if first.Lease.EventID != a.EventID || second.Lease.EventID != first.Lease.EventID || second.Lease.DeliveryID != first.Lease.DeliveryID || second.Lease.Generation != first.Lease.Generation || second.Lease.Attempt != 2 || second.Lease.AttemptToken == first.Lease.AttemptToken {
			t.Fatal("redelivery identity/fencing changed")
		}
		r := awaitState(t, a.DeliveryID, "succeeded")
		if r.Expired != 1 {
			t.Fatal("expired attempt not persisted")
		}
		release <- struct{}{}
		stopAgent(t, cancel, done)
	})
}
func TestAgentCancel(t *testing.T) {
	t.Run("context_cancel_drains", func(t *testing.T) {
		c := mustClient(t, true, "/actual", "")
		a := publish(t)
		seen := make(chan struct{}, 2)
		canceled := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		cancel, done := runAgent(t, c, func(ctx context.Context, d Delivery) (ProcessingResult, error) {
			seen <- struct{}{}
			<-ctx.Done()
			close(canceled)
			<-release
			return ProcessingResult{}, nil
		})
		receive(t, seen)
		cancel()
		receive(t, canceled)
		later := publish(t)
		time.Sleep(1200 * time.Millisecond)
		select {
		case <-done:
			t.Fatal("did not drain active handler")
		case <-seen:
			t.Fatal("admitted handler after cancellation")
		default:
		}
		// Frozen Gateway keeps claiming while the socket remains open to settle active work.
		laterRow := state(t, later.DeliveryID)
		if laterRow.Terminal {
			t.Fatal("unhandled work became terminal")
		}
		t.Logf("post-cancel Gateway state=%s attempt=%d (no drain frame exists)", laterRow.State, laterRow.Attempt)
		release <- struct{}{}
		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		awaitState(t, a.DeliveryID, "succeeded")
		if laterRow.State == "in_flight" {
			control(t, "recover", later.DeliveryID, nil)
			control(t, "due", later.DeliveryID, nil)
		}
		cancel, done = runAgent(t, c, success)
		awaitState(t, later.DeliveryID, "succeeded")
		stopAgent(t, cancel, done)
	})
}
func TestResponse(t *testing.T) {
	c := mustClient(t, true, "", "")
	a := publish(t)
	cancel, done := runAgent(t, c, func(context.Context, Delivery) (ProcessingResult, error) {
		r := fixture.Response
		return ProcessingResult{Response: &r}, nil
	})
	r := awaitState(t, a.DeliveryID, "succeeded")
	if !reflect.DeepEqual(r.Response, &fixture.Response) || r.Attempt != 1 {
		t.Fatal("typed response not persisted")
	}
	stopAgent(t, cancel, done)
}
func TestPublish(t *testing.T) {
	c := mustClient(t, false, "", "")
	var before, after []int
	control(t, "counts", "", &before)
	key := testID()
	a, err := c.Publish(context.Background(), input(), key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Publish(context.Background(), input(), key)
	if err != nil || a != b || a.Status != 202 {
		t.Fatal("idempotency receipt mismatch")
	}
	r := state(t, a.DeliveryID)
	if r.State != "pending" || r.Attempt != 0 || !reflect.DeepEqual(r.Input, input()) {
		t.Fatal("durable acceptance missing")
	}
	changed := input()
	changed.Body = []byte("changed")
	_, err = c.Publish(context.Background(), changed, key)
	status(t, err, 409)
	_, err = mustClient(t, false, "", server.OtherToken).Publish(context.Background(), input(), testID())
	status(t, err, 403)
	_, err = mustClient(t, false, "", "invalid-test-token").Publish(context.Background(), input(), testID())
	status(t, err, 401)
	changed = input()
	changed.Body = make([]byte, 1048577)
	_, err = c.Publish(context.Background(), changed, testID())
	status(t, err, 413)
	for _, key := range []string{"", " key", "key ", "key\n", strings.Repeat("k", 257)} {
		_, err = c.Publish(context.Background(), input(), key)
		status(t, err, 422)
	}
	control(t, "counts", "", &after)
	for i := range before {
		if after[i] != before[i]+1 {
			t.Fatal("rejected publish committed work")
		}
	}
	cancel, done := runAgent(t, mustClient(t, true, "", ""), success)
	awaitState(t, a.DeliveryID, "succeeded")
	stopAgent(t, cancel, done)
}
func TestRealtime(t *testing.T) {
	t.Run("race_free_subscription_close", func(t *testing.T) {
		c := mustClient(t, false, "", "")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var before, after []int
		control(t, "counts", "", &before)
		r, err := c.PublishRealtime(ctx, "orders.updated", []byte("past"))
		if err != nil || r.Status != 202 || r.ObservedRecipients != 0 {
			t.Fatal("offline realtime receipt", err)
		}
		sub, err := c.Subscribe(ctx, "orders.updated")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		other, err := mustClient(t, false, "", server.OtherToken).Subscribe(ctx, "orders.updated")
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		sibling, err := mustClient(t, false, "", server.SiblingToken).Subscribe(ctx, "orders.updated")
		if err != nil {
			t.Fatal(err)
		}
		defer sibling.Close()
		time.Sleep(100 * time.Millisecond)
		select {
		case <-sub.Messages():
			t.Fatal("replayed history")
		default:
		}
		r, err = c.PublishRealtime(ctx, "orders.updated", []byte{0, 255, 127, 128})
		if err != nil || r.ObservedRecipients != 1 {
			t.Fatal("scoped receipt", err)
		}
		message := receive(t, sub.Messages())
		if !bytes.Equal(message.Body, []byte{0, 255, 127, 128}) {
			t.Fatal("binary realtime changed")
		}
		time.Sleep(100 * time.Millisecond)
		select {
		case <-other.Messages():
			t.Fatal("foreign workspace message")
		case <-sibling.Messages():
			t.Fatal("sibling app message")
		default:
		}
		_, err = c.Subscribe(ctx, "orders.*")
		status(t, err, 422)
		_, err = c.PublishRealtime(ctx, "orders.updated", make([]byte, 1048577))
		status(t, err, 413)
		var wg sync.WaitGroup
		for i := 0; i < 24; i++ {
			wg.Go(func() { _ = sub.Close(); _ = sub.Err() })
		}
		wg.Go(cancel)
		wg.Go(func() { _, _ = c.PublishRealtime(context.Background(), "orders.updated", []byte("concurrent")) })
		wg.Wait()
		receive(t, sub.Done())
		for range sub.Messages() {
		}
		again, err := c.Subscribe(context.Background(), "orders.updated")
		if err != nil {
			t.Fatal(err)
		}
		defer again.Close()
		time.Sleep(100 * time.Millisecond)
		select {
		case <-again.Messages():
			t.Fatal("replayed after reconnect")
		default:
		}
		control(t, "counts", "", &after)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("realtime persisted durable work")
		}
	})
}
func TestStrictFrames(t *testing.T) {
	var original map[string]any
	_ = json.Unmarshal(fixture.Frames[1], &original)
	mutate := func(fn func(map[string]any)) []byte {
		var v map[string]any
		_ = json.Unmarshal(fixture.Frames[1], &v)
		fn(v)
		b, _ := json.Marshal(v)
		return b
	}
	invalid := [][]byte{
		mutate(func(v map[string]any) { v["version"] = 2 }), mutate(func(v map[string]any) { v["workspace_id"] = testID() }), mutate(func(v map[string]any) { v["type"] = "nack" }),
		mutate(func(v map[string]any) { v["lease"].(map[string]any)["generation"] = 0 }), mutate(func(v map[string]any) { v["lease"].(map[string]any)["attempt"] = 6 }),
		mutate(func(v map[string]any) { delete(v["input"].(map[string]any), "event_type") }), mutate(func(v map[string]any) { v["input"].(map[string]any)["body"] = "AP9/gB==" }),
		mutate(func(v map[string]any) { v["input"].(map[string]any)["body"] = nil }), mutate(func(v map[string]any) { v["input"].(map[string]any)["path"] = "/bad%ZZ" }),
		mutate(func(v map[string]any) {
			v["input"].(map[string]any)["headers"] = [][]string{{"Name", "value", "extra"}}
		}),
		[]byte(`{"version":1,"request_id":"00000000-0000-4000-8000-000000000001","type":"heartbeat"}`),
	}
	for _, b := range invalid {
		if _, err := DecodeFrame(b); err == nil {
			t.Fatalf("accepted malformed frame: %s", b)
		}
	}
	f, err := DecodeFrame(fixture.Frames[1])
	if err != nil {
		t.Fatal(err)
	}
	d := f.(*Delivery)
	d.Input.Headers = Headers{{"X-Label", "こんにちは\t"}}
	d.Input.Body = make([]byte, 1048576)
	if _, err = EncodeFrame(d); err != nil {
		t.Fatal("valid boundary rejected", err)
	}
	d.Input.Body = append(d.Input.Body, 0)
	_, err = EncodeFrame(d)
	status(t, err, 413)
	d.Input.Body = nil
	d.Input.Headers = nil
	b, err := EncodeFrame(d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"body":""`)) || !bytes.Contains(b, []byte(`"headers":[]`)) {
		t.Fatal("empty bytes/headers must not become null")
	}
	for _, v := range []string{"\x00", "\x08", "\x0b", "\x0c", "\x1f", "\x7f"} {
		d.Input.Headers = Headers{{"X", v}}
		if _, err = EncodeFrame(d); err == nil {
			t.Fatal("accepted control header")
		}
	}
	_, err = DecodeFrame(bytes.Repeat([]byte{' '}, 2097153))
	status(t, err, 413)
}
func TestCredentialsAndTransport(t *testing.T) {
	var requests atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "https://example.test", 307)
	}))
	defer peer.Close()
	secret := "synthetic-private-token"
	c, err := NewClient(peer.URL, secret, Options{})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(c)
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), string(b)} {
		if strings.Contains(s, secret) {
			t.Fatal("credential exposed")
		}
	}
	for _, key := range []string{" a", "a ", "\ta"} {
		_, err = c.Publish(context.Background(), fixture.Input, key)
		status(t, err, 422)
	}
	if requests.Load() != 0 {
		t.Fatal("invalid key reached HTTP")
	}
	_, err = c.Publish(context.Background(), fixture.Input, "valid")
	status(t, err, 307)
	if requests.Load() != 1 {
		t.Fatal("redirect/retry followed")
	}
	for _, u := range []string{"http://example.test", "https://user:pass@example.test", "https://example.test?token=bad", "https://example.test#bad"} {
		if _, err = NewClient(u, secret, Options{}); err == nil {
			t.Fatal("unsafe URL accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Publish(ctx, fixture.Input, "valid")
	if !errors.Is(err, context.Canceled) {
		t.Fatal("HTTP ignored cancellation", err)
	}
}

func TestNullHeaderValueRejected(t *testing.T) {
	var v map[string]any
	_ = json.Unmarshal(fixture.Frames[1], &v)
	v["input"].(map[string]any)["headers"] = [][]any{{"X-Name", nil}}
	b, _ := json.Marshal(v)
	if _, err := DecodeFrame(b); err == nil {
		t.Fatal("null header value silently became an empty string")
	}
}

func TestCancellationDeadlineAndHeartbeat(t *testing.T) {
	c := mustClient(t, true, "/actual", "")
	a := publish(t)
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	defer close(release)
	cancel, done := runAgent(t, c, func(ctx context.Context, d Delivery) (ProcessingResult, error) {
		defer close(exited)
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release // An uncooperative handler must not hold RunAgent past the budget.
		return ProcessingResult{}, nil
	})
	receive(t, started)
	start := time.Now()
	cancel()
	receive(t, canceled)
	time.Sleep(21 * time.Second)
	r := state(t, a.DeliveryID)
	if r.Remaining == nil || *r.Remaining < 45 {
		t.Fatal("20-second heartbeat did not renew active lease during drain")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(11 * time.Second):
		t.Fatal("RunAgent exceeded 30-second shutdown budget")
	}
	if elapsed := time.Since(start); elapsed < 29*time.Second || elapsed > 32*time.Second {
		t.Fatalf("shutdown duration %s", elapsed)
	}
	r = state(t, a.DeliveryID)
	if r.Terminal || r.State != "in_flight" || r.Attempt != 1 {
		t.Fatal("unfinished handler was acknowledged")
	}
	control(t, "recover", a.DeliveryID, nil)
	control(t, "due", a.DeliveryID, nil)
	cancel, done = runAgent(t, c, success)
	r = awaitState(t, a.DeliveryID, "succeeded")
	if r.Attempt != 2 || r.Expired != 1 {
		t.Fatal("timed-out work was not recoverable")
	}
	release <- struct{}{}
	receive(t, exited)
	stopAgent(t, cancel, done)
}

// Keep the quota test last because the disposable server's quota stays saturated.
func TestQuota(t *testing.T) {
	c := mustClient(t, false, "", "")
	var before, after []int
	control(t, "counts", "", &before)
	control(t, "quota", server.Endpoint, nil)
	_, err := c.Publish(context.Background(), input(), testID())
	status(t, err, 429)
	control(t, "counts", "", &after)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("quota rejection committed")
	}
}
