//go:build ignore

// An independent black-box client: no RelayHub internal imports or signing helpers.
// Application credentials, signatures, socket tokens and payloads remain in memory.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type object = map[string]any
type credential struct {
	ID     string `json:"app_id"`
	Key    string `json:"api_key"`
	Secret string `json:"hmac_secret"`
}
type publication struct {
	Event json.RawMessage `json:"event"`
	Jobs  []job           `json:"jobs"`
}
type job struct {
	ID               string `json:"id"`
	EventID          string `json:"event_id"`
	Target           string `json:"target_app_id"`
	Status           string `json:"status"`
	Attempts         int    `json:"attempts"`
	CallbackAttempts int    `json:"callback_attempts"`
}
type response struct {
	Status int
	Header http.Header
	Body   []byte
}
type suite struct {
	expected                   map[string]string
	ctx                        context.Context
	base, project, temp, stage string
	env, forbidden             []string
	mu                         sync.Mutex
	http                       *http.Client
	callbacks                  *callbackServer
}

func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func random() string {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	require(err == nil, "random source failed")
	return hex.EncodeToString(b)
}
func decode[T any](b []byte) T {
	var v T
	require(json.Unmarshal(b, &v) == nil, "invalid JSON response")
	return v
}
func encode(v any) []byte {
	if v == nil {
		return nil
	}
	b, e := json.Marshal(v)
	require(e == nil, "request encoding failed")
	return b
}

// Expected canonical vector is published in docs; this independently assembled
// formula is also used against exact received callback bytes, never server helpers.
func signature(secret, timestamp, method, target string, body []byte) string {
	digest := sha256.Sum256(body)
	canonical := strings.Join([]string{timestamp, method, target, hex.EncodeToString(digest[:])}, "\n")
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *suite) hide(values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forbidden = append(s.forbidden, values...)
}

// runCommand bounds both process lifetime and inherited stdout/stderr pipes.
// Docker Compose plugins share this fresh process group; a dead CLI must not
// leave a child holding CombinedOutput pipes forever or block deferred cleanup.
func runCommand(ctx context.Context, env []string, input io.Reader, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.Stdin = input
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 250 * time.Millisecond
	stopGroup := func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		group := -cmd.Process.Pid
		if err := syscall.Kill(group, syscall.SIGTERM); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
		// Always follow TERM with a group KILL: the CLI can exit while its child
		// ignores TERM and continues to own inherited pipe descriptors.
		time.Sleep(150 * time.Millisecond)
		err := syscall.Kill(group, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	cmd.Cancel = stopGroup
	output, err := cmd.CombinedOutput()
	// WaitDelay can expire after a successful parent exit before context expiry.
	// Kill any remaining descendants in that case as well.
	if err != nil {
		_ = stopGroup()
	}
	return output, err
}
func (s *suite) compose(args ...string) []byte {
	ctx, cancel := context.WithTimeout(s.ctx, 8*time.Minute)
	defer cancel()
	argv := []string{"compose", "--project-name", s.project, "--project-directory", ".", "--env-file", filepath.Join(s.temp, "empty.env"), "-f", "compose.yaml", "-f", filepath.Join(s.temp, "override.yaml")}
	out, err := runCommand(ctx, s.env, nil, "docker", append(argv, args...)...)
	require(err == nil, "Compose operation failed (output withheld to protect secrets)")
	return out
}
func (s *suite) docker(args ...string) []byte {
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	out, err := runCommand(ctx, nil, nil, "docker", args...)
	require(err == nil, "Docker inspection failed")
	return out
}
func (s *suite) request(c *credential, admin bool, method, target string, body []byte, key string) response {
	req, e := http.NewRequestWithContext(s.ctx, method, s.base+target, bytes.NewReader(body))
	require(e == nil, "request construction failed")
	req.Header.Set("Content-Type", "application/json")
	if admin {
		req.Header.Set("Authorization", "Bearer "+s.admin())
	}
	if c != nil {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		sig := signature(c.Secret, timestamp, method, target, body)
		s.hide(sig)
		req.Header.Set("X-RelayHub-Api-Key", c.Key)
		req.Header.Set("X-RelayHub-Timestamp", timestamp)
		req.Header.Set("X-RelayHub-Signature", sig)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	r, err := s.http.Do(req)
	require(err == nil, "HTTP transport failed")
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	require(err == nil, "response read failed")
	return response{r.StatusCode, r.Header, raw}
}
func (s *suite) admin() string {
	for _, v := range s.env {
		if strings.HasPrefix(v, "RELAYHUB_ADMIN_TOKEN=") {
			return strings.TrimPrefix(v, "RELAYHUB_ADMIN_TOKEN=")
		}
	}
	panic("admin not configured")
}
func (s *suite) call(c *credential, method, target string, value any, key string, want int) response {
	r := s.request(c, false, method, target, encode(value), key)
	require(r.Status == want, fmt.Sprintf("%s %s: expected %d, got %d", method, strings.Split(target, "?")[0], want, r.Status))
	return r
}
func (s *suite) create(name, mode, callback string) credential {
	body := object{"name": name, "delivery_mode": mode}
	if callback != "" {
		body["callback_url"] = callback
	}
	r := s.request(nil, true, "POST", "/api/v1/apps", encode(body), "")
	require(r.Status == 201, "admin create failed")
	c := decode[credential](r.Body)
	require(c.ID != "" && c.Key != "" && c.Secret != "", "missing application credentials")
	s.hide(c.Key, c.Secret)
	return c
}
func (s *suite) step(name string, fn func()) {
	s.stage = name
	fmt.Println("CHECK:", name)
	fn()
	fmt.Println("PASS:", name)
}
func (s *suite) poll(timeout time.Duration, fn func() bool) {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		select {
		case <-s.ctx.Done():
			panic("acceptance cancelled")
		case <-time.After(100 * time.Millisecond):
		}
	}
	panic("bounded assertion timed out")
}
func (s *suite) topology() {
	type container struct {
		Name  string
		State struct {
			Status string
			Health struct{ Status string }
		}
		Config struct {
			Labels map[string]string
			User   string
		}
		HostConfig struct {
			ReadonlyRootfs bool
			CapDrop        []string
			SecurityOpt    []string
			PortBindings   map[string][]object
		}
		NetworkSettings struct{ Networks map[string]any }
	}
	ids := strings.Fields(string(s.compose("ps", "-aq")))
	require(len(ids) == 3, "expected exactly three services")
	inspected := decode[[]container](s.docker(append([]string{"inspect"}, ids...)...))
	names := map[string]bool{}
	published := 0
	for _, c := range inspected {
		name := c.Config.Labels["com.docker.compose.service"]
		names[name] = true
		require(c.State.Status == "running" && c.State.Health.Status == "healthy", "service must be healthy")
		require(c.Config.User != "" && c.Config.User != "0" && c.HostConfig.ReadonlyRootfs, "container hardening absent")
		require(len(c.NetworkSettings.Networks) == 1 && c.NetworkSettings.Networks[s.project+"_relayhub"] != nil, "project network mismatch")
		for port, bindings := range c.HostConfig.PortBindings {
			if len(bindings) > 0 {
				require(name == "relayhub-api" && port == "8080/tcp", "non-API host port")
				published++
			}
		}
	}
	require(names["relayhub-api"] && names["relayhub-worker"] && names["relayhub-redis"] && published == 1, "service or publication inventory mismatch")
	for _, endpoint := range []string{"/healthz", "/readyz", "/metrics"} {
		r := s.call(nil, "GET", endpoint, nil, "", 200)
		if endpoint == "/metrics" {
			require(bytes.Contains(r.Body, []byte("# HELP")), "API metrics exposition absent")
		}
		s.compose("exec", "-T", "relayhub-worker", "/relayhub", "healthcheck", "http://127.0.0.1:9090"+endpoint)
	}
	// Distroless contains trusted CA roots and no shell; the API binary is the probe.
}
func (s *suite) publish(c *credential, targets []string, key, sentinel string) (publication, []byte) {
	body := encode(object{"type": "e2e.acceptance", "target_app_ids": targets, "data": object{"sentinel": sentinel, "integer": json.Number("9007199254740993")}})
	r := s.request(c, false, "POST", "/api/v1/events", body, key)
	require(r.Status == 202, "signed publication must return 202")
	p := decode[publication](r.Body)
	require(len(p.Jobs) == len(targets) && decode[object](p.Event)["type"] == "e2e.acceptance", "publication shape")
	return p, body
}
func (s *suite) current(c *credential, j job) job {
	return decode[job](s.call(c, "GET", "/api/v1/jobs/"+j.ID, nil, "", 200).Body)
}
func (s *suite) queueAck(c *credential, p publication) {
	r := s.call(c, "GET", "/api/v1/queue?limit=1&wait=0", nil, "", 200)
	items := decode[[]struct {
		Event json.RawMessage `json:"event"`
		Job   job             `json:"job"`
	}](r.Body)
	require(len(items) == 1 && items[0].Job.EventID == p.Jobs[0].EventID && items[0].Job.Status == "leased" && items[0].Job.Attempts == 1, "queue lease mismatch")
	for i := 0; i < 2; i++ {
		s.call(c, "POST", "/api/v1/events/"+p.Jobs[0].EventID+"/ack", nil, "", 204)
	}
	require(s.current(c, p.Jobs[0]).Status == "acked", "idempotent acknowledgement must reach acked")
}

type socket struct {
	conn   *websocket.Conn
	frames chan object
	closed chan error
	owner  string
	s      *suite
}

func (s *suite) connect(c *credential, topics []string) *socket {
	r := s.call(c, "POST", "/api/v1/socket/token", object{"scopes": []string{"ws:connect"}, "ttl_seconds": 600}, "", 201)
	token, _ := decode[object](r.Body)["token"].(string)
	require(token != "", "missing socket token")
	s.hide(token)
	endpoint := "ws" + strings.TrimPrefix(s.base, "http") + "/ws?token=" + url.QueryEscape(token)
	conn, _, err := websocket.DefaultDialer.DialContext(s.ctx, endpoint, nil)
	require(err == nil, "RFC 6455 handshake failed")
	require(conn.SetReadDeadline(time.Now().Add(5*time.Second)) == nil, "socket read deadline")
	var ready object
	require(conn.ReadJSON(&ready) == nil && ready["type"] == "ready" && ready["app_id"] == c.ID && ready["connection_id"] != "", "ready identity mismatch")
	require(conn.WriteJSON(object{"type": "subscribe", "topics": topics}) == nil, "subscribe write failed")
	var sub object
	require(conn.ReadJSON(&sub) == nil && sub["type"] == "subscribed" && bytes.Equal(encode(sub["topics"]), encode(topics)), "subscription acknowledgement mismatch")
	require(conn.SetReadDeadline(time.Now().Add(3*time.Minute)) == nil, "socket read deadline")
	w := &socket{conn: conn, frames: make(chan object, 64), closed: make(chan error, 1), owner: c.ID, s: s}
	go func() {
		for {
			var frame object
			if e := conn.ReadJSON(&frame); e != nil {
				w.closed <- e
				return
			}
			select {
			case w.frames <- frame:
			case <-s.ctx.Done():
				return
			}
		}
	}()
	return w
}
func (w *socket) next(kind string) object {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case frame := <-w.frames:
			if frame["type"] == kind {
				return frame
			}
		case <-w.closed:
			panic("WebSocket closed before expected frame")
		case <-timer.C:
			panic("WebSocket frame deadline")
		case <-w.s.ctx.Done():
			panic("acceptance cancelled")
		}
	}
}
func (w *socket) closeNormally() {
	require(w.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)) == nil, "normal close write")
	select {
	case err := <-w.closed:
		require(websocket.IsCloseError(err, websocket.CloseNormalClosure), "peer did not close normally")
	case <-time.After(3 * time.Second):
		panic("normal close deadline")
	}
	_ = w.conn.Close()
}
func (s *suite) functions(producer, owner *credential) {
	fn := decode[object](s.call(owner, "POST", "/api/v1/functions", object{"name": "e2e_calculate", "timeout_seconds": 1}, "", 201).Body)
	id, _ := fn["id"].(string)
	require(id != "", "function registration ID absent")
	s.expected["function_id"] = id
	target := "/api/v1/functions/" + id + "/invoke"
	offline := s.call(producer, "POST", target, object{"input": object{}}, "rpc-offline", 503)
	require(decode[object](offline.Body)["error"].(map[string]any)["code"] == "function_unavailable", "offline code")
	a, b := s.connect(owner, []string{"functions"}), s.connect(owner, []string{"functions"})
	// Independent readers record every invocation; exactly one handler may get it.
	var lock sync.Mutex
	received := map[string]int{}
	failures := make(chan string, 4)
	input := object{"a": 20, "b": 22, "sentinel": "function-input-" + random()}
	result := object{"value": 42, "sentinel": "function-result-" + random()}
	s.hide(string(encode(input)), input["sentinel"].(string), string(encode(result)), result["sentinel"].(string))
	stop := make(chan struct{})
	var handlers sync.WaitGroup
	for _, w := range []*socket{a, b} {
		handlers.Add(1)
		go func(w *socket) {
			defer handlers.Done()
			for {
				select {
				case frame := <-w.frames:
					if frame["type"] != "rpc.invoke" {
						failures <- "unexpected handler frame"
						return
					}
					inv, _ := frame["invocation_id"].(string)
					lock.Lock()
					received[inv]++
					lock.Unlock()
					if frame["function"] != "e2e_calculate" {
						failures <- "wrong function name"
						return
					}
					in, ok := frame["input"].(map[string]any)
					if !ok {
						failures <- "missing function input"
						return
					}
					if in["mode"] == "timeout" {
						continue
					}
					if !bytes.Equal(encode(in), encode(input)) {
						failures <- "function input mismatch"
						return
					}
					if w.conn.WriteJSON(object{"type": "rpc.result", "invocation_id": inv, "ok": true, "result": result}) != nil {
						failures <- "function reply failed"
						return
					}
				case <-stop:
					return
				case <-s.ctx.Done():
					return
				}
			}
		}(w)
	}
	successful := s.call(producer, "POST", target, object{"input": input}, "rpc-success", 200)
	got := decode[object](successful.Body)
	s.expected["invocation_id"], _ = got["invocation_id"].(string)
	s.expected["caller_app_id"] = producer.ID
	require(got["ok"] == true && bytes.Equal(encode(got["result"]), encode(result)), "RPC expected independent result")
	replay := s.call(producer, "POST", target, object{"input": object{"changed": true}}, "rpc-success", 200)
	require(replay.Header.Get("Idempotent-Replayed") == "true" && bytes.Equal(successful.Body, replay.Body), "RPC successful replay")
	again := s.call(producer, "POST", target, object{"input": input}, "rpc-offline", 503)
	require(again.Header.Get("Idempotent-Replayed") == "true" && bytes.Equal(offline.Body, again.Body), "offline replay must not redispatch")
	timed := s.call(producer, "POST", target, object{"input": object{"mode": "timeout"}}, "rpc-timeout", 504)
	require(decode[object](timed.Body)["error"].(map[string]any)["code"] == "function_timeout", "timeout code")
	replay = s.call(producer, "POST", target, object{"input": input}, "rpc-timeout", 504)
	require(replay.Header.Get("Idempotent-Replayed") == "true" && bytes.Equal(timed.Body, replay.Body), "timeout replay must not redispatch")
	time.Sleep(350 * time.Millisecond)
	lock.Lock()
	total := 0
	for _, count := range received {
		require(count == 1, "duplicate handler invocation")
		total += count
	}
	lock.Unlock()
	require(total == 2, "exactly one dispatch per new online invocation")
	select {
	case failure := <-failures:
		panic(failure)
	default:
	}
	close(stop)
	handlers.Wait()
	a.closeNormally()
	b.closeNormally()
}

type callbackRecord struct {
	body                                               []byte
	eventID, signature, timestamp, target, contentType string
}
type callbackServer struct {
	mu       sync.Mutex
	records  map[string][]callbackRecord
	server   *http.Server
	listener net.Listener
}

func newCallbacks() *callbackServer {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require(err == nil, "callback bind failed")
	cb := &callbackServer{records: map[string][]callbackRecord{}, listener: listener}
	cb.server = &http.Server{ReadHeaderTimeout: 2 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if e != nil {
			w.WriteHeader(500)
			return
		}
		rec := callbackRecord{body, r.Header.Get("X-RelayHub-Event-Id"), r.Header.Get("X-RelayHub-Signature"), r.Header.Get("X-RelayHub-Timestamp"), r.URL.RequestURI(), r.Header.Get("Content-Type")}
		cb.mu.Lock()
		cb.records[r.URL.Path] = append(cb.records[r.URL.Path], rec)
		n := len(cb.records[r.URL.Path])
		cb.mu.Unlock()
		if r.URL.Path == "/retry" {
			if n == 1 {
				w.WriteHeader(503)
			} else {
				w.WriteHeader(204)
			}
		} else {
			w.WriteHeader(400)
		}
	})}
	go func() { _ = cb.server.Serve(listener) }()
	return cb
}
func (s *suite) callbackChecks(producer, retry, terminal *credential, sentinel string) {
	p, _ := s.publish(producer, []string{retry.ID, terminal.ID}, "callback-event", sentinel)
	for _, j := range p.Jobs {
		c := retry
		want := "delivered"
		attempts := 2
		if j.Target == terminal.ID {
			c = terminal
			want = "dead_letter"
			attempts = 1
		}
		s.poll(15*time.Second, func() bool { return s.current(c, j).Status == want })
		got := s.current(c, j)
		if c == retry {
			s.expected["callback_job_id"] = j.ID
			s.expected["callback_event_id"] = j.EventID
			s.expected["callback_app_id"] = c.ID
		}
		require(got.CallbackAttempts == attempts && got.Attempts == attempts, "callback durable attempt count")
	}
	rawEvent := s.call(producer, "GET", "/api/v1/events/"+p.Jobs[0].EventID, nil, "", 200).Body
	rawEvent = bytes.TrimSuffix(rawEvent, []byte("\n"))
	s.callbacks.mu.Lock()
	defer s.callbacks.mu.Unlock()
	for target, c := range map[string]*credential{"/retry": retry, "/terminal": terminal} {
		records := s.callbacks.records[target]
		expected := 1
		if target == "/retry" {
			expected = 2
		}
		require(len(records) == expected, "callback HTTP attempt count")
		for _, rec := range records {
			s.hide(rec.signature)
			require(rec.eventID == p.Jobs[0].EventID && rec.contentType == "application/json", "callback metadata")
			require(bytes.Equal(rec.body, rawEvent), "callback body differs from persisted envelope bytes")
			expectedSig := signature(c.Secret, rec.timestamp, "POST", rec.target, rec.body)
			require(hmac.Equal([]byte(expectedSig), []byte(rec.signature)), "independent callback HMAC verification failed")
			ts, e := strconv.ParseInt(rec.timestamp, 10, 64)
			require(e == nil && time.Since(time.Unix(ts, 0)).Abs() < time.Minute, "callback timestamp freshness")
		}
	}
}
func (s *suite) docs() {
	resources := map[string]string{"": "text/html", "README.md": "text/markdown", "openapi.json": "application/json", "schemas/event-envelope.schema.json": "application/json", "schemas/client-frame.schema.json": "application/json", "schemas/server-frame.schema.json": "application/json", "llms.txt": "text/plain", "llms-full.txt": "text/plain", "skills/relayhub-integration/SKILL.md": "text/markdown", "skills/relayhub-integration.zip": "application/zip"}
	for resource, want := range resources {
		r := s.call(nil, "GET", "/docs/"+resource, nil, "", 200)
		typ, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		require(err == nil && typ == want, "documentation MIME mismatch")
		source := resource
		if source == "" {
			source = "index.html"
		}
		expected, err := os.ReadFile(filepath.Join("public-docs", source))
		require(err == nil && bytes.Equal(expected, r.Body), "embedded documentation bytes drift")
		if strings.HasSuffix(resource, ".zip") {
			require(strings.Contains(r.Header.Get("Content-Disposition"), "attachment"), "ZIP attachment metadata")
			archive, err := zip.NewReader(bytes.NewReader(r.Body), int64(len(r.Body)))
			require(err == nil, "Skill ZIP cannot open")
			expectedPaths := map[string]bool{"relayhub-integration/SKILL.md": true, "relayhub-integration/references/authentication.md": true, "relayhub-integration/references/openapi.json": true}
			require(len(archive.File) == len(expectedPaths), "Skill ZIP entry count")
			for _, file := range archive.File {
				require(expectedPaths[file.Name] && path.Clean(file.Name) == file.Name && !strings.HasPrefix(file.Name, "/"), "unexpected ZIP path")
				reader, e := file.Open()
				require(e == nil, "ZIP entry open")
				data, e := io.ReadAll(reader)
				_ = reader.Close()
				expected, e2 := os.ReadFile(filepath.Join("public-docs", "skills", file.Name))
				require(e == nil && e2 == nil && bytes.Equal(data, expected), "ZIP entry byte mismatch")
			}
		}
	}
}
func (s *suite) persistence(producer, consumer *credential, sentinel string) {
	p, _ := s.publish(producer, []string{consumer.ID}, "runtime-restart", sentinel)
	s.compose("restart", "relayhub-api", "relayhub-worker")
	s.compose("up", "-d", "--wait", "--wait-timeout", "90")
	s.topology()
	require(s.current(consumer, p.Jobs[0]).Status == "pending", "runtime restart lost queued state")
	s.queueAck(consumer, p)
	q, _ := s.publish(producer, []string{consumer.ID}, "redis-restart", sentinel)
	// Assert liveness stays up but readiness fails while Redis is stopped.
	s.compose("stop", "relayhub-redis")
	s.call(nil, "GET", "/healthz", nil, "", 200)
	s.call(nil, "GET", "/readyz", nil, "", 503)
	// Invoke worker readiness directly and require failure, without printing output.
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	argv := []string{"compose", "--project-name", s.project, "--project-directory", ".", "--env-file", filepath.Join(s.temp, "empty.env"), "-f", "compose.yaml", "-f", filepath.Join(s.temp, "override.yaml"), "exec", "-T", "relayhub-worker", "/relayhub", "healthcheck", "http://127.0.0.1:9090/readyz"}
	_, probeErr := runCommand(ctx, s.env, nil, "docker", argv...)
	require(probeErr != nil, "worker readiness must fail with Redis stopped")
	s.compose("start", "relayhub-redis")
	s.compose("up", "-d", "--wait", "--wait-timeout", "90")
	s.topology()
	require(s.current(consumer, q.Jobs[0]).Status == "pending", "Redis restart lost AOF queued state")
	s.queueAck(consumer, q)
}
func (s *suite) logs() {
	logs := s.compose("logs", "--no-color", "--no-log-prefix", "relayhub-api", "relayhub-worker")
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, value := range s.forbidden {
		if value != "" {
			require(!bytes.Contains(logs, []byte(value)), "secret or payload leaked to API/worker logs")
		}
	}
	require(bytes.Contains(logs, []byte("RelayHub worker running")) && bytes.Contains(logs, []byte("\"outcome\":\"success\"")), "expected runtime outcomes absent from logs")
	require(bytes.Contains(logs, []byte("\"request_id\":")), "generated request IDs absent from logs")
	matches := func(fields object) bool {
		for _, line := range bytes.Split(logs, []byte("\n")) {
			var entry object
			if json.Unmarshal(line, &entry) != nil {
				continue
			}
			ok := true
			for k, want := range fields {
				if entry[k] != want {
					ok = false
				}
			}
			if ok {
				return true
			}
		}
		return false
	}
	require(matches(object{"event_id": s.expected["callback_event_id"], "outcome": "published"}), "persisted publication ID/outcome absent from logs")
	require(matches(object{"job_id": s.expected["callback_job_id"], "event_id": s.expected["callback_event_id"], "app_id": s.expected["callback_app_id"], "attempt": float64(2), "outcome": "delivered"}), "callback persisted IDs/attempt/outcome absent from logs")
	require(matches(object{"function_id": s.expected["function_id"], "outcome": "registered"}), "function registration ID absent from logs")
	require(matches(object{"invocation_id": s.expected["invocation_id"], "app_id": s.expected["caller_app_id"], "outcome": "success"}), "function invocation ID/outcome absent from logs")
}
func (s *suite) run() {
	require(signature("test-secret", "1770000000", "POST", "/api/v1/socket/token?audience=browser", []byte(`{"scopes":["ws:connect"],"ttl_seconds":600}`)) == "4b6a6ada471e93240169f8878e877be6aa8d8cbf764e48f4a9d1b3c2ffef82ca", "independent signing golden vector")
	s.step("build and start isolated three-service stack", func() {
		s.compose("config", "--quiet")
		s.compose("up", "--build", "-d", "--wait", "--wait-timeout", "90")
	})
	s.step("health, readiness, metrics and private port inventory", s.topology)
	sentinel := "event-payload-" + random()
	s.hide(sentinel)
	var producer, consumer, unrelated, retry, terminal credential
	s.step("admin creates process-local credentials", func() {
		producer = s.create("e2e-producer", "queue", "")
		consumer = s.create("e2e-consumer", "queue", "")
		unrelated = s.create("e2e-unrelated", "websocket", "")
		port := strconv.Itoa(s.callbacks.listener.Addr().(*net.TCPAddr).Port)
		retry = s.create("e2e-retry", "callback", "http://e2e.internal:"+port+"/retry?kind=e2e")
		terminal = s.create("e2e-terminal", "callback", "http://e2e.internal:"+port+"/terminal")
	})
	s.step("signed publication, replay, queue, RFC 6455 isolation and ack", func() {
		w := s.connect(&consumer, []string{"events", "jobs"})
		other := s.connect(&unrelated, []string{"events", "jobs"})
		p, body := s.publish(&producer, []string{consumer.ID}, "first-event", sentinel)
		replay := s.request(&producer, false, "POST", "/api/v1/events", body, "first-event")
		require(replay.Status == 202 && replay.Header.Get("Idempotent-Replayed") == "true" && bytes.Equal(decode[publication](replay.Body).Event, p.Event), "event idempotency replay")
		frame := w.next("event")
		require(decode[object](encode(frame["event"]))["id"] == p.Jobs[0].EventID, "WebSocket event ID")
		s.queueAck(&consumer, p)
		s.poll(5*time.Second, func() bool {
			frame := w.next("job.updated")
			j := decode[job](encode(frame["job"]))
			return j.ID == p.Jobs[0].ID && j.Status == "acked"
		})
		// A round-trip barrier and bounded quiet period catch cross-app frames.
		require(other.conn.WriteJSON(object{"type": "ping"}) == nil, "isolation ping")
		select {
		case frame := <-other.frames:
			require(frame["type"] == "pong", "cross-application notification leaked")
		case <-time.After(3 * time.Second):
			panic("isolation pong deadline")
		}
		select {
		case <-other.frames:
			panic("cross-application notification leaked")
		case <-time.After(350 * time.Millisecond):
		}
		w.closeNormally()
		other.closeNormally()
	})
	s.step("single-handler functions, successful/offline/timeout replay", func() { s.functions(&producer, &consumer) })
	s.step("independent callback HMAC bytes, retry and terminal 400", func() { s.callbackChecks(&producer, &retry, &terminal, sentinel) })
	s.step("documentation MIME, exact bytes and normalized Skill ZIP", s.docs)
	s.step("API/worker and Redis AOF restart persistence", func() { s.persistence(&producer, &consumer, sentinel) })
	s.step("generated log IDs/outcomes and secret/payload scan", s.logs)
}
func execute() (code int) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, deadline := context.WithTimeout(ctx, 12*time.Minute)
	defer deadline()
	temp, err := os.MkdirTemp("", "relayhub-e2e-")
	require(err == nil, "acceptance workspace failed")
	s := &suite{expected: map[string]string{}, ctx: ctx, project: "relayhub-e2e-" + random()[:12], temp: temp, http: &http.Client{Timeout: 40 * time.Second}}
	defer func() {
		if p := recover(); p != nil {
			fmt.Fprintf(os.Stderr, "FAIL [%s]: %v\n", s.stage, p)
			code = 1
		}
		if s.callbacks != nil {
			_ = s.callbacks.server.Close()
		}
		if os.Getenv("RELAYHUB_E2E_KEEP") == "1" {
			fmt.Println("Kept isolated project:", s.project, "(remove with docker compose -p PROJECT down --volumes using this repository)")
		} else {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 45*time.Second)
			defer stop()
			// Cleanup only containers/volumes/networks bearing this unique project label.
			args := []string{"compose", "--project-name", s.project, "--project-directory", ".", "--env-file", filepath.Join(temp, "empty.env"), "-f", "compose.yaml", "-f", filepath.Join(temp, "override.yaml"), "down", "--volumes", "--remove-orphans", "--timeout", "20"}
			if _, e := runCommand(cleanupCtx, s.env, nil, "docker", args...); e != nil {
				fmt.Fprintln(os.Stderr, "FAIL: isolated project cleanup failed:", s.project)
				code = 1
			}
			// Remove only this acceptance build tag; Redis/base images are shared.
			_, _ = runCommand(cleanupCtx, nil, nil, "docker", "image", "rm", s.project+":local")
			if _, err := runCommand(cleanupCtx, nil, nil, "docker", "image", "inspect", s.project+":local"); err == nil {
				fmt.Fprintln(os.Stderr, "FAIL: isolated image cleanup verification")
				code = 1
			}
			for _, resource := range []string{"container", "network", "volume"} {
				remaining, err := runCommand(cleanupCtx, nil, nil, "docker", resource, "ls", "-q", "--filter", "label=com.docker.compose.project="+s.project)
				if err != nil || len(bytes.TrimSpace(remaining)) != 0 {
					fmt.Fprintln(os.Stderr, "FAIL: isolated resource cleanup verification:", resource)
					code = 1
				}
			}
		}
		_ = os.RemoveAll(temp)
	}()
	s.stage = "setup"
	require(os.WriteFile(filepath.Join(temp, "empty.env"), nil, 0600) == nil, "acceptance environment file")
	require(os.WriteFile(filepath.Join(temp, "override.yaml"), []byte("services:\n  relayhub-api:\n    image: "+s.project+":local\n  relayhub-worker:\n    image: "+s.project+":local\n    extra_hosts: [\"e2e.internal:host-gateway\"]\n"), 0600) == nil, "acceptance callback host configuration")
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "RELAYHUB_") && !strings.HasPrefix(v, "COMPOSE_") {
			s.env = append(s.env, v)
		}
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	require(e == nil, "API port reservation failed")
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	s.base = "http://127.0.0.1:" + strconv.Itoa(port)
	for _, key := range []string{"RELAYHUB_ADMIN_TOKEN", "RELAYHUB_SIGNING_SECRET", "RELAYHUB_REDIS_PASSWORD"} {
		secret := random()
		s.env = append(s.env, key+"="+secret)
		s.hide(secret)
	}
	s.env = append(s.env, "RELAYHUB_PORT=127.0.0.1:"+strconv.Itoa(port), "RELAYHUB_ALLOW_INSECURE_CALLBACKS=true")
	s.callbacks = newCallbacks()
	s.run()
	fmt.Println("PASS: RelayHub end-to-end acceptance; credentials and payloads withheld")
	return 0
}

// backupRehearsal exercises the runbook using private Redis-owned files.
func backupRehearsal() (code int) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	id := "relayhub-backup-" + random()[:12]
	volumes := []string{id + "-source", id + "-restore"}
	helper := id + "-helper"
	defer func() {
		if recovered := recover(); recovered != nil {
			fmt.Fprintln(os.Stderr, "FAIL: backup rehearsal:", recovered)
			code = 1
		}
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		_, _ = runCommand(cleanup, nil, nil, "docker", "rm", "-f", helper)
		for _, volume := range volumes {
			if _, err := runCommand(cleanup, nil, nil, "docker", "volume", "rm", volume); err != nil {
				fmt.Fprintln(os.Stderr, "FAIL: owned rehearsal volume cleanup")
				code = 1
			}
		}
	}()
	run := func(input io.Reader, args ...string) []byte {
		out, err := runCommand(ctx, nil, input, "docker", args...)
		require(err == nil, "disposable volume operation failed")
		return out
	}
	for _, volume := range volumes {
		run(nil, "volume", "create", "--label", "relayhub.backup-rehearsal="+id, volume)
	}
	container := func(user, volume string, input io.Reader, args ...string) []byte {
		mount := volume + ":/data"
		if strings.HasSuffix(volume, ":ro") {
			mount = strings.TrimSuffix(volume, ":ro") + ":/data:ro"
		}
		base := []string{"run", "--rm", "--name", helper, "--network", "none", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", user, "-i", "-v", mount, "redis:7-alpine"}
		return run(input, append(base, args...)...)
	}
	expected := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	manifest := []byte("file appendonly.aof.1.incr.aof seq 1 type i\n")
	fmt.Println("CHECK: private UID 999 AOF backup and restore")
	container("999:999", volumes[0], bytes.NewReader(expected), "sh", "-c", "umask 077; mkdir -m 700 /data/appendonlydir; cat > /data/appendonlydir/appendonly.aof.1.incr.aof; printf 'file appendonly.aof.1.incr.aof seq 1 type i\\n' > /data/appendonlydir/appendonly.aof.manifest")
	fmt.Println("CHECK: archive private source directory using the runbook helper identity")
	// Match the Redis file owner without granting DAC bypass or any capability.
	archive := container("999:999", volumes[0]+":ro", nil, "tar", "-C", "/data", "-czf", "-", ".")
	container("999:999", volumes[1], bytes.NewReader(archive), "tar", "-C", "/data", "-xzf", "-")
	actual := container("999:999", volumes[1], nil, "cat", "/data/appendonlydir/appendonly.aof.1.incr.aof")
	require(bytes.Equal(actual, expected), "restored AOF bytes differ")
	actual = container("999:999", volumes[1], nil, "cat", "/data/appendonlydir/appendonly.aof.manifest")
	require(bytes.Equal(actual, manifest), "restored manifest bytes differ")
	metadata := container("999:999", volumes[1], nil, "stat", "-c", "%u:%g:%a", "/data/appendonlydir", "/data/appendonlydir/appendonly.aof.1.incr.aof", "/data/appendonlydir/appendonly.aof.manifest")
	require(string(metadata) == "999:999:700\n999:999:600\n999:999:600\n", "restored private ownership or permissions differ")
	fmt.Println("PASS: UID 999 reads exact restored AOF/manifest bytes with original 700/600 permissions and no capabilities")
	return 0
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--backup-rehearsal" {
		os.Exit(backupRehearsal())
	}
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: e2e-client [--backup-rehearsal] (run ./scripts/e2e.sh)")
		os.Exit(2)
	}
	os.Exit(execute())
}
