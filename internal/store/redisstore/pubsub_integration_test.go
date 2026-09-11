//go:build integration

package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func bridgeRead(t *testing.T, s *realtime.Session) realtime.ServerFrame {
	t.Helper()
	select {
	case raw := <-s.Frames():
		var f realtime.ServerFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatal(err)
		}
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("missing cross-instance notification")
		return realtime.ServerFrame{}
	}
}

func reconnectTestClient(control *Client, disconnected *atomic.Bool, attempts chan<- struct{}) *Client {
	base := control.client.Options()
	options := *base
	if base.TLSConfig != nil {
		options.TLSConfig = base.TLSConfig.Clone()
	}
	options.ClientName = "relayhub-task9-bridge-" + uuid.NewString()
	// This processor belongs to the original client; let NewClient create its own.
	options.PushNotificationProcessor = nil
	dial := base.Dialer
	if dial == nil {
		dial = redis.NewDialer(&options)
	}
	options.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
		if disconnected.Load() {
			select {
			case attempts <- struct{}{}:
			default:
			}
			return nil, errors.New("controlled reconnect outage")
		}
		return dial(ctx, network, address)
	}
	return &Client{client: redis.NewClient(&options), prefix: control.prefix}
}

func TestPubSubReconnectAndShutdownDuringReconnect(t *testing.T) {
	control := integrationRedisClient(t)
	ctx := context.Background()
	var disconnected atomic.Bool
	attempts := make(chan struct{}, 1)
	c := reconnectTestClient(control, &disconnected, attempts)
	defer c.Close()
	h := realtime.NewHub()
	defer h.Close()
	b, err := NewBridge(ctx, c, h)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	s := h.Register("receiver")
	_ = h.Subscribe(s, []string{"events"})
	findID := func() string {
		list, err := control.client.ClientList(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(list, "\n") {
			if !strings.Contains(line, "name="+c.client.Options().ClientName+" ") || !strings.Contains(line, "psub=1 ") {
				continue
			}
			for _, field := range strings.Fields(line) {
				if strings.HasPrefix(field, "id=") {
					return strings.TrimPrefix(field, "id=")
				}
			}
		}
		return ""
	}
	old := findID()
	if old == "" {
		t.Fatal("subscription absent")
	}
	if err := control.client.Do(ctx, "CLIENT", "KILL", "ID", old).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		id := findID()
		if id != "" && id != old {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bridge failed to resubscribe")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := b.PublishEvent(ctx, domain.Event{ID: "after_reconnect", TargetAppIDs: []string{"receiver"}, Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if frame := bridgeRead(t, s); frame.Type != "event" || frame.Event.ID != "after_reconnect" {
		t.Fatal("incorrect recovered delivery")
	}
	id := findID()
	disconnected.Store(true)
	if err := control.client.Do(ctx, "CLIENT", "KILL", "ID", id).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attempts:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect never attempted")
	}
	done := make(chan struct{})
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked during reconnect")
	}
}
func TestPubSubCrossInstanceAndShutdown(t *testing.T) {
	c := integrationRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	one, two := realtime.NewHub(), realtime.NewHub()
	defer one.Close()
	defer two.Close()
	b1, err := NewBridge(ctx, c, one)
	if err != nil {
		t.Fatal(err)
	}
	defer b1.Close()
	b2, err := NewBridge(ctx, c, two)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	a, b, x := two.Register("a"), two.Register("b"), two.Register("x")
	for _, s := range []*realtime.Session{a, b, x} {
		if e := two.Subscribe(s, []string{"events", "jobs"}); e != nil {
			t.Fatal(e)
		}
	}
	e := domain.Event{ID: "evt_cross", TargetAppIDs: []string{"a", "b"}, Data: json.RawMessage(`{"n":9007199254740993}`)}
	if err = b1.PublishEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*realtime.Session{a, b} {
		f := bridgeRead(t, s)
		if f.Type != "event" || f.Event.ID != e.ID || len(f.Event.TargetAppIDs) != 2 {
			t.Fatalf("frame %#v", f)
		}
	}
	if err = b1.PublishJob(ctx, domain.Job{ID: "job_cross", TargetAppID: "b"}); err != nil {
		t.Fatal(err)
	}
	if bridgeRead(t, b).Type != "job.updated" {
		t.Fatal("duplicate event or missing job")
	}
	select {
	case raw := <-x.Frames():
		t.Fatalf("leak %s", raw)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	done := make(chan struct{})
	go func() { b1.Close(); b2.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bridge shutdown blocked")
	}
}
func TestRedisPrefixIsolation(t *testing.T) {
	base := integrationRedisClient(t)
	one := derivedIntegrationClient(t, base, "one")
	two := derivedIntegrationClient(t, base, "two")
	var err error
	ctx := context.Background()
	now := time.Now().UTC()
	ret := store.EventRetention{Event: time.Hour, Job: time.Hour, Idempotency: time.Hour}
	for _, c := range []*Client{one, two} {
		seedTargets(t, c)
		if _, _, err := publishFixture(t, c, "same", now, ret); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = one.TransitionJob(ctx, "job_samea", domain.JobDeadLetter, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	j, err := two.GetJob(ctx, "job_samea")
	if err != nil || j.Status != domain.JobPending {
		t.Fatalf("job collision %#v %v", j, err)
	}
	if err = one.RotateApplicationCredential(ctx, "a", store.AppCredential{AppID: "a", APIKeyHash: "new", HMACSecret: []byte("secret")}, now); err != nil {
		t.Fatal(err)
	}
	if _, err = one.FindCredentialByAPIKeyHash(ctx, "a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("old credential remains")
	}
	if _, err = two.FindCredentialByAPIKeyHash(ctx, "a"); err != nil {
		t.Fatal("cross-prefix credential deletion")
	}
	if _, err = one.FindCredentialByAPIKeyHash(ctx, "new"); err != nil {
		t.Fatal("prefixed credential lookup")
	}
	if err = two.AckEvent(ctx, "b", "evt_same", now, time.Hour); err != nil {
		t.Fatal(err)
	}
	ack, err := two.GetEventJob(ctx, "b", "evt_same")
	if err != nil || ack.Status != domain.JobAcked {
		t.Fatalf("ack lookup %#v %v", ack, err)
	}
	if j, _ := one.GetJob(ctx, "job_sameb"); j.Status != domain.JobPending {
		t.Fatal("cross-prefix ack")
	}
	h1, h2 := realtime.NewHub(), realtime.NewHub()
	defer h1.Close()
	defer h2.Close()
	b1, err := NewBridge(ctx, one, h1)
	if err != nil {
		t.Fatal(err)
	}
	defer b1.Close()
	b2, err := NewBridge(ctx, two, h2)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	s1, s2 := h1.Register("a"), h2.Register("a")
	_ = h1.Subscribe(s1, []string{"events"})
	_ = h2.Subscribe(s2, []string{"events"})
	if err = b1.PublishEvent(ctx, domain.Event{ID: "one", TargetAppIDs: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	bridgeRead(t, s1)
	select {
	case raw := <-s2.Frames():
		t.Fatalf("namespace leak %s", raw)
	case <-time.After(50 * time.Millisecond):
	}
	for _, c := range []*Client{one, two} {
		keys, err := c.client.Keys(ctx, c.prefix+":*").Result()
		if err != nil || len(keys) < 8 {
			t.Fatalf("missing namespace keys %v %v", keys, err)
		}
	}
}
