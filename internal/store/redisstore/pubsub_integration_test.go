//go:build integration

package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/store"
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
	url := "redis://" + base.client.Options().Addr + "/0"
	one, err := NewClientWithPrefix(url, "namespace_one")
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	two, err := NewClientWithPrefix(url, "namespace_two")
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
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
