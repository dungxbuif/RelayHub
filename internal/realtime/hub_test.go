package realtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

func receive(t *testing.T, s *Session) ServerFrame {
	t.Helper()
	select {
	case b := <-s.Frames():
		var f ServerFrame
		if err := json.Unmarshal(b, &f); err != nil {
			t.Fatal(err)
		}
		return f
	case <-time.After(time.Second):
		t.Fatal("missing frame")
		return ServerFrame{}
	}
}
func TestHubIsolationAndSubscriptions(t *testing.T) {
	h := NewHub()
	defer h.Close()
	a := h.Register("a")
	a2 := h.Register("a")
	b := h.Register("b")
	e := domain.Event{ID: "evt_1", TargetAppIDs: []string{"a"}}
	_ = h.PublishEvent(context.Background(), e)
	if len(a.outbound) != 0 {
		t.Fatal("delivery before subscription")
	}
	for _, s := range []*Session{a, a2, b} {
		if err := h.Subscribe(s, []string{"events"}); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.PublishEvent(context.Background(), e)
	for _, s := range []*Session{a, a2} {
		if f := receive(t, s); f.Event == nil || f.Event.ID != e.ID {
			t.Fatalf("bad event %#v", f)
		}
	}
	if len(b.outbound) != 0 {
		t.Fatal("cross-app delivery")
	}
	_ = h.PublishJob(context.Background(), domain.Job{ID: "j", TargetAppID: "a"})
	if len(a.outbound) != 0 {
		t.Fatal("unsubscribed jobs")
	}
	if err := h.Subscribe(a, []string{"jobs"}); err != nil {
		t.Fatal(err)
	}
	_ = h.PublishJob(context.Background(), domain.Job{ID: "j", TargetAppID: "a"})
	if receive(t, a).Type != "job.updated" {
		t.Fatal("missing job")
	}
	if err := h.Subscribe(a, []string{"functions"}); err == nil {
		t.Fatal("functions authorized")
	}
	if err := h.InvokeFunction(context.Background(), "a", ServerFrame{Type: "rpc.invoke"}); err == nil {
		t.Fatal("function invoked")
	}
	if err := h.HandleResult(a, ClientFrame{Type: "rpc.result"}); err == nil {
		t.Fatal("rpc routed")
	}
	h.Disconnect(a)
	h.Disconnect(a)
	select {
	case <-a.Done():
	default:
		t.Fatal("not closed")
	}
}
func TestHubSlowClientAndShutdown(t *testing.T) {
	h := NewHub()
	s := h.Register("a")
	_ = h.Subscribe(s, []string{"events"})
	e := domain.Event{TargetAppIDs: []string{"a"}}
	for range 64 {
		_ = h.PublishEvent(context.Background(), e)
	}
	select {
	case <-s.Done():
		t.Fatal("closed before queue full")
	default:
	}
	_ = h.PublishEvent(context.Background(), e)
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("slow client not closed")
	}
	h.Close()
	later := h.Register("a")
	select {
	case <-later.Done():
	default:
		t.Fatal("registration after shutdown")
	}
}
func TestHubConcurrentPublishClose(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := h.Register("a")
			_ = h.Subscribe(s, []string{"events"})
			for range 100 {
				_ = h.PublishEvent(context.Background(), domain.Event{TargetAppIDs: []string{"a"}})
			}
			s.Close()
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); h.Close() }()
	wg.Wait()
	h.Close()
}
