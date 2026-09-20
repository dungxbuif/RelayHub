package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
)

func TestCallbackSignedPersistedBytes(t *testing.T) {
	now := time.Unix(1789084800, 0)
	raw := []byte(`{"id":"evt_exact", "data":{"n":9007199254740993}}`)
	secret := []byte("target-secret")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != string(raw) {
			t.Error("body changed")
		}
		if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-RelayHub-Event-Id") != "evt_exact" || r.Header.Get("X-RelayHub-Timestamp") != "1789084800" {
			t.Error("headers missing")
		}
		if r.Header.Get("X-RelayHub-Signature") != "b89bfb8065d0a89fb89e06853d3167794b0092e60e4790cdfbd1ed6d600e5d2a" {
			t.Error("canonical signature vector mismatch")
		}
		if err := auth.Verify(secret, r.Header.Get("X-RelayHub-Timestamp"), "POST", "/a%2Fb?x=1&x=2", r.Header.Get("X-RelayHub-Signature"), body, now, time.Second); err != nil {
			t.Error(err)
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	url := server.URL + "/a%2Fb?x=1&x=2"
	cb := NewCallback(time.Second)
	cb.Now = func() time.Time { return now }
	result := cb.Deliver(context.Background(), Request{App: domain.App{CallbackURL: &url}, Event: domain.Event{ID: "evt_exact"}, Body: raw, Secret: secret})
	if result.Err != nil || result.Status != 204 {
		t.Fatalf("%+v", result)
	}
}
func TestCallbackTimeoutAndRedirect(t *testing.T) {
	var forwarded atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
	defer dest.Close()
	for _, code := range []int{301, 302, 303, 307, 308} {
		src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", dest.URL)
			w.WriteHeader(code)
		}))
		url := src.URL
		got := NewCallback(time.Second).Deliver(context.Background(), Request{App: domain.App{CallbackURL: &url}, Body: []byte(`{}`), Secret: []byte("secret")})
		src.Close()
		if got.Status != code || got.Err != nil {
			t.Fatalf("redirect %+v", got)
		}
	}
	if forwarded.Load() != 0 {
		t.Fatal("credentials forwarded")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(150 * time.Millisecond):
		}
	}))
	defer slow.Close()
	url := slow.URL
	start := time.Now()
	got := NewCallback(20*time.Millisecond).Deliver(context.Background(), Request{App: domain.App{CallbackURL: &url}, Body: []byte(`{}`)})
	if got.Err == nil || time.Since(start) > time.Second {
		t.Fatal("timeout not bounded")
	}
	for _, url := range []string{"http://user:secret@localhost/a?api_key=secret", "http://[bad", "ftp://example.com", "http://127.0.0.1:1/?payload=secret"} {
		got := NewCallback(time.Second).Deliver(context.Background(), Request{App: domain.App{CallbackURL: &url}, Body: json.RawMessage(`{"secret":true}`)})
		if got.Err == nil || strings.Contains(fmt.Sprint(got), "secret") {
			t.Fatalf("unsafe error: %+v", got)
		}
	}
}

type countedBody struct {
	n      int
	closed bool
}

func (b *countedBody) Read(p []byte) (int, error) { b.n += len(p); return len(p), nil }
func (b *countedBody) Close() error               { b.closed = true; return nil }

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestCallbackDrainBound(t *testing.T) {
	body := &countedBody{}
	cb := NewCallback(time.Second)
	cb.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body, Header: http.Header{}}, nil
	})
	url := "https://callback.example"
	got := cb.Deliver(context.Background(), Request{App: domain.App{CallbackURL: &url}, Body: []byte(`{}`)})
	if got.Err != nil || body.n != 1<<20 || !body.closed {
		t.Fatalf("drain %d %t %+v", body.n, body.closed, got)
	}
}

func TestCallbackDeadlineWhileDraining(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()
	url := srv.URL
	got := NewCallback(10*time.Millisecond).Deliver(context.Background(), Request{App: domain.App{CallbackURL: &url}, Body: []byte(`{}`)})
	if got.Err == nil {
		t.Fatal("body deadline must retry")
	}
}
