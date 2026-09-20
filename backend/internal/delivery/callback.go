package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/domain"
)

type Request struct {
	App    domain.App
	Event  domain.Event
	Body   []byte
	Secret []byte
}
type Result struct {
	Status  int
	Headers http.Header
	Err     error
}
type Callback struct {
	Timeout   time.Duration
	Now       func() time.Time
	Transport http.RoundTripper
}

func NewCallback(timeout time.Duration) *Callback {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Callback{Timeout: timeout, Now: time.Now}
}
func (c *Callback) Deliver(ctx context.Context, input Request) Result {
	if input.App.CallbackURL == nil {
		return Result{Err: errors.New("callback_unavailable")}
	}
	u, err := url.Parse(*input.App.CallbackURL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return Result{Err: errors.New("callback_invalid")}
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(input.Body))
	if err != nil {
		return Result{Err: errors.New("callback_invalid")}
	}
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-RelayHub-Event-Id", input.Event.ID)
	req.Header.Set("X-RelayHub-Timestamp", timestamp)
	req.Header.Set("X-RelayHub-Signature", auth.Sign(input.Secret, timestamp, http.MethodPost, u.RequestURI(), input.Body))
	client := &http.Client{Transport: c.Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return Result{Err: errors.New("callback_transport")}
	}
	defer response.Body.Close()
	_, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if drainErr != nil || ctx.Err() != nil {
		return Result{Err: errors.New("callback_transport")}
	}
	// Only the retry hint crosses the boundary; arbitrary response headers may contain secrets.
	headers := http.Header{}
	headers.Set("Retry-After", response.Header.Get("Retry-After"))
	return Result{Status: response.StatusCode, Headers: headers}
}
