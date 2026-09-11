package delivery

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Kind string

const (
	Delivered  Kind = "delivered"
	Retry      Kind = "retry"
	DeadLetter Kind = "dead_letter"
)

type Outcome struct {
	Kind    Kind
	RetryAt time.Time
	Reason  string
}

// MaxAttempts includes the initial delivery and five retries.
const MaxAttempts = 6

func Classify(status int, headers http.Header, err error, attempt int, now time.Time) Outcome {
	if err == nil && status >= 200 && status < 300 {
		return Outcome{Kind: Delivered, Reason: "http_success"}
	}
	reason := "http_permanent"
	retry := err != nil || status == 408 || status == 425 || status == 429 || status >= 500 && status <= 599
	if !retry {
		return Outcome{Kind: DeadLetter, Reason: reason}
	}
	if err != nil {
		reason = "transport_error"
	} else {
		reason = "http_transient"
	}
	if attempt >= MaxAttempts {
		return Outcome{Kind: DeadLetter, Reason: "attempts_exhausted"}
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 60 * time.Second, 300 * time.Second}[attempt-1]
	if status == 429 && err == nil {
		raw := strings.TrimSpace(headers.Get("Retry-After"))
		if secs, e := strconv.ParseInt(raw, 10, 64); e == nil && secs >= 0 {
			if secs > 300 {
				secs = 300
			}
			delay = time.Duration(secs) * time.Second
		} else if at, e := http.ParseTime(raw); e == nil && !at.Before(now) {
			delay = at.Sub(now)
			if delay > 300*time.Second {
				delay = 300 * time.Second
			}
		}
	}
	return Outcome{Kind: Retry, RetryAt: now.Add(delay), Reason: reason}
}
