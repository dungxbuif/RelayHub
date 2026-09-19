package delivery

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	now := time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		status int
		err    error
		kind   Kind
	}{
		{200, nil, Delivered}, {204, nil, Delivered}, {299, nil, Delivered}, {400, nil, DeadLetter}, {401, nil, DeadLetter}, {403, nil, DeadLetter}, {404, nil, DeadLetter}, {422, nil, DeadLetter}, {408, nil, Retry}, {425, nil, Retry}, {429, nil, Retry}, {500, nil, Retry}, {503, nil, Retry}, {599, nil, Retry}, {0, errors.New("secret payload"), Retry}, {0, context.DeadlineExceeded, Retry}, {302, nil, DeadLetter},
	} {
		got := Classify(c.status, nil, c.err, 1, now)
		if got.Kind != c.kind {
			t.Fatalf("%d: %+v", c.status, got)
		}
		if got.Reason == "secret payload" {
			t.Fatal("unsafe error")
		}
	}
	for attempt := 1; attempt <= 7; attempt++ {
		got := Classify(503, nil, nil, attempt, now)
		if attempt >= 6 {
			if got.Kind != DeadLetter {
				t.Fatalf("attempt %d: %+v", attempt, got)
			}
		} else {
			want := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 60 * time.Second, 300 * time.Second}[attempt-1]
			if got.Kind != Retry || !got.RetryAt.Equal(now.Add(want)) {
				t.Fatalf("attempt %d: %+v", attempt, got)
			}
		}
	}
	for _, c := range []struct {
		raw   string
		delay time.Duration
	}{{"0", 0}, {"17", 17 * time.Second}, {"999999", 300 * time.Second}, {now.Add(42 * time.Second).Format(http.TimeFormat), 42 * time.Second}, {now.Add(900 * time.Second).Format(http.TimeFormat), 300 * time.Second}, {"bad", time.Second}, {"-1", time.Second}, {"1.5", time.Second}, {now.Add(-time.Second).Format(http.TimeFormat), time.Second}} {
		got := Classify(429, http.Header{"Retry-After": []string{c.raw}}, nil, 1, now)
		if !got.RetryAt.Equal(now.Add(c.delay)) {
			t.Fatalf("retry-after %q: %+v", c.raw, got)
		}
	}
}
