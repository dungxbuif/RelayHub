package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/dungxbuif/RelayHub/internal/observability"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func scrapeMetrics() string {
	w := httptest.NewRecorder()
	observability.MetricsHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	return w.Body.String()
}

type failedPublicationStore struct {
	store.EventStore
	panic bool
}

func (s failedPublicationStore) FindPublication(context.Context, string, string) (store.Publication, error) {
	if s.panic {
		panic("private-repository-panic")
	}
	return store.Publication{}, errors.New("private-redis-address")
}

func TestPublicationFailureMetricsNeverCountDurableAcceptance(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprint("panic=", panics), func(t *testing.T) {
			h, c := eventRouter(t, func(s store.EventStore) store.EventStore { return failedPublicationStore{EventStore: s, panic: panics} })
			before := scrapeMetrics()
			body := []byte(fmt.Sprintf(`{"type":"valid","target_app_ids":[%q],"data":{}}`, c[1].AppID))
			response := signedEventRequest(t, h, c[0], "POST", "/api/v1/events", body, "failure-key")
			if response.Code != 500 {
				t.Fatal(response.Code)
			}
			after := scrapeMetrics()
			published := `relayhub_event_outcomes_total{outcome="published"}`
			if metricValue(t, after, published) != metricValue(t, before, published) {
				t.Error("failed publication counted as durable acceptance")
			}
			if !panics {
				failure := `relayhub_event_outcomes_total{outcome="store_error"}`
				if metricValue(t, after, failure)-metricValue(t, before, failure) != 1 {
					t.Error("storage failure metric missing")
				}
			}
			request := `relayhub_http_requests_total{method="POST",route="/api/v1/events",status="500"}`
			if metricValue(t, after, request)-metricValue(t, before, request) != 1 {
				t.Error("HTTP failure metric missing")
			}
			if strings.Contains(after, "private-repository-panic") || strings.Contains(after, "private-redis-address") {
				t.Error("failure details leaked")
			}
		})
	}
}

func metricValue(t *testing.T, body, sample string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, sample+" ") {
			value, err := strconv.ParseFloat(strings.TrimPrefix(line, sample+" "), 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	return 0
}

func TestHTTPAndEventMetricsCountOutcomesWithoutPrivateLabels(t *testing.T) {
	h, creds := eventRouter(t)
	before := scrapeMetrics()
	body := []byte(fmt.Sprintf(`{"type":"private-event-type","target_app_ids":[%q],"data":{"private-payload":"value"}}`, creds[1].AppID))
	for range 2 {
		if r := signedEventRequest(t, h, creds[0], "POST", "/api/v1/events", body, "private-idempotency-key"); r.Code != 202 {
			t.Fatal(r.Code)
		}
	}
	for _, invalid := range []string{`{`, `{"type":"private-event-type","target_app_ids":[],"data":{}}`} {
		if r := signedEventRequest(t, h, creds[0], "POST", "/api/v1/events", []byte(invalid), "invalid"); r.Code != 400 {
			t.Fatal(r.Code)
		}
	}
	signedEventRequest(t, h, creds[0], "GET", "/api/v1/events/private-event-id?secret=private-query", nil, "")
	requestJSON(t, h, "PRIVATE-METHOD", "/private-path?private-query", nil, map[string]string{"Authorization": "Bearer private-token"})
	after := scrapeMetrics()
	for sample, want := range map[string]float64{
		`relayhub_event_outcomes_total{outcome="published"}`:                                              1,
		`relayhub_event_outcomes_total{outcome="replayed"}`:                                               1,
		`relayhub_event_outcomes_total{outcome="rejected"}`:                                               2,
		`relayhub_http_requests_total{method="POST",route="/api/v1/events",status="202"}`:                 2,
		`relayhub_http_requests_total{method="POST",route="/api/v1/events",status="400"}`:                 2,
		`relayhub_http_requests_total{method="GET",route="/api/v1/events/{eventID}",status="404"}`:        1,
		`relayhub_http_requests_total{method="OTHER",route="unmatched",status="405"}`:                     1,
		`relayhub_http_request_duration_seconds_count{method="POST",route="/api/v1/events",status="202"}`: 2,
	} {
		if delta := metricValue(t, after, sample) - metricValue(t, before, sample); delta != want {
			t.Errorf("%s delta=%v want=%v", sample, delta, want)
		}
	}
	for _, private := range []string{"private-event-type", "private-payload", "private-idempotency-key", "private-event-id", "private-query", "private-path", "private-token", "PRIVATE-METHOD", creds[0].AppID, creds[0].APIKey, creds[0].HMACSecret} {
		if strings.Contains(after, private) {
			t.Errorf("private metric label: %q", private)
		}
	}
}
