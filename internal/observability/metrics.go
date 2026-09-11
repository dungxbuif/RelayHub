package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

var httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "relayhub_http_requests_total", Help: "Completed HTTP requests by bounded method, registered route and status."}, []string{"method", "route", "status"})
var httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "relayhub_http_request_duration_seconds", Help: "HTTP handler duration, including long polls and WebSocket session lifetime.", Buckets: []float64{.01, .05, .1, .5, 1, 5, 30, 60}}, []string{"method", "route", "status"})

// route must come from the router's registered template, never a request URL.
func HTTPRequest(method, route string, status int, elapsed time.Duration) {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
	default:
		method = "OTHER"
	}
	if status < 100 || status > 599 {
		status = 500
	}
	labels := []string{method, route, strconv.Itoa(status)}
	httpRequests.WithLabelValues(labels...).Inc()
	httpDuration.WithLabelValues(labels...).Observe(elapsed.Seconds())
}

var eventOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{Name: "relayhub_event_outcomes_total", Help: "Event publication outcomes; replays do not count as new publications."}, []string{"outcome"})

func EventOutcome(outcome string) {
	switch outcome {
	case "published", "replayed", "rejected", "store_error":
		eventOutcomes.WithLabelValues(outcome).Inc()
	}
}

var WebSocketConnections = promauto.NewGauge(prometheus.GaugeOpts{Name: "relayhub_websocket_connections", Help: "Current registered WebSocket sessions."})
var WebSocketSlowClients = promauto.NewCounter(prometheus.CounterOpts{Name: "relayhub_websocket_slow_clients_total", Help: "WebSocket sessions disconnected after their outbound queue filled."})
var notificationFailures = promauto.NewCounter(prometheus.CounterOpts{Name: "relayhub_notification_failures_total", Help: "Best-effort notification failures after durable state changes."})

func NotificationFailed() { notificationFailures.Inc() }

var callbackOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{Name: "relayhub_callback_outcomes_total", Help: "Callback worker durable outcomes and store error categories."}, []string{"outcome"})

// CallbackOutcome accepts bounded category labels, never callback URLs or payloads.
func CallbackOutcome(outcome string) {
	switch outcome {
	case "delivered", "pending", "dead_letter", "store_error":
		callbackOutcomes.WithLabelValues(outcome).Inc()
	}
}

var functionOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{Name: "relayhub_function_outcomes_total", Help: "Function registrations, accepted invocations and initial caller outcomes."}, []string{"outcome"})
var functionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "relayhub_function_duration_seconds", Help: "Initial function caller latency by terminal outcome.", Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30}}, []string{"outcome"})

// FunctionOutcome never accepts application IDs, function names or payload values
// as labels. Replays do not increment accepted/terminal invocation counters.
func FunctionOutcome(outcome string, elapsed time.Duration) {
	switch outcome {
	case "registered", "invoked":
		functionOutcomes.WithLabelValues(outcome).Inc()
	case "success", "handler_error", "unavailable", "timeout":
		functionOutcomes.WithLabelValues(outcome).Inc()
		functionDuration.WithLabelValues(outcome).Observe(elapsed.Seconds())
	}
}
