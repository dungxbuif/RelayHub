package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func MetricsHandler() http.Handler {
	return promhttp.Handler()
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
