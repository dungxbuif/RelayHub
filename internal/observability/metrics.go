package observability

import (
	"net/http"

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
