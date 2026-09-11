package outbox

import (
	"time"

	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var outboxPending = promauto.NewGauge(prometheus.GaugeOpts{Name: "relayhub_outbox_pending", Help: "PostgreSQL outbox rows awaiting confirmed broker dispatch."})
var outboxClaimed = promauto.NewGauge(prometheus.GaugeOpts{Name: "relayhub_outbox_claimed", Help: "Pending outbox rows with an active dispatcher claim."})
var outboxOldestSeconds = promauto.NewGauge(prometheus.GaugeOpts{Name: "relayhub_outbox_oldest_pending_seconds", Help: "Age of the oldest pending outbox row."})
var outboxOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{Name: "relayhub_outbox_dispatch_total", Help: "Bounded outbox dispatch and recovery outcomes."}, []string{"outcome"})

func recordOutcome(outcome string) {
	switch outcome {
	case "published", "duplicate", "publish_error", "store_error", "reclaimed":
		outboxOutcomes.WithLabelValues(outcome).Inc()
	}
}

func recordStats(stats store.OutboxStats, now time.Time) {
	outboxPending.Set(float64(stats.Pending))
	outboxClaimed.Set(float64(stats.Claimed))
	if stats.OldestPendingAt == nil {
		outboxOldestSeconds.Set(0)
		return
	}
	age := now.Sub(*stats.OldestPendingAt).Seconds()
	if age < 0 {
		age = 0
	}
	outboxOldestSeconds.Set(age)
}
