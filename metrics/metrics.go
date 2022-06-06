package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	SyncEvent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sync_event_total",
		Help: "Total Number of sync events",
	})
	SyncEventWithLabel = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_event",
		Help: "Number of sync events",
	}, []string{"type"})
)
