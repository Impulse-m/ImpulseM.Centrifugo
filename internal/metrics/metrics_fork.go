package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// forkRegistry holds metrics of fork features: access revocation, rate limits and the RPC registry. It is embedded
// into Registry, so the metrics are created by initForkMetrics and exported by setForkMetrics.
type forkRegistry struct {
	// Access revocation metrics
	accessRevokeRejectsTotal            *prometheus.CounterVec
	accessRevokeDisconnectsTotal        *prometheus.CounterVec
	accessRevokeDisconnectsDroppedTotal prometheus.Counter
	accessRevokeStoreSize               *prometheus.GaugeVec

	// Access revocation source metrics
	accessRevokeSourceReady              prometheus.Gauge
	accessRevokeSourceLagging            prometheus.Gauge
	accessRevokeSourceLagRecords         prometheus.Gauge
	accessRevokeSourceEventsAppliedTotal *prometheus.CounterVec
	accessRevokeSourceEventsSkippedTotal *prometheus.CounterVec
	accessRevokeSourceEventsBeyondWindow prometheus.Counter
	accessRevokeSourceCatchUpDuration    prometheus.Gauge
	accessRevokeSourceFailOpenTotal      prometheus.Counter

	// Rate limit metrics
	rateLimitRejectsTotal          *prometheus.CounterVec
	rateLimitErrorDisconnectsTotal prometheus.Counter
	rateLimitUserEntries           prometheus.Gauge
	rateLimitRedisErrorsTotal      prometheus.Counter
	rateLimitRedisDuration         prometheus.Histogram

	// RPC method registry metrics
	rpcRegistryCallsTotal   *prometheus.CounterVec
	rpcRegistryErrorsTotal  *prometheus.CounterVec
	rpcRegistryReloadsTotal *prometheus.CounterVec
	rpcRegistryDedupTotal   *prometheus.CounterVec
	rpcRegistryRetransmits  *prometheus.CounterVec
	rpcRegistryDedupWait    prometheus.Histogram
	rpcRegistryDedupEntries prometheus.Gauge
}

// initForkMetrics creates the fork metrics in namespace with constLabels and returns their collectors for
// registration.
func (m *Registry) initForkMetrics(metricsNamespace string, constLabels prometheus.Labels) []prometheus.Collector {
	m.accessRevokeRejectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "rejects_total",
		Help:        "Number of connections or refreshes rejected by access revocation checks.",
		ConstLabels: constLabels,
	}, []string{"reason", "stage"})

	m.accessRevokeDisconnectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "disconnects_total",
		Help:        "Number of live connections disconnected by access revocation.",
		ConstLabels: constLabels,
	}, []string{"reason"})

	m.accessRevokeDisconnectsDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "disconnects_dropped_total",
		Help:        "Number of live disconnects dropped because the access revocation disconnect queue was full.",
		ConstLabels: constLabels,
	})

	m.accessRevokeStoreSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "store_size",
		Help:        "Number of entries in access revocation store by kind.",
		ConstLabels: constLabels,
	}, []string{"kind"})

	m.accessRevokeSourceReady = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_ready",
		Help:        "1 when the access revocation source is ready (caught up or failed open), 0 otherwise.",
		ConstLabels: constLabels,
	})

	m.accessRevokeSourceLagging = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_lagging",
		Help:        "1 when the access revocation source is lagging behind, 0 otherwise.",
		ConstLabels: constLabels,
	})

	m.accessRevokeSourceLagRecords = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_lag_records",
		Help:        "Number of records between the consumed offsets and last stable offsets of the access revocation source.",
		ConstLabels: constLabels,
	})

	m.accessRevokeSourceEventsAppliedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_events_applied_total",
		Help:        "Number of access revocation events applied from the source by kind.",
		ConstLabels: constLabels,
	}, []string{"kind"})

	m.accessRevokeSourceEventsSkippedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_events_skipped_total",
		Help:        "Number of access revocation events skipped by the source by reason.",
		ConstLabels: constLabels,
	}, []string{"reason"})

	m.accessRevokeSourceEventsBeyondWindow = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_events_beyond_window_total",
		Help:        "Number of applied access revocation events which expire later than the source window allows.",
		ConstLabels: constLabels,
	})

	m.accessRevokeSourceCatchUpDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_catch_up_duration_seconds",
		Help:        "Time in seconds from start until the access revocation source caught up for the first time.",
		ConstLabels: constLabels,
	})

	m.accessRevokeSourceFailOpenTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "access_revoke",
		Name:        "source_fail_open_total",
		Help:        "Number of times the node became ready before catching up with the access revocation source, after startup timeout.",
		ConstLabels: constLabels,
	})

	m.rateLimitRejectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rate_limit",
		Name:        "rejects_total",
		Help:        "Number of client commands rejected by rate limits.",
		ConstLabels: constLabels,
	}, []string{"limiter", "operation"})

	m.rateLimitErrorDisconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rate_limit",
		Name:        "error_disconnects_total",
		Help:        "Number of connections disconnected because of too many protocol errors.",
		ConstLabels: constLabels,
	})

	m.rateLimitUserEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rate_limit",
		Name:        "user_entries",
		Help:        "Number of per-user rate limit states kept in memory.",
		ConstLabels: constLabels,
	})

	m.rateLimitRedisErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rate_limit",
		Name:        "redis_errors_total",
		Help:        "Number of Redis errors of redis_user_command rate limits. Commands are allowed on such errors.",
		ConstLabels: constLabels,
	})

	m.rateLimitRedisDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "rate_limit",
		Name:      "redis_duration_seconds",
		Help:      "Duration of redis_user_command rate limit script calls.",
		Buckets: []float64{
			0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
		},
		ConstLabels: constLabels,
	})

	m.rpcRegistryCallsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "calls_total",
		Help:        "Number of RPC method registry calls by method and result.",
		ConstLabels: constLabels,
	}, []string{"rpc_method", "result"})

	m.rpcRegistryErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "errors_total",
		Help:        "Number of RPC method registry calls which failed with an application error, by method and application error type.",
		ConstLabels: constLabels,
	}, []string{"rpc_method", "app_type"})

	m.rpcRegistryReloadsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "reloads_total",
		Help:        "Number of RPC method registry reloads by trigger and result.",
		ConstLabels: constLabels,
	}, []string{"trigger", "result"})

	m.rpcRegistryDedupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "dedup_total",
		Help:        "Number of exactly-once RPC calls by outcome.",
		ConstLabels: constLabels,
	}, []string{"outcome"})

	m.rpcRegistryDedupWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "dedup_wait_seconds",
		Help:        "Time duplicate RPC calls waited for the call they joined.",
		ConstLabels: constLabels,
	})

	m.rpcRegistryRetransmits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "retransmit_total",
		Help:        "Number of RPC method registry calls which the client retransmitted, by method.",
		ConstLabels: constLabels,
	}, []string{"rpc_method"})

	m.rpcRegistryDedupEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "rpc_registry",
		Name:        "dedup_entries",
		Help:        "Number of exactly-once RPC answers kept by the node.",
		ConstLabels: constLabels,
	})

	return []prometheus.Collector{
		m.accessRevokeRejectsTotal,
		m.accessRevokeDisconnectsTotal,
		m.accessRevokeDisconnectsDroppedTotal,
		m.accessRevokeStoreSize,
		m.accessRevokeSourceReady,
		m.accessRevokeSourceLagging,
		m.accessRevokeSourceLagRecords,
		m.accessRevokeSourceEventsAppliedTotal,
		m.accessRevokeSourceEventsSkippedTotal,
		m.accessRevokeSourceEventsBeyondWindow,
		m.accessRevokeSourceCatchUpDuration,
		m.accessRevokeSourceFailOpenTotal,
		m.rateLimitRejectsTotal,
		m.rateLimitErrorDisconnectsTotal,
		m.rateLimitUserEntries,
		m.rateLimitRedisErrorsTotal,
		m.rateLimitRedisDuration,
		m.rpcRegistryCallsTotal,
		m.rpcRegistryErrorsTotal,
		m.rpcRegistryReloadsTotal,
		m.rpcRegistryDedupTotal,
		m.rpcRegistryDedupWait,
		m.rpcRegistryDedupEntries,
		m.rpcRegistryRetransmits,
	}
}

// ProxyCallChildren holds the proxy metric children of one proxy name. They are resolved once by
// NewProxyCallChildren, so a call does no label lookup.
type ProxyCallChildren struct {
	Duration prometheus.Observer
	Summary  prometheus.Observer
	Errors   prometheus.Counter
	Inflight prometheus.Gauge
}

// NewProxyCallChildren resolves the proxy metric children with the labels protocol, kind and name. It
// returns nil when the proxy metrics are not initialised.
func NewProxyCallChildren(protocol string, kind string, name string) *ProxyCallChildren {
	if ProxyCallDurationHistogram == nil || ProxyCallDurationSummary == nil ||
		ProxyCallErrorCount == nil || ProxyCallInflightRequests == nil {
		return nil
	}
	return &ProxyCallChildren{
		Duration: ProxyCallDurationHistogram.WithLabelValues(protocol, kind, name),
		Summary:  ProxyCallDurationSummary.WithLabelValues(protocol, kind, name),
		Errors:   ProxyCallErrorCount.WithLabelValues(protocol, kind, name),
		Inflight: ProxyCallInflightRequests.WithLabelValues(protocol, kind, name),
	}
}

// setForkMetrics assigns the fork metrics created by initForkMetrics to the package level variables.
func (m *Registry) setForkMetrics() {
	AccessRevokeRejectsTotal = m.accessRevokeRejectsTotal
	AccessRevokeDisconnectsTotal = m.accessRevokeDisconnectsTotal
	AccessRevokeDisconnectsDroppedTotal = m.accessRevokeDisconnectsDroppedTotal
	AccessRevokeStoreSize = m.accessRevokeStoreSize

	AccessRevokeSourceReady = m.accessRevokeSourceReady
	AccessRevokeSourceLagging = m.accessRevokeSourceLagging
	AccessRevokeSourceLagRecords = m.accessRevokeSourceLagRecords
	AccessRevokeSourceEventsAppliedTotal = m.accessRevokeSourceEventsAppliedTotal
	AccessRevokeSourceEventsSkippedTotal = m.accessRevokeSourceEventsSkippedTotal
	AccessRevokeSourceEventsBeyondWindow = m.accessRevokeSourceEventsBeyondWindow
	AccessRevokeSourceCatchUpDuration = m.accessRevokeSourceCatchUpDuration
	AccessRevokeSourceFailOpenTotal = m.accessRevokeSourceFailOpenTotal

	RateLimitRejectsTotal = m.rateLimitRejectsTotal
	RateLimitErrorDisconnectsTotal = m.rateLimitErrorDisconnectsTotal
	RateLimitUserEntries = m.rateLimitUserEntries
	RateLimitRedisErrorsTotal = m.rateLimitRedisErrorsTotal
	RateLimitRedisDuration = m.rateLimitRedisDuration

	RPCRegistryCallsTotal = m.rpcRegistryCallsTotal
	RPCRegistryErrorsTotal = m.rpcRegistryErrorsTotal
	RPCRegistryReloadsTotal = m.rpcRegistryReloadsTotal
	RPCRegistryDedupTotal = m.rpcRegistryDedupTotal
	RPCRegistryDedupWaitSeconds = m.rpcRegistryDedupWait
	RPCRegistryDedupEntries = m.rpcRegistryDedupEntries
	RPCRegistryRetransmitsTotal = m.rpcRegistryRetransmits
}
