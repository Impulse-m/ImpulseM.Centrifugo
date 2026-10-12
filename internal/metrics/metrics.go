package metrics

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const defaultMetricsNamespace = "centrifugo"

// Config contains metrics configuration.
type Config struct {
	// Namespace is the prometheus namespace for all metrics. If empty, defaults to "centrifugo".
	Namespace string
	// ConstLabels are labels that will be added to all metrics as constant labels.
	// These are useful for adding environment, region, or other deployment-specific labels.
	ConstLabels map[string]string
	// Registerer is the prometheus registerer to use. If nil, prometheus.DefaultRegisterer is used.
	Registerer prometheus.Registerer
	// NativeHistograms switches Histogram instruments to Prometheus native
	// (sparse, exponential) schema with no explicit buckets exposed. Designed
	// for OpenTelemetry export via the client_golang Prometheus bridge.
	// Text-format scrapes lose _bucket series; use protobuf scrape format.
	NativeHistograms bool
}

// nativeHistogramOpts returns opts unchanged when native is false. When true,
// it enables Prometheus native histogram schema with no explicit buckets —
// the metric exposes only _count, _sum, and the native histogram chunk.
func nativeHistogramOpts(opts prometheus.HistogramOpts, native bool) prometheus.HistogramOpts {
	if !native {
		return opts
	}
	opts.Buckets = nil
	opts.NativeHistogramBucketFactor = 1.1
	opts.NativeHistogramMaxBucketNumber = 200
	opts.NativeHistogramMinResetDuration = time.Hour
	return opts
}

// noopObserverVec implements prometheus.ObserverVec with all no-op methods.
// Assigned to Summary accessors when NativeHistograms is true so that
// callers (proxy handlers, helpers) can still construct cached observers
// and call .Observe() without nil checks — the calls become no-ops and no
// metric data is recorded for the disabled Summary instrument.
type noopObserverVec struct{}

func (noopObserverVec) Describe(chan<- *prometheus.Desc)              {}
func (noopObserverVec) Collect(chan<- prometheus.Metric)              {}
func (noopObserverVec) WithLabelValues(...string) prometheus.Observer { return noopObserver{} }
func (noopObserverVec) With(prometheus.Labels) prometheus.Observer    { return noopObserver{} }
func (noopObserverVec) GetMetricWith(prometheus.Labels) (prometheus.Observer, error) {
	return noopObserver{}, nil
}
func (noopObserverVec) GetMetricWithLabelValues(...string) (prometheus.Observer, error) {
	return noopObserver{}, nil
}
func (noopObserverVec) CurryWith(prometheus.Labels) (prometheus.ObserverVec, error) {
	return noopObserverVec{}, nil
}
func (noopObserverVec) MustCurryWith(prometheus.Labels) prometheus.ObserverVec {
	return noopObserverVec{}
}

type noopObserver struct{}

func (noopObserver) Observe(float64) {}

// Registry holds all Centrifugo metrics.
type Registry struct {
	config Config

	// Proxy metrics
	proxyCallDurationSummary   prometheus.ObserverVec
	proxyCallDurationHistogram *prometheus.HistogramVec
	proxyCallErrorCount        *prometheus.CounterVec
	proxyCallInflightRequests  *prometheus.GaugeVec

	// API metrics
	apiCommandErrorsTotal       *prometheus.CounterVec
	apiCommandDurationSummary   prometheus.ObserverVec
	apiCommandDurationHistogram *prometheus.HistogramVec
	// rpcDurationSummary is the legacy Summary by default; no-op when
	// NativeHistograms is true. rpcDurationHistogram is the new companion
	// Histogram exposed as "rpc_duration_seconds_histogram" — unconditional,
	// with native schema when NativeHistograms is true.
	rpcDurationSummary   prometheus.ObserverVec
	rpcDurationHistogram *prometheus.HistogramVec

	// Consumer metrics
	consumerProcessedTotal *prometheus.CounterVec
	consumerErrorsTotal    *prometheus.CounterVec

	// Shared poll proxy metrics
	sharedPollProxyRequestItems  *prometheus.HistogramVec
	sharedPollProxyResponseItems *prometheus.HistogramVec

	// Middleware metrics
	connLimitReached  prometheus.Counter
	httpRequestsTotal *prometheus.CounterVec

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

	// PostgreSQL-specific broker metrics. Cleanup is Postgres-specific here (only
	// the PG stream broker prunes rows, with PG table names in the "pass" label),
	// so it carries the postgres_ prefix like the rest. Metrics emitted by both
	// PG brokers are split per kind so stream and map never share a series.
	brokerPostgresCleanupRemovedTotal       *prometheus.CounterVec
	brokerPostgresOutboxCursorLagSeconds    *prometheus.GaugeVec
	mapBrokerPostgresOutboxCursorLagSeconds *prometheus.GaugeVec
	brokerPostgresPartitions                *prometheus.GaugeVec
	mapBrokerPostgresPartitions             *prometheus.GaugeVec
}

// Init initializes the metrics registry with the provided configuration.
// It creates all metrics and registers them with the provided registerer.
// If registerer is nil, prometheus.DefaultRegisterer is used.
// Returns an error if metric registration fails.
func Init(cfg Config) error {
	reg, err := newRegistry(cfg)
	if err != nil {
		return err
	}

	// Populate exported variables for backward compatibility
	ProxyCallDurationSummary = reg.proxyCallDurationSummary
	ProxyCallDurationHistogram = reg.proxyCallDurationHistogram
	ProxyCallErrorCount = reg.proxyCallErrorCount
	ProxyCallInflightRequests = reg.proxyCallInflightRequests

	APICommandErrorsTotal = reg.apiCommandErrorsTotal
	APICommandDurationSummary = reg.apiCommandDurationSummary
	APICommandDurationHistogram = reg.apiCommandDurationHistogram
	RPCDurationSummary = reg.rpcDurationSummary
	RPCDurationHistogram = reg.rpcDurationHistogram

	ConsumerProcessedTotal = reg.consumerProcessedTotal
	ConsumerErrorsTotal = reg.consumerErrorsTotal

	SharedPollProxyRequestItems = reg.sharedPollProxyRequestItems
	SharedPollProxyResponseItems = reg.sharedPollProxyResponseItems

	ConnLimitReached = reg.connLimitReached
	HTTPRequestsTotal = reg.httpRequestsTotal

	AccessRevokeRejectsTotal = reg.accessRevokeRejectsTotal
	AccessRevokeDisconnectsTotal = reg.accessRevokeDisconnectsTotal
	AccessRevokeDisconnectsDroppedTotal = reg.accessRevokeDisconnectsDroppedTotal
	AccessRevokeStoreSize = reg.accessRevokeStoreSize

	AccessRevokeSourceReady = reg.accessRevokeSourceReady
	AccessRevokeSourceLagging = reg.accessRevokeSourceLagging
	AccessRevokeSourceLagRecords = reg.accessRevokeSourceLagRecords
	AccessRevokeSourceEventsAppliedTotal = reg.accessRevokeSourceEventsAppliedTotal
	AccessRevokeSourceEventsSkippedTotal = reg.accessRevokeSourceEventsSkippedTotal
	AccessRevokeSourceEventsBeyondWindow = reg.accessRevokeSourceEventsBeyondWindow
	AccessRevokeSourceCatchUpDuration = reg.accessRevokeSourceCatchUpDuration
	AccessRevokeSourceFailOpenTotal = reg.accessRevokeSourceFailOpenTotal

	RateLimitRejectsTotal = reg.rateLimitRejectsTotal
	RateLimitErrorDisconnectsTotal = reg.rateLimitErrorDisconnectsTotal
	RateLimitUserEntries = reg.rateLimitUserEntries
	RateLimitRedisErrorsTotal = reg.rateLimitRedisErrorsTotal
	RateLimitRedisDuration = reg.rateLimitRedisDuration
	BrokerPostgresCleanupRemovedTotal = reg.brokerPostgresCleanupRemovedTotal
	BrokerPostgresOutboxCursorLagSeconds = reg.brokerPostgresOutboxCursorLagSeconds
	MapBrokerPostgresOutboxCursorLagSeconds = reg.mapBrokerPostgresOutboxCursorLagSeconds
	BrokerPostgresPartitions = reg.brokerPostgresPartitions
	MapBrokerPostgresPartitions = reg.mapBrokerPostgresPartitions

	return nil
}

func newRegistry(cfg Config) (*Registry, error) {
	registerer := cfg.Registerer
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	metricsNamespace := cfg.Namespace
	if metricsNamespace == "" {
		metricsNamespace = defaultMetricsNamespace
	}

	constLabels := prometheus.Labels(cfg.ConstLabels)

	m := &Registry{
		config: cfg,
	}

	// Proxy metrics
	if cfg.NativeHistograms {
		m.proxyCallDurationSummary = noopObserverVec{}
	} else {
		m.proxyCallDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   metricsNamespace,
			Subsystem:   "proxy",
			Name:        "duration_seconds",
			Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
			Help:        "DEPRECATED — use centrifugo_proxy_duration_seconds_histogram. Will be removed in Centrifugo v7. Duration of proxy call.",
			ConstLabels: constLabels,
		}, []string{"protocol", "type", "name"})
	}

	m.proxyCallDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "proxy",
		Name:        "duration_seconds_histogram",
		Buckets:     prometheus.DefBuckets,
		Help:        "Histogram of duration of proxy call.",
		ConstLabels: constLabels,
	}, cfg.NativeHistograms), []string{"protocol", "type", "name"})

	m.proxyCallErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "proxy",
		Name:        "errors",
		Help:        "Proxy call error count.",
		ConstLabels: constLabels,
	}, []string{"protocol", "type", "name"})

	m.proxyCallInflightRequests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "proxy",
		Name:        "inflight_requests",
		Help:        "Number of inflight proxy requests.",
		ConstLabels: constLabels,
	}, []string{"protocol", "type", "name"})

	// API metrics
	m.apiCommandErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "api",
		Name:        "command_errors_total",
		Help:        "Total errors in API commands.",
		ConstLabels: constLabels,
	}, []string{"protocol", "method", "error"})

	if cfg.NativeHistograms {
		m.apiCommandDurationSummary = noopObserverVec{}
	} else {
		m.apiCommandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   metricsNamespace,
			Subsystem:   "api",
			Name:        "command_duration_seconds",
			Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
			Help:        "DEPRECATED — use centrifugo_api_command_duration_seconds_histogram. Will be removed in Centrifugo v7. Duration of API per command.",
			ConstLabels: constLabels,
		}, []string{"protocol", "method"})
	}

	m.apiCommandDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "api",
		Buckets:     prometheus.DefBuckets,
		Name:        "command_duration_seconds_histogram",
		Help:        "Histogram of duration of API per command.",
		ConstLabels: constLabels,
	}, cfg.NativeHistograms), []string{"protocol", "method"})

	if cfg.NativeHistograms {
		m.rpcDurationSummary = noopObserverVec{}
	} else {
		m.rpcDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   metricsNamespace,
			Subsystem:   "api",
			Name:        "rpc_duration_seconds",
			Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
			Help:        "DEPRECATED — use centrifugo_api_rpc_duration_seconds_histogram. Will be removed in Centrifugo v7. Duration of API per command.",
			ConstLabels: constLabels,
		}, []string{"protocol", "method"})
	}

	m.rpcDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "api",
		Name:        "rpc_duration_seconds_histogram",
		Buckets:     prometheus.DefBuckets,
		Help:        "Histogram of duration of API per command.",
		ConstLabels: constLabels,
	}, cfg.NativeHistograms), []string{"protocol", "method"})

	// Consumer metrics
	m.consumerProcessedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "consumers",
		Name:        "messages_processed_total",
		Help:        "Total number of processed messages in consumer",
		ConstLabels: constLabels,
	}, []string{"consumer_name"})

	m.consumerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "consumers",
		Name:        "errors_total",
		Help:        "Total number of errors in consumer",
		ConstLabels: constLabels,
	}, []string{"consumer_name"})

	// Shared poll proxy metrics
	m.sharedPollProxyRequestItems = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "shared_poll_proxy",
		Name:        "request_items",
		Help:        "Number of items per shared poll proxy request.",
		Buckets:     []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		ConstLabels: constLabels,
	}, cfg.NativeHistograms), []string{"name"})

	m.sharedPollProxyResponseItems = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "shared_poll_proxy",
		Name:        "response_items",
		Help:        "Number of items per shared poll proxy response.",
		Buckets:     []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		ConstLabels: constLabels,
	}, cfg.NativeHistograms), []string{"name"})

	// Middleware metrics
	m.connLimitReached = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "node",
		Name:        "client_connection_limit",
		Help:        "Number of refused requests due to node client connection limit.",
		ConstLabels: constLabels,
	})

	m.httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   metricsNamespace,
			Subsystem:   "node",
			Name:        "incoming_http_requests_total",
			Help:        "Number of incoming HTTP requests",
			ConstLabels: constLabels,
		},
		[]string{"path", "method", "status"},
	)

	// Access revocation metrics
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
	// Postgres-specific broker metrics carry a postgres_ name prefix. Cleanup is
	// Postgres-specific here: only the PG stream broker prunes rows (the "pass"
	// label is the PG table being cleaned), so it is not the generic cleanup
	// metric. Metrics emitted by both PG brokers are split per kind below.
	m.brokerPostgresCleanupRemovedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "broker",
		Name:        "postgres_cleanup_removed_total",
		Help:        "Total rows removed by each Postgres cleanup pass.",
		ConstLabels: constLabels,
	}, []string{"broker_name", "pass"})

	m.brokerPostgresOutboxCursorLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "broker",
		Name:        "postgres_outbox_cursor_lag_seconds",
		Help:        "Time between the outbox cursor's row created_at and now, per shard.",
		ConstLabels: constLabels,
	}, []string{"broker_name", "shard"})

	m.mapBrokerPostgresOutboxCursorLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "map_broker",
		Name:        "postgres_outbox_cursor_lag_seconds",
		Help:        "Time between the outbox cursor's row created_at and now, per shard.",
		ConstLabels: constLabels,
	}, []string{"broker_name", "shard"})

	m.brokerPostgresPartitions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "broker",
		Name:        "postgres_partitions",
		Help:        "Count of stream/history table partitions.",
		ConstLabels: constLabels,
	}, []string{"broker_name"})

	m.mapBrokerPostgresPartitions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "map_broker",
		Name:        "postgres_partitions",
		Help:        "Count of stream/history table partitions.",
		ConstLabels: constLabels,
	}, []string{"broker_name"})

	// Register all metrics
	var alreadyRegistered prometheus.AlreadyRegisteredError

	collectors := []prometheus.Collector{
		m.proxyCallDurationSummary,
		m.proxyCallDurationHistogram,
		m.proxyCallErrorCount,
		m.proxyCallInflightRequests,
		m.apiCommandErrorsTotal,
		m.apiCommandDurationSummary,
		m.apiCommandDurationHistogram,
		m.rpcDurationSummary,
		m.rpcDurationHistogram,
		m.consumerProcessedTotal,
		m.consumerErrorsTotal,
		m.sharedPollProxyRequestItems,
		m.sharedPollProxyResponseItems,
		m.connLimitReached,
		m.httpRequestsTotal,
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
		m.brokerPostgresCleanupRemovedTotal,
		m.brokerPostgresOutboxCursorLagSeconds,
		m.mapBrokerPostgresOutboxCursorLagSeconds,
		m.brokerPostgresPartitions,
		m.mapBrokerPostgresPartitions,
	}

	for _, collector := range collectors {
		err := registerer.Register(collector)
		if err != nil {
			// Ignore if already registered (allows re-initialization in tests)
			if !errors.As(err, &alreadyRegistered) {
				return nil, err
			}
		}
	}

	return m, nil
}
