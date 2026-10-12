package metrics

import (
	"errors"

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
}

// Registry holds all Centrifugo metrics.
type Registry struct {
	config Config

	// Proxy metrics
	proxyCallDurationSummary   *prometheus.SummaryVec
	proxyCallDurationHistogram *prometheus.HistogramVec
	proxyCallErrorCount        *prometheus.CounterVec
	proxyCallInflightRequests  *prometheus.GaugeVec

	// API metrics
	apiCommandErrorsTotal       *prometheus.CounterVec
	apiCommandDurationSummary   *prometheus.SummaryVec
	apiCommandDurationHistogram *prometheus.HistogramVec
	rpcDurationSummary          *prometheus.SummaryVec

	// Consumer metrics
	consumerProcessedTotal *prometheus.CounterVec
	consumerErrorsTotal    *prometheus.CounterVec

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

	ConsumerProcessedTotal = reg.consumerProcessedTotal
	ConsumerErrorsTotal = reg.consumerErrorsTotal

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
	m.proxyCallDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "proxy",
		Name:        "duration_seconds",
		Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:        "Duration of proxy call.",
		ConstLabels: constLabels,
	}, []string{"protocol", "type", "name"})

	m.proxyCallDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "proxy",
		Name:        "duration_seconds_histogram",
		Buckets:     prometheus.DefBuckets,
		Help:        "Histogram of duration of proxy call.",
		ConstLabels: constLabels,
	}, []string{"protocol", "type", "name"})

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

	m.apiCommandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "api",
		Name:        "command_duration_seconds",
		Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:        "Duration of API per command.",
		ConstLabels: constLabels,
	}, []string{"protocol", "method"})

	m.apiCommandDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "api",
		Buckets:     prometheus.DefBuckets,
		Name:        "command_duration_seconds_histogram",
		Help:        "Histogram of duration of API per command.",
		ConstLabels: constLabels,
	}, []string{"protocol", "method"})

	m.rpcDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "api",
		Name:        "rpc_duration_seconds",
		Objectives:  map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:        "Duration of API per command.",
		ConstLabels: constLabels,
	}, []string{"protocol", "method"})

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
		Help:        "Number of records between the consumed offsets and high watermarks of the access revocation source.",
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
		m.consumerProcessedTotal,
		m.consumerErrorsTotal,
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
