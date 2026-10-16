package metrics

import "github.com/prometheus/client_golang/prometheus"

// Proxy metrics - exported for use by proxy package.
//
// ProxyCallDurationSummary holds the legacy Summary instrument by default;
// when prometheus.native_histograms is enabled it is a no-op observer (see
// internal noopObserverVec), so callers can continue to construct cached
// observers and call .Observe() without nil-checks. ProxyCallDurationHistogram
// is the canonical companion — prefer it for new queries and OTel export.
var (
	ProxyCallDurationSummary   prometheus.ObserverVec
	ProxyCallDurationHistogram *prometheus.HistogramVec
	ProxyCallErrorCount        *prometheus.CounterVec
	ProxyCallInflightRequests  *prometheus.GaugeVec
)

// API metrics - exported for use by api package.
//
// APICommandDurationSummary follows the same pattern as ProxyCallDurationSummary
// (real Summary by default, no-op when native_histograms is enabled).
// APICommandDurationHistogram is the canonical companion.
//
// RPCDurationSummary follows the same pattern. RPCDurationHistogram is the
// companion Histogram exposed as "rpc_duration_seconds_histogram" —
// unconditional, with native (sparse, exponential) schema when
// native_histograms is enabled.
var (
	APICommandErrorsTotal       *prometheus.CounterVec
	APICommandDurationSummary   prometheus.ObserverVec
	APICommandDurationHistogram *prometheus.HistogramVec
	RPCDurationSummary          prometheus.ObserverVec
	RPCDurationHistogram        *prometheus.HistogramVec
)

// Consumer metrics - exported for use by consuming package
var (
	ConsumerProcessedTotal *prometheus.CounterVec
	ConsumerErrorsTotal    *prometheus.CounterVec
)

// Shared poll proxy metrics - exported for use by proxy package
var (
	SharedPollProxyRequestItems  *prometheus.HistogramVec
	SharedPollProxyResponseItems *prometheus.HistogramVec
)

// Middleware metrics - exported for use by middleware package
var (
	ConnLimitReached  prometheus.Counter
	HTTPRequestsTotal *prometheus.CounterVec
)

// Access revocation metrics - exported for use by accessrevoke and client packages
var (
	AccessRevokeRejectsTotal            *prometheus.CounterVec
	AccessRevokeDisconnectsTotal        *prometheus.CounterVec
	AccessRevokeDisconnectsDroppedTotal prometheus.Counter
	AccessRevokeStoreSize               *prometheus.GaugeVec

	AccessRevokeSourceReady              prometheus.Gauge
	AccessRevokeSourceLagging            prometheus.Gauge
	AccessRevokeSourceLagRecords         prometheus.Gauge
	AccessRevokeSourceEventsAppliedTotal *prometheus.CounterVec
	AccessRevokeSourceEventsSkippedTotal *prometheus.CounterVec
	AccessRevokeSourceEventsBeyondWindow prometheus.Counter
	AccessRevokeSourceCatchUpDuration    prometheus.Gauge
	AccessRevokeSourceFailOpenTotal      prometheus.Counter
)

// Rate limit metrics - exported for use by client package
var (
	RateLimitRejectsTotal          *prometheus.CounterVec
	RateLimitErrorDisconnectsTotal prometheus.Counter
	RateLimitUserEntries           prometheus.Gauge
	RateLimitRedisErrorsTotal      prometheus.Counter
	RateLimitRedisDuration         prometheus.Histogram
)

// RPC method registry metrics - exported for use by client and proxy packages
var (
	RPCRegistryCallsTotal  *prometheus.CounterVec
	RPCRegistryErrorsTotal *prometheus.CounterVec
)

// Postgres broker metrics exported for use by pgmapbroker and pgstreambroker.
//
// All carry a postgres_ name prefix: these are Postgres-specific (outbox,
// partitions, row cleanup), not generic broker concepts. Metrics emitted by both
// PG brokers are split per kind (broker_* for stream, map_broker_* for map) so
// the two never share a series. Cleanup is stream-only today.
var (
	BrokerPostgresCleanupRemovedTotal *prometheus.CounterVec

	BrokerPostgresOutboxCursorLagSeconds    *prometheus.GaugeVec
	MapBrokerPostgresOutboxCursorLagSeconds *prometheus.GaugeVec
	BrokerPostgresPartitions                *prometheus.GaugeVec
	MapBrokerPostgresPartitions             *prometheus.GaugeVec
)

// ChannelStateOldestFailingBatch holds the time, in Unix seconds, when the oldest channel state batch that
// the channel state proxy still rejects was first sent; 0 when no batch fails. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var ChannelStateOldestFailingBatch prometheus.Gauge
