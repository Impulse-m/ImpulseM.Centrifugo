package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// pushJobDurationBuckets are the buckets of the duration of a push job, from a single fast provider answer to a job
// which waits for its lease.
var pushJobDurationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Metrics of the push notification subsystem. They are assigned by setPushMetrics. Their label values are bounded: a
// provider, a credential and a kind are configuration values, an error code is the enumeration of the classifier, and
// a token, a user, a device id or a payload is never a label.
var (
	PushNotificationCount     *prometheus.CounterVec
	PushQueueConsumingLag     *prometheus.GaugeVec
	PushConsumingInflightJobs *prometheus.GaugeVec
	PushJobDurationSeconds    *prometheus.HistogramVec
	PushDevicesRemovedTotal   *prometheus.CounterVec
	PushRateLimitedTotal      *prometheus.CounterVec
	PushCredentialBreakerOpen *prometheus.GaugeVec
	PushEventsTotal           *prometheus.CounterVec
	PushIdempotencyHitsTotal  prometheus.Counter
	PushBusFailoverTotal      prometheus.Counter
	PushJobsLeaseExpiredTotal *prometheus.CounterVec
	PushJobsStaleResultsTotal prometheus.Counter
	PushJobsExpiredTotal      prometheus.Counter
)

// pushRegistry holds the metrics of the push notification subsystem. It is embedded into forkRegistry, so the metrics
// are created by initPushMetrics and assigned by setPushMetrics. The vectors have no children until a push feature
// records into them, so with push notifications disabled they export no series. The counters without labels are
// created at zero, as the other fork counters are, and export a zero value until a push feature records into them.
type pushRegistry struct {
	pushNotificationCount     *prometheus.CounterVec
	pushQueueConsumingLag     *prometheus.GaugeVec
	pushConsumingInflightJobs *prometheus.GaugeVec
	pushJobDurationSeconds    *prometheus.HistogramVec
	pushDevicesRemovedTotal   *prometheus.CounterVec
	pushRateLimitedTotal      *prometheus.CounterVec
	pushCredentialBreakerOpen *prometheus.GaugeVec
	pushEventsTotal           *prometheus.CounterVec
	pushIdempotencyHitsTotal  prometheus.Counter
	pushBusFailoverTotal      prometheus.Counter
	pushJobsLeaseExpiredTotal *prometheus.CounterVec
	pushJobsStaleResultsTotal prometheus.Counter
	pushJobsExpiredTotal      prometheus.Counter
}

// initPushMetrics creates the push metrics in namespace with constLabels and returns their collectors for
// registration.
func (m *Registry) initPushMetrics(metricsNamespace string, constLabels prometheus.Labels) []prometheus.Collector {
	m.pushNotificationCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "notification_count",
		Help:        "Number of push notifications sent to a provider, by provider, credential, recipient type, platform, kind, result and error code.",
		ConstLabels: constLabels,
	}, []string{"provider", "credential", "recipient_type", "platform", "kind", "success", "err_code"})

	m.pushQueueConsumingLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "queue_consuming_lag",
		Help:        "Number of push jobs waiting in a queue lane for a worker, by provider, queue and lane.",
		ConstLabels: constLabels,
	}, []string{"provider", "queue", "lane"})

	m.pushConsumingInflightJobs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "consuming_inflight_jobs",
		Help:        "Number of push jobs which a worker runs now, by provider, queue and lane.",
		ConstLabels: constLabels,
	}, []string{"provider", "queue", "lane"})

	m.pushJobDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "job_duration_seconds",
		Help:        "Duration of a push job, by provider, recipient type and kind of job.",
		Buckets:     pushJobDurationBuckets,
		ConstLabels: constLabels,
	}, []string{"provider", "recipient_type", "kind_of_job"})

	m.pushDevicesRemovedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "devices_removed_total",
		Help:        "Number of device registrations removed after a provider refused their token, by provider and reason.",
		ConstLabels: constLabels,
	}, []string{"provider", "reason"})

	m.pushRateLimitedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "rate_limited_total",
		Help:        "Number of push calls deferred by a rate limit, by provider and credential.",
		ConstLabels: constLabels,
	}, []string{"provider", "credential"})

	m.pushCredentialBreakerOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "credential_breaker_open",
		Help:        "1 when the circuit breaker of a provider credential is open, 0 otherwise.",
		ConstLabels: constLabels,
	}, []string{"provider", "credential"})

	m.pushEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "events_total",
		Help:        "Number of push events delivered to a sink, by sink, event type and result.",
		ConstLabels: constLabels,
	}, []string{"sink", "type", "result"})

	m.pushIdempotencyHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "idempotency_hits_total",
		Help:        "Number of push sends answered from an earlier send with the same idempotency key.",
		ConstLabels: constLabels,
	})

	m.pushBusFailoverTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "bus_failover_total",
		Help:        "Number of detected failovers of the push queue, after which the owner ran a discovery pass.",
		ConstLabels: constLabels,
	})

	m.pushJobsLeaseExpiredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "jobs_lease_expired_total",
		Help:        "Number of push jobs whose lease expired, by whether the job ended terminally.",
		ConstLabels: constLabels,
	}, []string{"terminal"})

	m.pushJobsStaleResultsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "jobs_stale_results_total",
		Help:        "Number of results of push jobs discarded because the job had been claimed again.",
		ConstLabels: constLabels,
	})

	m.pushJobsExpiredTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "jobs_expired_total",
		Help:        "Number of push jobs which ended because their expiry passed.",
		ConstLabels: constLabels,
	})

	return []prometheus.Collector{
		m.pushNotificationCount,
		m.pushQueueConsumingLag,
		m.pushConsumingInflightJobs,
		m.pushJobDurationSeconds,
		m.pushDevicesRemovedTotal,
		m.pushRateLimitedTotal,
		m.pushCredentialBreakerOpen,
		m.pushEventsTotal,
		m.pushIdempotencyHitsTotal,
		m.pushBusFailoverTotal,
		m.pushJobsLeaseExpiredTotal,
		m.pushJobsStaleResultsTotal,
		m.pushJobsExpiredTotal,
	}
}

// setPushMetrics assigns the push metrics created by initPushMetrics to the package level variables.
func (m *Registry) setPushMetrics() {
	PushNotificationCount = m.pushNotificationCount
	PushQueueConsumingLag = m.pushQueueConsumingLag
	PushConsumingInflightJobs = m.pushConsumingInflightJobs
	PushJobDurationSeconds = m.pushJobDurationSeconds
	PushDevicesRemovedTotal = m.pushDevicesRemovedTotal
	PushRateLimitedTotal = m.pushRateLimitedTotal
	PushCredentialBreakerOpen = m.pushCredentialBreakerOpen
	PushEventsTotal = m.pushEventsTotal
	PushIdempotencyHitsTotal = m.pushIdempotencyHitsTotal
	PushBusFailoverTotal = m.pushBusFailoverTotal
	PushJobsLeaseExpiredTotal = m.pushJobsLeaseExpiredTotal
	PushJobsStaleResultsTotal = m.pushJobsStaleResultsTotal
	PushJobsExpiredTotal = m.pushJobsExpiredTotal
}
