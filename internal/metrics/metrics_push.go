package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PushNotificationCount, PushDirectRequestsTotal and PushDirectInflight are assigned by setPushMetrics. They are nil
// while push notifications are disabled, so no push family is exported. Their label values are bounded: a provider,
// a result and a status class are enumerations, and a token, a device id, a ref, a payload or error text is never a
// label.
var (
	PushNotificationCount   *prometheus.CounterVec
	PushDirectRequestsTotal *prometheus.CounterVec
	PushDirectInflight      *prometheus.GaugeVec
)

// pushRegistry holds the push notification metrics. It is embedded into Registry, so the metrics are created by
// initPushMetrics and assigned by setPushMetrics, and only when Config.PushEnabled is set.
type pushRegistry struct {
	pushNotificationCount   *prometheus.CounterVec
	pushDirectRequestsTotal *prometheus.CounterVec
	pushDirectInflight      *prometheus.GaugeVec
}

// initPushMetrics creates the push metrics in namespace with constLabels and returns their collectors for
// registration. The first metric keeps the name and the label names of the PRO metric of the same name.
func (m *Registry) initPushMetrics(metricsNamespace string, constLabels prometheus.Labels) []prometheus.Collector {
	m.pushNotificationCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "notification_count",
		Help:        "Number of push notifications sent to a provider, by provider, recipient type, platform, success and error code.",
		ConstLabels: constLabels,
	}, []string{"provider", "recipient_type", "platform", "success", "err_code"})

	m.pushDirectRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "direct_requests_total",
		Help:        "Number of direct send calls, by result: completed or refused.",
		ConstLabels: constLabels,
	}, []string{"result"})

	m.pushDirectInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "direct_inflight",
		Help:        "Number of provider requests of direct send in flight, by provider.",
		ConstLabels: constLabels,
	}, []string{"provider"})

	return []prometheus.Collector{
		m.pushNotificationCount,
		m.pushDirectRequestsTotal,
		m.pushDirectInflight,
	}
}

// setPushMetrics assigns the push metrics created by initPushMetrics to the package level variables.
func (m *Registry) setPushMetrics() {
	PushNotificationCount = m.pushNotificationCount
	PushDirectRequestsTotal = m.pushDirectRequestsTotal
	PushDirectInflight = m.pushDirectInflight
}
