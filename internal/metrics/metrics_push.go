package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PushNotificationCount, PushDirectRequestsTotal and PushDirectInflight are assigned by initPushMetrics, which runs only
// when Config.PushEnabled is set. They are nil otherwise, so no push family is exported, and a caller nil-checks them
// before use: the direct send route of the sender must check PushDirectRequestsTotal and PushNotificationCount too.
// Their label values are bounded: a provider, a result and a status class are enumerations, and a token, a device id,
// a ref, a payload or error text is never a label.
var (
	PushNotificationCount   *prometheus.CounterVec
	PushDirectRequestsTotal *prometheus.CounterVec
	PushDirectInflight      *prometheus.GaugeVec
)

// initPushMetrics creates the push metrics in namespace with constLabels, assigns them to the package variables and
// returns their collectors for registration. The first metric keeps the name and the label names of the PRO metric of
// the same name.
func initPushMetrics(metricsNamespace string, constLabels prometheus.Labels) []prometheus.Collector {
	pushNotificationCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "notification_count",
		Help:        "Number of push notifications sent to a provider, by provider, recipient type, platform, success and error code.",
		ConstLabels: constLabels,
	}, []string{"provider", "recipient_type", "platform", "success", "err_code"})

	pushDirectRequestsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "direct_requests_total",
		Help:        "Number of direct send calls, by result: completed or refused.",
		ConstLabels: constLabels,
	}, []string{"result"})

	pushDirectInflight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   metricsNamespace,
		Subsystem:   "push",
		Name:        "direct_inflight",
		Help:        "Number of provider requests of direct send in flight, by provider.",
		ConstLabels: constLabels,
	}, []string{"provider"})

	PushNotificationCount = pushNotificationCount
	PushDirectRequestsTotal = pushDirectRequestsTotal
	PushDirectInflight = pushDirectInflight

	return []prometheus.Collector{
		pushNotificationCount,
		pushDirectRequestsTotal,
		pushDirectInflight,
	}
}
