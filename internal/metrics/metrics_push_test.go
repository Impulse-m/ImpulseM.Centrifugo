package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newPushTestRegistry creates the push metrics the way the fork registry does, in a registry of its own.
func newPushTestRegistry(t *testing.T) (*Registry, *prometheus.Registry) {
	t.Helper()

	m := &Registry{}
	reg := prometheus.NewPedanticRegistry()
	for _, collector := range m.initPushMetrics("test", nil) {
		require.NoError(t, reg.Register(collector))
	}
	return m, reg
}

func TestPushMetricsAcceptExactlyTheBoundedLabels(t *testing.T) {
	m, _ := newPushTestRegistry(t)

	_, err := m.pushNotificationCount.GetMetricWith(prometheus.Labels{
		"provider":       "provider",
		"credential":     "credential",
		"recipient_type": "recipient_type",
		"platform":       "platform",
		"kind":           "kind",
		"success":        "true",
		"err_code":       "err_code",
	})
	require.NoError(t, err)

	_, err = m.pushQueueConsumingLag.GetMetricWith(prometheus.Labels{"provider": "p", "queue": "q", "lane": "l"})
	require.NoError(t, err)

	_, err = m.pushConsumingInflightJobs.GetMetricWith(prometheus.Labels{"provider": "p", "queue": "q", "lane": "l"})
	require.NoError(t, err)

	_, err = m.pushJobDurationSeconds.GetMetricWith(prometheus.Labels{"provider": "p", "recipient_type": "r", "kind_of_job": "k"})
	require.NoError(t, err)

	_, err = m.pushDevicesRemovedTotal.GetMetricWith(prometheus.Labels{"provider": "p", "reason": "r"})
	require.NoError(t, err)

	_, err = m.pushRateLimitedTotal.GetMetricWith(prometheus.Labels{"provider": "p", "credential": "c"})
	require.NoError(t, err)

	_, err = m.pushCredentialBreakerOpen.GetMetricWith(prometheus.Labels{"provider": "p", "credential": "c"})
	require.NoError(t, err)

	_, err = m.pushEventsTotal.GetMetricWith(prometheus.Labels{"sink": "s", "type": "t", "result": "r"})
	require.NoError(t, err)

	_, err = m.pushJobsLeaseExpiredTotal.GetMetricWith(prometheus.Labels{"terminal": "true"})
	require.NoError(t, err)
}

func TestPushMetricsRejectLabelsOutsideTheSet(t *testing.T) {
	m, _ := newPushTestRegistry(t)

	_, err := m.pushRateLimitedTotal.GetMetricWith(prometheus.Labels{"provider": "p", "credential": "c", "user": "u"})
	require.Error(t, err)

	_, err = m.pushDevicesRemovedTotal.GetMetricWith(prometheus.Labels{"provider": "p"})
	require.Error(t, err)
}

func TestPushVectorsExportNoSeriesUntilRecorded(t *testing.T) {
	_, reg := newPushTestRegistry(t)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		require.NotContains(t, []string{
			"test_push_notification_count",
			"test_push_queue_consuming_lag",
			"test_push_consuming_inflight_jobs",
			"test_push_job_duration_seconds",
			"test_push_devices_removed_total",
			"test_push_rate_limited_total",
			"test_push_credential_breaker_open",
			"test_push_events_total",
			"test_push_jobs_lease_expired_total",
		}, family.GetName())
	}
}

func TestSetPushMetricsAssignsThePackageVariables(t *testing.T) {
	previousCount := PushNotificationCount
	previousFailover := PushBusFailoverTotal
	t.Cleanup(func() {
		PushNotificationCount = previousCount
		PushBusFailoverTotal = previousFailover
	})

	m, _ := newPushTestRegistry(t)
	m.setPushMetrics()

	require.Same(t, m.pushNotificationCount, PushNotificationCount)
	require.Same(t, m.pushBusFailoverTotal, PushBusFailoverTotal)
}
