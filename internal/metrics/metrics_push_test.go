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

func TestPushVectorsHaveExactlyTheSpecLabels(t *testing.T) {
	m, _ := newPushTestRegistry(t)

	vectors := []struct {
		name   string
		labels []string
		with   func(prometheus.Labels) error
	}{
		{
			name:   "push_notification_count",
			labels: []string{"provider", "credential", "recipient_type", "platform", "kind", "success", "err_code"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushNotificationCount.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_queue_consuming_lag",
			labels: []string{"provider", "queue", "lane"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushQueueConsumingLag.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_consuming_inflight_jobs",
			labels: []string{"provider", "queue", "lane"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushConsumingInflightJobs.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_job_duration_seconds",
			labels: []string{"provider", "recipient_type", "kind_of_job"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushJobDurationSeconds.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_devices_removed_total",
			labels: []string{"provider", "reason"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushDevicesRemovedTotal.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_rate_limited_total",
			labels: []string{"provider", "credential"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushRateLimitedTotal.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_credential_breaker_open",
			labels: []string{"provider", "credential"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushCredentialBreakerOpen.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_events_total",
			labels: []string{"sink", "type", "result"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushEventsTotal.GetMetricWith(labels)
				return err
			},
		},
		{
			name:   "push_jobs_lease_expired_total",
			labels: []string{"terminal"},
			with: func(labels prometheus.Labels) error {
				_, err := m.pushJobsLeaseExpiredTotal.GetMetricWith(labels)
				return err
			},
		},
	}

	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			full := prometheus.Labels{}
			for _, label := range vector.labels {
				full[label] = "value"
			}
			require.NoError(t, vector.with(full))

			for _, dropped := range vector.labels {
				missing := prometheus.Labels{}
				for label, value := range full {
					if label != dropped {
						missing[label] = value
					}
				}
				require.Error(t, vector.with(missing), "label %q must be required", dropped)
			}

			extra := prometheus.Labels{"user": "value"}
			for label, value := range full {
				extra[label] = value
			}
			require.Error(t, vector.with(extra), "a label outside the spec set must be refused")
		})
	}
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
