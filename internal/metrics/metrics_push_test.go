package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// pushFamilyNames returns the names of the exported families of reg which start with the push prefix.
func pushFamilyNames(t *testing.T, reg *prometheus.Registry) []string {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	var names []string
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "centrifugo_push_") {
			names = append(names, family.GetName())
		}
	}
	return names
}

func TestPushMetricsAbsentWhenDisabled(t *testing.T) {
	reg := prometheus.NewRegistry()

	err := Init(Config{Registerer: reg, PushEnabled: false})
	require.NoError(t, err)

	require.Empty(t, pushFamilyNames(t, reg))
}

func TestPushMetricsRegisteredWhenEnabled(t *testing.T) {
	reg := prometheus.NewRegistry()

	err := Init(Config{Registerer: reg, PushEnabled: true})
	require.NoError(t, err)

	// A vector exports a family only after a child exists, so create one child of each.
	PushNotificationCount.WithLabelValues("apns", "token", "ios", "true", "ok").Inc()
	PushDirectInflight.WithLabelValues("apns").Set(0)
	PushDirectRequests.WithLabelValues("completed").Inc()

	names := pushFamilyNames(t, reg)
	require.ElementsMatch(t, []string{
		"centrifugo_push_notification_count",
		"centrifugo_push_direct_inflight",
		"centrifugo_push_direct_requests_total",
	}, names)
}
