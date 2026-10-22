package rpcregistry

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// useRegistryCallMetrics replaces the proxy call vectors and the method calls vector for one test, and
// restores the previous ones when it ends.
func useRegistryCallMetrics(t *testing.T) {
	t.Helper()
	previousHistogram := metrics.ProxyCallDurationHistogram
	previousSummary := metrics.ProxyCallDurationSummary
	previousErrors := metrics.ProxyCallErrorCount
	previousInflight := metrics.ProxyCallInflightRequests
	previousCalls := metrics.RPCRegistryCallsTotal
	t.Cleanup(func() {
		metrics.ProxyCallDurationHistogram = previousHistogram
		metrics.ProxyCallDurationSummary = previousSummary
		metrics.ProxyCallErrorCount = previousErrors
		metrics.ProxyCallInflightRequests = previousInflight
		metrics.RPCRegistryCallsTotal = previousCalls
	})

	proxyLabels := []string{"protocol", "kind", "name"}
	metrics.ProxyCallDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_proxy_duration_seconds",
	}, proxyLabels)
	metrics.ProxyCallDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "test_proxy_duration_summary",
	}, proxyLabels)
	metrics.ProxyCallErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_proxy_errors",
	}, proxyLabels)
	metrics.ProxyCallInflightRequests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_proxy_inflight",
	}, proxyLabels)
	metrics.RPCRegistryCallsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_rpc_registry_calls",
	}, []string{"rpc_method", "result"})
}

// seriesWithLabel returns how many series of the collector carry the label with the value.
func seriesWithLabel(t *testing.T, collector prometheus.Collector, label string, value string) int {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(collector))
	families, err := reg.Gather()
	require.NoError(t, err)
	count := 0
	for _, family := range families {
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					count++
					break
				}
			}
		}
	}
	return count
}

func TestReloadRemovesMetricSeriesOfRemovedTarget(t *testing.T) {
	useRegistryCallMetrics(t)
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reg.DrainTimeout = configtypes.Duration(time.Minute)
	reloadWith(t, r, reg)

	require.Equal(t, 1, seriesWithLabel(t, metrics.ProxyCallDurationHistogram, "name", "users"))
	require.Positive(t, seriesWithLabel(t, metrics.RPCRegistryCallsTotal, "rpc_method", testGetUserMethod))

	reg.Targets = []configtypes.RPCTarget{{Name: "files", Endpoint: "dns:///files:9000"}}
	reg.Methods = []configtypes.RPCMethod{
		{Method: "files.get", GRPCMethod: "/example.v1.FileService/Get", Target: "files", AuthClass: "anonymous"},
	}
	reloadWith(t, r, reg)

	require.Zero(t, seriesWithLabel(t, metrics.ProxyCallDurationHistogram, "name", "users"))
	require.Zero(t, seriesWithLabel(t, metrics.ProxyCallErrorCount, "name", "users"))
	require.Zero(t, seriesWithLabel(t, metrics.RPCRegistryCallsTotal, "rpc_method", testGetUserMethod))
	require.Equal(t, 1, seriesWithLabel(t, metrics.ProxyCallDurationHistogram, "name", "files"))
}

func TestReloadKeepsMetricSeriesOfReplacedTarget(t *testing.T) {
	useRegistryCallMetrics(t)
	dialer := &fakeDialer{}
	r := newRegistryWithDialer(t, dialer)
	reg := testRegistry()
	reg.DrainTimeout = configtypes.Duration(time.Minute)
	reloadWith(t, r, reg)

	reg.Targets[0].Endpoint = "dns:///users-new.example:9000"
	reloadWith(t, r, reg)

	require.Equal(t, 1, seriesWithLabel(t, metrics.ProxyCallDurationHistogram, "name", "users"))
	require.Positive(t, seriesWithLabel(t, metrics.RPCRegistryCallsTotal, "rpc_method", testGetUserMethod))
}
