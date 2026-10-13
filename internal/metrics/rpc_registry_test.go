package metrics

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// resetRPCRegistryMetrics replaces the registry counters and the app_type set for one test.
func resetRPCRegistryMetrics(t *testing.T) {
	t.Helper()
	previousCalls := RPCRegistryCallsTotal
	previousErrors := RPCRegistryErrorsTotal
	previousAppTypes := rpcRegistryAppTypes.values

	RPCRegistryCallsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "rpc_registry",
		Name:      "calls_total",
		Help:      "test",
	}, []string{"rpc_method", "result"})
	RPCRegistryErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "rpc_registry",
		Name:      "errors_total",
		Help:      "test",
	}, []string{"rpc_method", "app_type"})
	rpcRegistryAppTypes.values = map[string]struct{}{}

	t.Cleanup(func() {
		RPCRegistryCallsTotal = previousCalls
		RPCRegistryErrorsTotal = previousErrors
		rpcRegistryAppTypes.values = previousAppTypes
	})
}

func TestIncRPCRegistryCallCountsResults(t *testing.T) {
	resetRPCRegistryMetrics(t)

	IncRPCRegistryCall("auth.signIn", RPCRegistryResultOK)
	IncRPCRegistryCall("auth.signIn", RPCRegistryResultOK)
	IncRPCRegistryCall("auth.signIn", RPCRegistryResultTimeout)
	IncRPCRegistryCall("unknown", RPCRegistryResultUnknown)

	require.Equal(t, 2.0, testutil.ToFloat64(RPCRegistryCallsTotal.WithLabelValues("auth.signIn", "ok")))
	require.Equal(t, 1.0, testutil.ToFloat64(RPCRegistryCallsTotal.WithLabelValues("auth.signIn", "timeout")))
	require.Equal(t, 1.0, testutil.ToFloat64(RPCRegistryCallsTotal.WithLabelValues("unknown", "unknown")))
}

func TestIncRPCRegistryCallUnknownResultCountsAsError(t *testing.T) {
	resetRPCRegistryMetrics(t)

	IncRPCRegistryCall("messages.getHistory", "not-a-result")
	require.Equal(t, 1.0, testutil.ToFloat64(RPCRegistryCallsTotal.WithLabelValues("messages.getHistory", "error")))
}

func TestIncRPCRegistryErrorNormalisesAppType(t *testing.T) {
	resetRPCRegistryMetrics(t)

	IncRPCRegistryError("messages.sendMessage", "QUOTA_EXCEEDED_17")
	IncRPCRegistryError("messages.sendMessage", "QUOTA_EXCEEDED_30")
	IncRPCRegistryError("messages.sendMessage", "")

	require.Equal(t, 2.0, testutil.ToFloat64(RPCRegistryErrorsTotal.WithLabelValues("messages.sendMessage", "QUOTA_EXCEEDED_X")))
	require.Equal(t, 1.0, testutil.ToFloat64(RPCRegistryErrorsTotal.WithLabelValues("messages.sendMessage", "none")))
}

func TestIncRPCRegistryHelpersWithoutInit(t *testing.T) {
	previousCalls := RPCRegistryCallsTotal
	previousErrors := RPCRegistryErrorsTotal
	RPCRegistryCallsTotal = nil
	RPCRegistryErrorsTotal = nil
	t.Cleanup(func() {
		RPCRegistryCallsTotal = previousCalls
		RPCRegistryErrorsTotal = previousErrors
	})

	// Counters which are not initialised are skipped, nothing panics.
	IncRPCRegistryCall("auth.signIn", RPCRegistryResultOK)
	IncRPCRegistryError("auth.signIn", "QUOTA_EXCEEDED_17")
}

func TestNormalizeNumericSuffix(t *testing.T) {
	tests := map[string]string{
		"QUOTA_EXCEEDED_17":   "QUOTA_EXCEEDED_X",
		"QUOTA_EXCEEDED_0":    "QUOTA_EXCEEDED_X",
		"PLAIN":           "PLAIN",
		"TRAILING_":       "TRAILING_",
		"MIXED_1A":        "MIXED_1A",
		"NO_DIGITS_AB_1B": "NO_DIGITS_AB_1B",
		"_42":             "_X",
		"ONLY_NUMBERS_12": "ONLY_NUMBERS_X",
	}
	for input, want := range tests {
		require.Equal(t, want, normalizeNumericSuffix(input), input)
	}
}

func TestRPCRegistryAppTypeLabelCap(t *testing.T) {
	resetRPCRegistryMetrics(t)

	for i := 0; i < rpcRegistryMaxAppTypes; i++ {
		value := fmt.Sprintf("TYPE_%d_A", i)
		require.Equal(t, value, rpcRegistryAppTypeLabel(value))
	}

	// Once the cap is reached new values are reported as other, known values keep their label.
	require.Equal(t, rpcRegistryOtherAppType, rpcRegistryAppTypeLabel("BRAND_NEW_TYPE"))
	require.Equal(t, "TYPE_0_A", rpcRegistryAppTypeLabel("TYPE_0_A"))
	require.Len(t, rpcRegistryAppTypes.values, rpcRegistryMaxAppTypes)
}

func TestRPCRegistryAppTypeLabelCapCountsNormalisedValues(t *testing.T) {
	resetRPCRegistryMetrics(t)

	// Values which normalise to the same label take one slot.
	require.Equal(t, "ERR_X", rpcRegistryAppTypeLabel("ERR_1"))
	require.Equal(t, "ERR_X", rpcRegistryAppTypeLabel("ERR_2"))
	require.Len(t, rpcRegistryAppTypes.values, 1)
}
