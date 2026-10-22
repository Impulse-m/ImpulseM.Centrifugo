package metrics

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The set of dropped names is bounded: a drop beyond rpcRegistryDroppedMax starts the set again, and the name which
// was dropped last is in it.
func TestDroppedMethodNamesBounded(t *testing.T) {
	resetRPCRegistryMetrics(t)
	t.Cleanup(func() {
		rpcRegistryDropped.names.Store(nil)
	})
	for i := range rpcRegistryDroppedMax + 1 {
		DropRPCRegistryMethodSeries(fmt.Sprintf("drop.test.bounded.%d", i))
	}
	require.LessOrEqual(t, len(*rpcRegistryDropped.names.Load()), rpcRegistryDroppedMax)
	require.True(t, rpcRegistryDropped.has(fmt.Sprintf("drop.test.bounded.%d", rpcRegistryDroppedMax)))
}

// A call of a method which a reload dropped, and which finishes after the drop, does not create its series again.
// The method counts again once it is built again.
func TestDroppedMethodCountsAreSkipped(t *testing.T) {
	resetRPCRegistryMetrics(t)
	const method = "drop.test.method"

	m := NewRPCRegistryMethodMetrics(method)
	IncRPCRegistryError(method, "")
	require.Equal(t, 1, testutil.CollectAndCount(RPCRegistryErrorsTotal))

	DropRPCRegistryMethodSeries(method)
	require.Zero(t, testutil.CollectAndCount(RPCRegistryErrorsTotal))
	require.Zero(t, testutil.CollectAndCount(RPCRegistryCallsTotal))

	m.ExpiredRefusal(method)
	IncRPCRegistryError(method, "")
	require.Zero(t, testutil.CollectAndCount(RPCRegistryErrorsTotal), "a count of a dropped method is skipped")
	require.Zero(t, testutil.CollectAndCount(RPCRegistryCallsTotal), "a refusal of a dropped method is skipped")

	NewRPCRegistryMethodMetrics(method)
	IncRPCRegistryError(method, "")
	require.Equal(t, 1, testutil.CollectAndCount(RPCRegistryErrorsTotal), "a method which is built again counts")
}
