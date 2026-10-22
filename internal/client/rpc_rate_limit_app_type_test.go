package client

import (
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// rateLimitedPlaceholderRegistry returns the test registry whose rate limited error has the app type appType.
func rateLimitedPlaceholderRegistry(appType string) configtypes.RPCRegistry {
	reg := rateLimitedRegistry()
	reg.Errors.RateLimited.AppType = appType
	return reg
}

// The placeholder of the app type takes the same whole seconds as the retry-after metadata of the refusal.
func TestRPCRefusalAppTypeExpandsRetryAfter(t *testing.T) {
	h := newDedupHandler(t, rateLimitedPlaceholderRegistry("QUOTA_WAIT_{retry_after}"), newDedupBackend(t))

	refused := h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", 1200*time.Millisecond).(*centrifuge.Error)
	require.Equal(t, "QUOTA_WAIT_2", refused.AppType)
	require.Equal(t, map[string]string{"x-centrifugo-retry-after": "2"}, refused.Metadata)

	minute := h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", time.Minute).(*centrifuge.Error)
	require.Equal(t, "QUOTA_WAIT_60", minute.AppType)

	// The configured app type keeps its placeholder, so the next refusal expands it again.
	require.Equal(t, "QUOTA_WAIT_{retry_after}", h.registryErrors().RateLimited.AppType)
}

// A placeholder which is not in the app type leaves it as it is, and a sub-second wait gives 1.
func TestRPCRefusalAppTypeWithoutPlaceholderUnchanged(t *testing.T) {
	h := newDedupHandler(t, rateLimitedPlaceholderRegistry("example.throttle"), newDedupBackend(t))

	refused := h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", time.Millisecond).(*centrifuge.Error)
	require.Equal(t, "example.throttle", refused.AppType)
	require.Equal(t, map[string]string{"x-centrifugo-retry-after": "1"}, refused.Metadata)
}

// The expanded app type reaches the client, and matches the wait in its retry-after metadata.
func TestRPCRefusalAppTypeOverWire(t *testing.T) {
	_, node := newRPCRateLimitNode(t, rpcRateLimitOf(time.Minute, 1), rateLimitedPlaceholderRegistry("QUOTA_WAIT_{retry_after}"))
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "users.get"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "users.get"))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	requireRetryAfterShape(t, reply.Error.Metadata, 60)
	seconds := reply.Error.Metadata["x-centrifugo-retry-after"]
	_, err := strconv.Atoi(seconds)
	require.NoError(t, err)
	require.Equal(t, "QUOTA_WAIT_"+seconds, reply.Error.AppType)
}

// The errors counter counts the expanded app type, normalised: QUOTA_WAIT_17 and QUOTA_WAIT_2 share QUOTA_WAIT_X.
func TestRPCRefusalAppTypeCounted(t *testing.T) {
	h := newDedupHandler(t, rateLimitedPlaceholderRegistry("QUOTA_WAIT_{retry_after}"), newDedupBackend(t))
	before := testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues("users.get", "QUOTA_WAIT_X"))

	_ = h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", 17*time.Second)
	_ = h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", 2*time.Second)

	require.Equal(t, before+2, testutil.ToFloat64(metrics.RPCRegistryErrorsTotal.WithLabelValues("users.get", "QUOTA_WAIT_X")))
}
