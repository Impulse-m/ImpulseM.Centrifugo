package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// newRPCRateLimitNode returns a handler and a real node whose registry is reg and whose rate limits are rl.
func newRPCRateLimitNode(t *testing.T, rl configtypes.RateLimit, reg configtypes.RPCRegistry) (*Handler, *centrifuge.Node) {
	t.Helper()
	backend := newDedupBackend(t)
	registry := startDedupRegistry(t, reg, backend.conn(t))
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Client.RateLimit = rl
	cfg.RPC.Registry = reg
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, emptyJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	h.SetRPCRegistry(registry)
	require.NoError(t, h.Setup())
	return h, node
}

// rpcRateLimitOf returns client command limits of RPC calls: rate calls per interval.
func rpcRateLimitOf(interval time.Duration, rate int) configtypes.RateLimit {
	return configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				RPC: configtypes.RateLimitRPCBuckets{Enabled: true, Buckets: rateLimitBucketsOf(interval, rate).Buckets},
			},
		},
	}
}

// rateLimitedRegistry returns the test registry with rpc.registry.errors.rate_limited set.
func rateLimitedRegistry() configtypes.RPCRegistry {
	reg := registryTestRegistryConfig()
	reg.Errors.RateLimited = configtypes.RPCRegistryError{Code: 417, Message: "throttled", AppCode: 17, AppType: "example.throttle"}
	return reg
}

func rpcRateLimitCommand(id uint32, method string) *protocol.Command {
	return &protocol.Command{Id: id, Rpc: &protocol.RPCRequest{Method: method}}
}

// The wait of 1.2 s is a little under 1.2 s on the wire, so the retry-after value is 2: the rounding up is seen here.
func TestRPCRefusalConfiguredError(t *testing.T) {
	reg := rateLimitedRegistry()
	reg.Errors.RateLimited.Code = 400
	_, node := newRPCRateLimitNode(t, rpcRateLimitOf(1200*time.Millisecond, 1), reg)
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "users.get"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "users.get"))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	require.NotNil(t, reply.Error)
	require.Equal(t, uint32(400), reply.Error.Code)
	require.Equal(t, "throttled", reply.Error.Message)
	require.True(t, reply.Error.Temporary)
	require.Equal(t, int32(17), reply.Error.AppCode)
	require.Equal(t, "example.throttle", reply.Error.AppType)
	require.Equal(t, map[string]string{"x-centrifugo-retry-after": "2"}, reply.Error.Metadata)
}

// A whole-minute wait keeps its exact value on the wire.
func TestRPCRefusalWholeMinuteWait(t *testing.T) {
	_, node := newRPCRateLimitNode(t, rpcRateLimitOf(time.Minute, 1), rateLimitedRegistry())
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "users.get"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "users.get"))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	require.NotNil(t, reply.Error)
	require.Equal(t, uint32(417), reply.Error.Code)
	require.Equal(t, "throttled", reply.Error.Message)
	require.True(t, reply.Error.Temporary)
	require.Equal(t, int32(17), reply.Error.AppCode)
	require.Equal(t, "example.throttle", reply.Error.AppType)
	// The bucket refills one call per minute, so the wait is a little under a minute, rounded up to a minute.
	require.Equal(t, map[string]string{"x-centrifugo-retry-after": "60"}, reply.Error.Metadata)
}

func TestRPCRefusalDefaultIs111(t *testing.T) {
	_, node := newRPCRateLimitNode(t, rpcRateLimitOf(time.Minute, 1), registryTestRegistryConfig())
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "users.get"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "users.get"))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	require.True(t, isRateLimited(reply))
	require.Empty(t, reply.Error.Metadata)
}

func TestRPCRefusalNotRegisteredMethod(t *testing.T) {
	_, node := newRPCRateLimitNode(t, rpcRateLimitOf(time.Minute, 1), rateLimitedRegistry())
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "not.registered"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "not.registered"))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	require.True(t, isRateLimited(reply))
	require.Empty(t, reply.Error.Metadata)
}

func TestRPCRefusalRegistryDisabled(t *testing.T) {
	reg := rateLimitedRegistry()
	reg.Enabled = false
	_, node := newRPCRateLimitNode(t, rpcRateLimitOf(time.Minute, 1), reg)
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "users.get"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "users.get"))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	require.True(t, isRateLimited(reply))
	require.Empty(t, reply.Error.Metadata)
}

func TestRPCRefusalNonRPCUnchanged(t *testing.T) {
	rl := configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				History: rateLimitChannelOf(time.Minute, 1),
			},
		},
	}
	_, node := newRPCRateLimitNode(t, rl, rateLimitedRegistry())
	client, transport, _ := connectTestClient(t, node, 0)

	history := func(id uint32) *protocol.Command {
		return &protocol.Command{Id: id, History: &protocol.HistoryRequest{Channel: "chat"}}
	}
	sendRateLimitCommand(client, history(2))
	sendRateLimitCommand(client, history(3))
	transport.waitReply(t, 2)
	reply := transport.waitReply(t, 3)

	require.True(t, isRateLimited(reply))
	require.Empty(t, reply.Error.Metadata)
}

// The configured error is read from the running config at each refusal, so a reload applies to the next one.
func TestRPCRefusalReloadAppliesNext(t *testing.T) {
	h, node := newRPCRateLimitNode(t, rpcRateLimitOf(time.Minute, 1), rateLimitedRegistry())
	client, transport, _ := connectTestClient(t, node, 0)

	sendRateLimitCommand(client, rpcRateLimitCommand(2, "users.get"))
	sendRateLimitCommand(client, rpcRateLimitCommand(3, "users.get"))
	transport.waitReply(t, 2)
	require.Equal(t, uint32(417), transport.waitReply(t, 3).Error.Code)

	h.cfgContainer.ConfigRef().RPC.Registry.Errors.RateLimited.Code = 418
	sendRateLimitCommand(client, rpcRateLimitCommand(4, "users.get"))
	require.Equal(t, uint32(418), transport.waitReply(t, 4).Error.Code)
}

func TestRPCRefusalErrorIsFresh(t *testing.T) {
	h := newDedupHandler(t, rateLimitedRegistry(), newDedupBackend(t))
	first := h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", time.Second).(*centrifuge.Error)
	second := h.rateLimitRefusal(ratelimit.OperationRPC, "users.get", time.Second).(*centrifuge.Error)
	require.NotSame(t, first, second)

	first.Metadata["kept-key"] = "changed"
	require.NotContains(t, second.Metadata, "kept-key")
}

func TestRPCRefusalRetryAfterRounding(t *testing.T) {
	require.Equal(t, int64(1), retryAfterSeconds(1))
	require.Equal(t, int64(1), retryAfterSeconds(time.Second))
	require.Equal(t, int64(2), retryAfterSeconds(time.Second+1))
	require.Equal(t, int64(2), retryAfterSeconds(1200*time.Millisecond))
	require.Equal(t, int64(60), retryAfterSeconds(59999*time.Millisecond))
	require.Equal(t, int64(1), retryAfterSeconds(0))
	require.Equal(t, int64(1), retryAfterSeconds(-time.Second))
}

// A backend trailer named with the reserved prefix is dropped, the other trailers are kept.
func TestRegistryTrailerReservedPrefixDropped(t *testing.T) {
	m := &rpcregistry.Method{ErrorMetadata: []string{"x-centrifugo-retry-after", "x-centrifugo-retry", "kept-key"}}
	trailers := metadata.MD{
		"x-centrifugo-retry-after": {"5"},
		"x-centrifugo-retry":       {"later"},
		"kept-key":                 {"v"},
	}
	err := registryCallError(status.Error(codes.Unknown, "boom"), trailers, m, configtypes.RPCRegistry{}, false)

	var protoErr *centrifuge.Error
	require.True(t, errors.As(err, &protoErr))
	require.Equal(t, map[string]string{"kept-key": "v"}, protoErr.Metadata)
}

// A stored error replays without the keys with the reserved prefix, and the stored record is not changed.
func TestRegistryReplayStripsReservedPrefix(t *testing.T) {
	stored := &centrifuge.Error{
		Code:     100,
		Message:  "boom",
		Metadata: map[string]string{"x-centrifugo-retry-after": "2", "kept-key": "v"},
	}
	replayed := withoutRetrySignal(stored)
	require.Equal(t, map[string]string{"kept-key": "v"}, replayed.Metadata)
	require.Len(t, stored.Metadata, 2)
}
