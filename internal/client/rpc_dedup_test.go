package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const testInstance string = "install_0123456789abcdef"

// rpcDedupTestConfig returns a config with the registry and the dedup on.
func rpcDedupTestConfig() config.Config {
	var cfg config.Config = config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry = registryTestRegistryConfig()
	cfg.RPC.Registry.Dedup = configtypes.RPCDedup{
		Enabled:         true,
		Window:          configtypes.Duration(120 * time.Second),
		MaxSkew:         configtypes.Duration(30 * time.Second),
		MaxReplySize:    65536,
		LocalMaxEntries: 1000,
		OnStoreError:    configtypes.RPCDedupFailClosed,
	}
	return cfg
}

// newRPCDedupTestClient returns a client whose token session is session, and whose identity is zero when
// session is empty.
func newRPCDedupTestClient(session [16]byte, storage map[string]any) *tools.TestClientMock {
	var client *tools.TestClientMock = newRegistryTestClient("c1", "42", nil, storage)
	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{Session: session}
	}
	return client
}

func TestRPCDedupScopePrefersSession(t *testing.T) {
	var session [16]byte = [16]byte{1, 2, 3}
	var storage map[string]any = map[string]any{clientstorage.KeyInstance: testInstance}

	var scope string = rpcDedupScope(newRPCDedupTestClient(session, storage), storage)
	require.Equal(t, "s:01020300000000000000000000000000", scope)
}

func TestRPCDedupScopeFallsBackToInstance(t *testing.T) {
	var storage map[string]any = map[string]any{clientstorage.KeyInstance: testInstance}

	var scope string = rpcDedupScope(newRPCDedupTestClient([16]byte{}, storage), storage)
	require.Equal(t, "i:"+testInstance, scope)
}

func TestRPCDedupScopeNoneWithoutSessionOrInstance(t *testing.T) {
	var storage map[string]any = map[string]any{}

	var scope string = rpcDedupScope(newRPCDedupTestClient([16]byte{}, storage), storage)
	require.Empty(t, scope)
}

func TestRPCDedupConnectStoresValidInstance(t *testing.T) {
	var registry *rpcregistry.Registry = startRegistryTestRegistry(t, rpcDedupTestConfig().RPC.Registry, newRegistryTestBackend(t))
	var h *Handler
	h, _ = newRegistryTestHandler(t, rpcDedupTestConfig(), registry)

	var reply centrifuge.ConnectReply
	var err error
	reply, err = h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenHS("42", 0),
		Instance: testInstance,
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, testInstance, reply.Storage[clientstorage.KeyInstance])
}

func TestRPCDedupConnectDropsInvalidInstance(t *testing.T) {
	var registry *rpcregistry.Registry = startRegistryTestRegistry(t, rpcDedupTestConfig().RPC.Registry, newRegistryTestBackend(t))
	var h *Handler
	h, _ = newRegistryTestHandler(t, rpcDedupTestConfig(), registry)

	for _, instance := range []string{"short", "has space in the middle!!", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefX"} {
		var reply centrifuge.ConnectReply
		var err error
		reply, err = h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
			ClientID: "c1",
			Token:    getConnTokenHS("42", 0),
			Instance: instance,
		}, nil, false)
		require.NoError(t, err)
		_, ok := reply.Storage[clientstorage.KeyInstance]
		require.False(t, ok, instance)
	}
}

func TestRPCDedupOffStoresNoInstance(t *testing.T) {
	var cfg config.Config = rpcDedupTestConfig()
	cfg.RPC.Registry.Dedup.Enabled = false
	var registry *rpcregistry.Registry = startRegistryTestRegistry(t, cfg.RPC.Registry, newRegistryTestBackend(t))
	var h *Handler
	h, _ = newRegistryTestHandler(t, cfg, registry)

	var reply centrifuge.ConnectReply
	var err error
	reply, err = h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenHS("42", 0),
		Instance: testInstance,
	}, nil, false)
	require.NoError(t, err)
	_, ok := reply.Storage[clientstorage.KeyInstance]
	require.False(t, ok)
	require.Nil(t, h.rpcDedup)
}
