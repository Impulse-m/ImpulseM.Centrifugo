package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
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

// newRPCDedupTestClient returns a client of userID whose token session is session, and whose identity is
// zero when session is empty.
func newRPCDedupTestClient(userID string, session [16]byte, storage map[string]any) *tools.TestClientMock {
	var client *tools.TestClientMock = newRegistryTestClient("c1", userID, nil, storage)
	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{Session: session}
	}
	return client
}

// testScopeOf returns the dedup scope of client with storage.
func testScopeOf(client *tools.TestClientMock, storage map[string]any) string {
	return rpcDedupScope(client.UserID(), client.TokenIdentity(), storage)
}

func TestRPCDedupScopePrefersSession(t *testing.T) {
	var session [16]byte = [16]byte{1, 2, 3}
	var storage map[string]any = map[string]any{clientstorage.KeyInstance: testInstance}

	var scope string = testScopeOf(newRPCDedupTestClient("42", session, storage), storage)
	require.Equal(t, rpcdedup.ScopeOf("s", "42", session[:]), scope)
	require.NotEqual(t, rpcdedup.ScopeOf("i", "42", []byte(testInstance)), scope)
}

func TestRPCDedupScopeFallsBackToInstance(t *testing.T) {
	var storage map[string]any = map[string]any{clientstorage.KeyInstance: testInstance}

	var scope string = testScopeOf(newRPCDedupTestClient("42", [16]byte{}, storage), storage)
	require.Equal(t, rpcdedup.ScopeOf("i", "42", []byte(testInstance)), scope)
}

// The same instance on another account is another scope.
func TestRPCDedupScopeBoundToUser(t *testing.T) {
	var storage map[string]any = map[string]any{clientstorage.KeyInstance: testInstance}

	var first string = testScopeOf(newRPCDedupTestClient("42", [16]byte{}, storage), storage)
	var second string = testScopeOf(newRPCDedupTestClient("43", [16]byte{}, storage), storage)
	var anonymous string = testScopeOf(newRPCDedupTestClient("", [16]byte{}, storage), storage)
	require.NotEqual(t, first, second)
	require.NotEqual(t, first, anonymous)
}

func TestRPCDedupScopeNoneWithoutSessionOrInstance(t *testing.T) {
	var storage map[string]any = map[string]any{}

	var scope string = testScopeOf(newRPCDedupTestClient("42", [16]byte{}, storage), storage)
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
