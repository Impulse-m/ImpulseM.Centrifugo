package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

const testInstance string = "install_0123456789abcdef"

// rpcDedupTestConfig returns a config with the registry and the dedup on.
func rpcDedupTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Client.ReplyIncludeServerTime = true
	cfg.RPC.Registry = registryTestRegistryConfig()
	cfg.RPC.Registry.Dedup = configtypes.RPCDedup{
		Enabled:            true,
		Window:             configtypes.Duration(120 * time.Second),
		MaxSkew:            configtypes.Duration(30 * time.Second),
		MaxReplySize:       65536,
		LocalMaxEntries:    1000,
		LocalMaxBytes:      1 << 28,
		ScopeReserve:       4,
		ScopeReserveBytes:  8192,
		MaxScopesPerUser:   8,
		MaxRecordsPerScope: 1024,
		OnStoreError:       configtypes.RPCDedupFailClosed,
	}
	return cfg
}

// newRPCDedupTestClient returns a client of userID whose token session is session, and whose identity is
// zero when session is empty.
func newRPCDedupTestClient(userID string, session [16]byte, storage map[string]any) *rebindClientMock {
	client := newRegistryTestClient("c1", userID, nil, storage)
	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{Session: session}
	}
	return client
}

// testScopeOf returns the dedup scope of client with storage.
func testScopeOf(client *rebindClientMock, storage map[string]any) string {
	return rpcDedupScope(client.UserID(), client.TokenIdentity(), storage)
}

func TestRPCDedupScopePrefersSession(t *testing.T) {
	session := [16]byte{1, 2, 3}
	storage := map[string]any{rpcInstanceKey: testInstance}

	scope := testScopeOf(newRPCDedupTestClient("42", session, storage), storage)
	require.Equal(t, rpcdedup.ScopeOf("s", "42", session[:]), scope)
	require.NotEqual(t, rpcdedup.ScopeOf("i", "42", []byte(testInstance)), scope)
}

func TestRPCDedupScopeFallsBackToInstance(t *testing.T) {
	storage := map[string]any{rpcInstanceKey: testInstance}

	scope := testScopeOf(newRPCDedupTestClient("42", [16]byte{}, storage), storage)
	require.Equal(t, rpcdedup.ScopeOf("i", "42", []byte(testInstance)), scope)
}

// The same instance on another account is another scope.
func TestRPCDedupScopeBoundToUser(t *testing.T) {
	storage := map[string]any{rpcInstanceKey: testInstance}

	first := testScopeOf(newRPCDedupTestClient("42", [16]byte{}, storage), storage)
	second := testScopeOf(newRPCDedupTestClient("43", [16]byte{}, storage), storage)
	anonymous := testScopeOf(newRPCDedupTestClient("", [16]byte{}, storage), storage)
	require.NotEqual(t, first, second)
	require.NotEqual(t, first, anonymous)
}

func TestRPCDedupScopeNoneWithoutSessionOrInstance(t *testing.T) {
	storage := map[string]any{}

	scope := testScopeOf(newRPCDedupTestClient("42", [16]byte{}, storage), storage)
	require.Empty(t, scope)
}

func TestRPCDedupConnectStoresValidInstance(t *testing.T) {
	registry := startRegistryTestRegistry(t, rpcDedupTestConfig().RPC.Registry, newRegistryTestBackend(t))
	h, _ := newRegistryTestHandler(t, rpcDedupTestConfig(), registry)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenHS("42", 0),
		Instance: testInstance,
	}, nil, false)
	require.NoError(t, err)
	require.Equal(t, testInstance, reply.Storage[rpcInstanceKey])
}

func TestRPCDedupConnectDropsInvalidInstance(t *testing.T) {
	registry := startRegistryTestRegistry(t, rpcDedupTestConfig().RPC.Registry, newRegistryTestBackend(t))
	h, _ := newRegistryTestHandler(t, rpcDedupTestConfig(), registry)

	instances := []string{
		"short",
		"has space in the middle!!",
		"0123456789abcdef01234",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefX",
	}
	for _, instance := range instances {
		reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
			ClientID: "c1",
			Token:    getConnTokenHS("42", 0),
			Instance: instance,
		}, nil, false)
		require.NoError(t, err)
		_, ok := reply.Storage[rpcInstanceKey]
		require.False(t, ok, instance)
	}
}

func TestRPCDedupOffStoresNoInstance(t *testing.T) {
	cfg := rpcDedupTestConfig()
	cfg.RPC.Registry.Dedup.Enabled = false
	registry := startRegistryTestRegistry(t, cfg.RPC.Registry, newRegistryTestBackend(t))
	h, _ := newRegistryTestHandler(t, cfg, registry)

	reply, err := h.OnClientConnecting(context.Background(), centrifuge.ConnectEvent{
		ClientID: "c1",
		Token:    getConnTokenHS("42", 0),
		Instance: testInstance,
	}, nil, false)
	require.NoError(t, err)
	_, ok := reply.Storage[rpcInstanceKey]
	require.False(t, ok)
	require.Nil(t, h.rpcDedup)
}

// The scope of a call follows the session of the identity the connection holds when the call starts, so the calls after
// a refresh are scoped to the new session.
func TestRPCDedupScopeFollowsRefreshedSession(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newRegistryTestClient("c1", "42", nil, nil)
	first := [16]byte{1}
	second := [16]byte{2}
	storage, release := client.AcquireStorage()
	h.storeRPCConnection(storage, testInstance)
	release(storage)
	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{Session: first}
	}
	require.Equal(t, rpcdedup.ScopeOf("s", "42", first[:]), trackedScope(t, h, client))

	client.IdentityFunc = func() centrifuge.TokenIdentity {
		return centrifuge.TokenIdentity{Session: second}
	}
	require.Equal(t, rpcdedup.ScopeOf("s", "42", second[:]), trackedScope(t, h, client))
}
