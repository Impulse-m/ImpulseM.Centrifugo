package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// rebindScopeMsgID is the msg_id of the tracked calls of the scope tests. Any non-zero msg_id tracks a call.
const rebindScopeMsgID uint64 = 1

// rebindScopeConfig returns a config with rebind on and the exactly-once RPC dedup on, see rpcDedupTestConfig. The
// anonymous pool is on, so an anonymous connection is scoped to its install instance.
func rebindScopeConfig() config.Config {
	cfg := rpcDedupTestConfig()
	cfg.Client.Rebind.Enabled = true
	cfg.RPC.Registry.Dedup.LocalAnonymousMaxEntries = 256
	cfg.RPC.Registry.Dedup.LocalAnonymousMaxBytes = 64 * 8192
	return cfg
}

// trackedScope returns the dedup scope which a tracked call of client gets, from the identity the client holds now.
func trackedScope(t *testing.T, h *Handler, client rebindableClient) string {
	t.Helper()
	track := h.rpcTrackCall(client, registryConnection(client, ""), centrifuge.RPCCallOptions{MsgID: rebindScopeMsgID})
	require.NotNil(t, track)
	return track.scope
}

func TestRebindUpgradeScopesTrackedCallToUser(t *testing.T) {
	h, _, node := newAuthClassTestHandler(t, rebindScopeConfig())
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "", "anonymous", revokeTestUUID, nil))
	require.Empty(t, client.UserID())

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", client.UserID())

	_, identity := connStateOf(client).AccessIdentity()
	require.NotEqual(t, [16]byte{}, identity.Session)
	require.Equal(t, rpcdedup.ScopeOf("s", "42", identity.Session[:]), trackedScope(t, h, client))
}

func TestRebindDowngradeScopesTrackedCallToInstance(t *testing.T) {
	cfg := rebindScopeConfig()
	cfg.Client.Rebind.AllowDowngrade = true
	h, _, node := newAuthClassTestHandler(t, cfg)
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "42", "authenticated", revokeTestUUID, nil))
	require.Equal(t, "42", client.UserID())

	storage, release := client.AcquireStorage()
	storage[rpcInstanceKey] = testInstance
	release(storage)

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, client.UserID())
	require.Equal(t, rpcdedup.ScopeOf("a", "", []byte(testInstance)), trackedScope(t, h, client))
}

// TestPendingConnectScopesTrackedCallToTokenUser checks that a pending connection, which Core sees as anonymous, is
// scoped to the user of its token, as its record holds it.
func TestPendingConnectScopesTrackedCallToTokenUser(t *testing.T) {
	h, _, node := newAuthClassTestHandler(t, rebindScopeConfig())
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "42", "pending_2fa", revokeTestUUID, nil))
	require.Empty(t, client.UserID())

	_, identity := connStateOf(client).AccessIdentity()
	require.NotEqual(t, [16]byte{}, identity.Session)
	require.Equal(t, rpcdedup.ScopeOf("s", "42", identity.Session[:]), trackedScope(t, h, client))
}

// TestRPCCallRunsWithTheIdentityOfItsScope changes the identity of a connection between the dedup state of a call and its
// registry call. The call runs with the token of the identity it read first, and its answer is recorded under the scope
// of that identity, so a retransmit under that identity is answered from the record.
func TestRPCCallRunsWithTheIdentityOfItsScope(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	first := &connIdentity{
		origin:    originToken,
		class:     registryTestClass("authenticated"),
		token:     "token-first",
		tokenUser: "42",
		account:   "42",
		identity:  centrifuge.TokenIdentity{Session: [16]byte{1}},
	}
	second := &connIdentity{
		origin:    originToken,
		class:     registryTestClass("authenticated"),
		token:     "token-second",
		tokenUser: "43",
		account:   "43",
		identity:  centrifuge.TokenIdentity{Session: [16]byte{2}},
	}
	rec := &connState{}
	rec.identity.Store(first)
	client := newRegistryTestClient("c1", "42", nil, map[string]any{storageKeyConnState: rec})
	hook := func(rebindableClient) {
		rec.identity.Store(second)
	}
	testHookRPCTracked.Store(&hook)
	t.Cleanup(func() { testHookRPCTracked.Store(nil) })

	msgID := dedupMsgID(1)
	_, err := dedupCall(h, client, "users.put", msgID, "put")
	require.NoError(t, err)
	calls := backend.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, []string{"Bearer token-first"}, calls[0].Metadata.Get("authorization"))

	testHookRPCTracked.Store(nil)
	rec.identity.Store(first)
	_, err = dedupCall(h, client, "users.put", msgID, "put")
	require.NoError(t, err)
	require.Len(t, backend.Calls(), 1, "the retransmit under the first identity is answered from the record of the call")
}

func TestRefreshSameIdentityScopesTrackedCallToSession(t *testing.T) {
	h, _, node := newAuthClassTestHandler(t, rebindScopeConfig())
	client := connectRevokeTestClient(t, node, rebindTestToken(t, "42", "authenticated", revokeTestUUID, nil))
	require.Equal(t, "42", client.UserID())

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "42", "authenticated", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "42", client.UserID())

	_, identity := connStateOf(client).AccessIdentity()
	require.NotEqual(t, [16]byte{}, identity.Session)
	require.Equal(t, rpcdedup.ScopeOf("s", "42", identity.Session[:]), trackedScope(t, h, client))
}
