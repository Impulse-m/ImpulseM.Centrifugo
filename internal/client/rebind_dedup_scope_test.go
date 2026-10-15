package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
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

// trackedScope returns the dedup scope which a tracked call of client gets. The scope is read from the connection
// storage, where a refresh leaves it.
func trackedScope(t *testing.T, h *Handler, client *centrifuge.Client) string {
	t.Helper()
	track := h.rpcTrackCall(client, centrifuge.RPCCallOptions{MsgID: rebindScopeMsgID})
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
	storage[clientstorage.KeyInstance] = testInstance
	release(storage)

	_, _, err := h.OnRefresh(client, centrifuge.RefreshEvent{
		Token: rebindTestToken(t, "", "anonymous", revokeTestOtherUUID, nil),
	}, nil)
	require.NoError(t, err)
	require.Empty(t, client.UserID())
	require.Equal(t, rpcdedup.ScopeOf("a", "", []byte(testInstance)), trackedScope(t, h, client))
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
