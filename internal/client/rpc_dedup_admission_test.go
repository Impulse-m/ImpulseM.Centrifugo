package client

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// otherTestInstance is a second valid install instance, for a second scope of one user.
const otherTestInstance = "other_install_0123456789ab"

// testSmallDedupStore replaces the store of h with a one-shard store of maxEntries records, which admits
// maxEntries/4 scopes, and of maxScopesPerUser scopes per user. The metrics of h are kept. It returns the store.
func testSmallDedupStore(h *Handler, maxEntries int, maxScopesPerUser int) *rpcdedup.Memory {
	store := rpcdedup.NewMemory(rpcdedup.MemoryOptions{
		MaxEntries:         maxEntries,
		MaxBytes:           1 << 28,
		MaxRecordsPerScope: 1024,
		ScopeReserve:       4,
		ScopeReserveBytes:  8192,
		MaxScopesPerUser:   maxScopesPerUser,
		TTL:                120 * time.Second,
		MaxReplyBytes:      65536,
		Shards:             1,
	})
	h.rpcDedup.store = store
	return store
}

// testFillSmallStore records one call for each of the users, so that their scopes are admitted.
func testFillSmallStore(t *testing.T, h *Handler, backend *dedupBackend, users ...string) {
	t.Helper()
	for _, user := range users {
		r := dedupCallAsyncResult(t, h, newDedupClient("fill-"+user, user, nil, testInstance), "users.get", dedupMsgID(1), "x")
		require.NoError(t, r.err)
	}
	require.Len(t, backend.Calls(), len(users))
}

// A call of a scope which the full pool refuses is answered with the temporary dedup_unavailable error under
// fail_closed, and is not run.
func TestRPCDedupRefusedScopeFailsClosed(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	store := testSmallDedupStore(h, 8, 8)
	testFillSmallStore(t, h, backend, "1", "2")

	refused := dedupCallAsyncResult(t, h, newDedupClient("c3", "3", nil, testInstance), "users.get", dedupMsgID(2), "x")
	protoErr := requireProtocolError(t, refused.err)
	require.Equal(t, uint32(108), protoErr.Code)
	require.True(t, protoErr.Temporary)
	require.Len(t, backend.Calls(), 2)
	require.Equal(t, 2, store.Len())
	require.Equal(t, float64(1), testDedupCount(vec, "refused_pool"))
	require.Zero(t, testDedupCount(vec, "store_error"))
}

// A call of a refused scope under fail_open runs without dedup, and nothing is recorded for it.
func TestRPCDedupRefusedScopeFailsOpen(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.OnStoreError = configtypes.RPCDedupFailOpen
	h := newDedupHandler(t, reg, backend)
	store := testSmallDedupStore(h, 8, 8)
	testFillSmallStore(t, h, backend, "1", "2")

	r := dedupCallAsyncResult(t, h, newDedupClient("c3", "3", nil, testInstance), "users.get", dedupMsgID(2), "x")
	require.NoError(t, r.err)
	require.False(t, r.reply.Cached)
	require.Len(t, backend.Calls(), 3)
	require.Equal(t, 2, store.Len())
	require.Equal(t, float64(1), testDedupCount(vec, "refused_pool"))
}

// A user at max_scopes_per_user is refused for a further scope, and counted by the user limit.
func TestRPCDedupRefusedUserIsCounted(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	store := testSmallDedupStore(h, 1000, 1)
	testFillSmallStore(t, h, backend, "4")

	refused := dedupCallAsyncResult(t, h, newDedupClient("c4b", "4", nil, otherTestInstance), "users.get", dedupMsgID(2), "x")
	protoErr := requireProtocolError(t, refused.err)
	require.Equal(t, uint32(108), protoErr.Code)
	require.Len(t, backend.Calls(), 1)
	require.Equal(t, 1, store.Len())
	require.Equal(t, float64(1), testDedupCount(vec, "refused_user"))
	require.Zero(t, testDedupCount(vec, "refused_pool"))
}

// A cancel for a scope which the full pool refuses records nothing, and is answered with the empty reply.
func TestRPCDedupRefusedCancelRecordsNothing(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	store := testSmallDedupStore(h, 8, 8)
	testFillSmallStore(t, h, backend, "1", "2")

	client := newDedupClient("c3", "3", nil, testInstance)
	msgID := dedupMsgID(3)
	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.Data)
	require.Equal(t, 2, store.Len())
	require.Equal(t, float64(1), testDedupCount(vec, "refused_pool"))
	require.Zero(t, testDedupCount(vec, "store_error"))
}

// An anonymous connection without a scope runs its calls each time: its msg_id is not deduplicated.
func TestRPCDedupAnonymousWithoutScopeRunsEachTime(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("anon", "", nil, testInstance)
	msgID := dedupMsgID(4)

	for i := 0; i < 2; i++ {
		r := dedupCallAsyncResult(t, h, client, "users.anon", msgID, "x")
		require.NoError(t, r.err)
		require.False(t, r.reply.Cached)
	}
	require.Len(t, backend.Calls(), 2)
	require.Equal(t, float64(2), testDedupCount(vec, "no_scope"))
}

// An anonymous connection is deduplicated by its instance when the store keeps anonymous scopes: a retransmit is
// answered from the record.
func TestRPCDedupAnonymousScopeRecords(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	reg := dedupRegistryConfig()
	reg.Dedup.LocalAnonymousMaxEntries = 256
	reg.Dedup.LocalAnonymousMaxBytes = 64 * 8192
	h := newDedupHandler(t, reg, backend)
	client := newDedupClient("anon", "", nil, testInstance)
	msgID := dedupMsgID(5)

	first := dedupCallAsyncResult(t, h, client, "users.anon", msgID, "x")
	require.NoError(t, first.err)
	retransmit := dedupCallAsyncResult(t, h, client, "users.anon", msgID, "x")
	require.NoError(t, retransmit.err)
	require.True(t, retransmit.reply.Cached)
	require.Len(t, backend.Calls(), 1)
	require.Zero(t, testDedupCount(vec, "no_scope"))
}

// A reload which widens the window does not widen the msg_ids the handler accepts: the window is kept with the
// store, whose records last for the window at start.
func TestRPCDedupReloadDoesNotWidenWindow(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	cfg := h.cfgContainer.Config()
	cfg.RPC.Registry.Dedup.Window = configtypes.Duration(600 * time.Second)
	require.NoError(t, h.cfgContainer.Reload(cfg))
	client := newDedupClient("c1", "42", nil, testInstance)

	past := uint64(time.Now().Add(-200*time.Second).Unix())<<rpcdedup.MsgIDSeqBits | 1
	_, err := dedupCall(h, client, "users.get", past, "x")
	protoErr := requireProtocolError(t, err)
	require.Equal(t, uint32(107), protoErr.Code)
	require.Equal(t, "invalid msg_id", protoErr.Message)
	require.Empty(t, backend.Calls())
}

// A duplicate which joins a call on its own connection does not take the in-flight entry of the call: a cancel of
// the msg_id on the connection reaches the call, and both the owner and the duplicate are answered cancelled.
func TestRPCDedupCancelReachesOwnerOfJoinedCall(t *testing.T) {
	vec := testDedupOutcomes(t)
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(6)

	owner := dedupCallAsync(h, client, "users.get", msgID, "gate")
	backend.waitEntered(t)
	joined := dedupCallAsync(h, client, "users.get", msgID, "gate")
	require.Eventually(t, func() bool {
		return testDedupCount(vec, "hit_inflight_local") == 1
	}, 5*time.Second, time.Millisecond)

	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.Data)
	requireCancelledError(t, waitDedupResult(t, owner).err)
	requireCancelledError(t, waitDedupResult(t, joined).err)
	require.Len(t, backend.Calls(), 1)
}

// The scope of a connection follows the session of its identity when the token is refreshed, so the calls after a
// refresh are scoped to the new session.
func TestRPCDedupScopeFollowsRefreshedSession(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newRegistryTestClient("c1", "42", nil, nil)
	first := [16]byte{1}
	second := [16]byte{2}
	storage, release := client.AcquireStorage()
	h.storeRPCConnection(storage, testInstance, &centrifuge.Credentials{
		UserID:        "42",
		TokenIdentity: centrifuge.TokenIdentity{Session: first},
	})
	release(storage)
	require.Equal(t, rpcdedup.ScopeOf("s", "42", first[:]), testStoredScope(client))

	h.refreshRPCScope(client, centrifuge.TokenIdentity{Session: second})
	require.Equal(t, rpcdedup.ScopeOf("s", "42", second[:]), testStoredScope(client))
}

// testStoredScope returns the dedup scope stored in the connection storage of client.
func testStoredScope(client *tools.TestClientMock) string {
	storage, release := client.AcquireStorage()
	defer release(storage)
	scope, _ := storage[rpcScopeKey].(string)
	return scope
}
