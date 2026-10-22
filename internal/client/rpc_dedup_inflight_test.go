package client

import (
	"testing"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// Two idempotent calls with one msg_id on one connection both run. A cancel of the msg_id reaches the first call, which
// was registered first: the second call is not registered in its place, so it does not take the cancel entry of the first.
func TestRPCDedupIdempotentSameMsgIDCancelReachesFirst(t *testing.T) {
	backend := newDedupBackend(t)
	h := newDedupHandler(t, dedupRegistryConfig(), backend)
	client := newDedupClient("c1", "42", nil, testInstance)
	msgID := dedupMsgID(10)

	first := dedupCallAsync(h, client, "users.read", msgID, "gate")
	backend.waitEntered(t)
	second := dedupCallAsync(h, client, "users.read", msgID, "gate")
	backend.waitEntered(t)

	reply, err := h.OnRPC(client, centrifuge.RPCEvent{Call: centrifuge.RPCCallOptions{Cancel: msgID}}, nil)
	require.NoError(t, err)
	require.Empty(t, reply.Data)

	requireCancelledError(t, waitDedupResult(t, first).err)
	backend.releaseGate()
	require.NoError(t, waitDedupResult(t, second).err)
	require.Len(t, backend.Calls(), 2)
}
