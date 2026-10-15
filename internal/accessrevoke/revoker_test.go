package accessrevoke

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// newRevokeTestNode creates a node which takes identity from the connect token. Token has form
// "user|session|generation|iat".
func newRevokeTestNode(t testing.TB) *centrifuge.Node {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		parts := strings.Split(e.Token, "|")
		generation, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return centrifuge.ConnectReply{}, err
		}
		issuedAt, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return centrifuge.ConnectReply{}, err
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID:        parts[0],
				TokenIdentity: Identity(parts[1], generation, issuedAt),
			},
		}, nil
	})
	return node
}

func connectTestClient(t testing.TB, node *centrifuge.Node, token string) {
	t.Helper()
	transport := tools.NewTestTransport()
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{Token: token},
	}
	data, err := protocol.NewJSONCommandEncoder().Encode(command)
	require.NoError(t, err)
	require.True(t, centrifuge.HandleReadFrame(client, bytes.NewReader(data), 1<<20))
}

func newTestRevoker(t *testing.T, node *centrifuge.Node, store *Store) *Revoker {
	t.Helper()
	queue := NewDisconnectQueue(10000, 100)
	cancel, errCh := runQueue(t, queue)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})
	return NewRevoker(store, node.Hub(), queue)
}

func waitConnections(t *testing.T, node *centrifuge.Node, userID string, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(node.Hub().UserConnections(userID)) == count
	}, 5*time.Second, 5*time.Millisecond)
}

func TestRevokerBlockUserDisconnectsUserConnections(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-1|"+testOtherUUID+"|0|100")
	connectTestClient(t, node, "user-2|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 2)

	require.NoError(t, revoker.BlockUser("user-1", 0))

	waitConnections(t, node, "user-1", 0)
	waitConnections(t, node, "user-2", 1)
}

func TestRevokerUnblockUserDoesNotDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)

	require.NoError(t, revoker.UnblockUser("user-1"))
	require.Never(t, func() bool {
		return len(node.Hub().UserConnections("user-1")) == 0
	}, 200*time.Millisecond, 20*time.Millisecond)
}

func TestRevokerRevokeSessionDisconnectsMatchingSession(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-1|"+testOtherUUID+"|0|100")
	connectTestClient(t, node, "user-2|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 2)

	require.NoError(t, revoker.RevokeSession("user-1", testUUID, 0, 0))

	waitConnections(t, node, "user-1", 1)
	for _, client := range node.Hub().UserConnections("user-1") {
		otherKey, _ := Key(testOtherUUID)
		require.Equal(t, otherKey, client.TokenIdentity().Session)
	}
	waitConnections(t, node, "user-2", 1)
}

func TestRevokerInvalidateUserDisconnectsOlderTokens(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	// Generation 4 is below the minimum generation 5.
	connectTestClient(t, node, "user-3|"+testUUID+"|4|100")
	// Generation 5 issued before the cut-off 200.
	connectTestClient(t, node, "user-3|"+testOtherUUID+"|5|150")
	// Generation 5 issued after the cut-off stays.
	connectTestClient(t, node, "user-3|"+testOtherUUID+"|5|300")
	waitConnections(t, node, "user-3", 3)

	require.NoError(t, revoker.InvalidateUser("user-3", 5, 200, 0, 0))

	waitConnections(t, node, "user-3", 1)
	for _, client := range node.Hub().UserConnections("user-3") {
		require.Equal(t, int64(300), client.TokenIdentity().IssuedAt)
	}
}

func TestRevokerRevokeSessionWithoutUserSweepsConnections(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-2|"+testOtherUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	waitConnections(t, node, "user-2", 1)

	require.NoError(t, revoker.RevokeSession("", testUUID, 0, 0))

	waitConnections(t, node, "user-1", 0)
	waitConnections(t, node, "user-2", 1)
}

func TestRevokerRevokeSessionWithoutUserCoalescesSweeps(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	var sweeps atomic.Int64
	swept := make(chan struct{})
	gate := make(chan struct{})
	original := revoker.sweep
	revoker.sweep = func() {
		original()
		if sweeps.Add(1) == 1 {
			close(swept)
			<-gate
		}
	}

	connectTestClient(t, node, "user-1|"+testOtherUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)

	// The first sweep runs and then blocks, so the revocations below happen while a sweep is running.
	require.NoError(t, revoker.RevokeSession("", "dummy-first", 0, 0))
	select {
	case <-swept:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "sweep did not start")
	}

	for i := 0; i < 50; i++ {
		require.NoError(t, revoker.RevokeSession("", fmt.Sprintf("dummy-%d", i), 0, 0))
	}
	// The last revocation targets the connected session and happens after the sweep started.
	require.NoError(t, revoker.RevokeSession("", testOtherUUID, 0, 0))
	close(gate)

	waitConnections(t, node, "user-1", 0)
	require.LessOrEqual(t, sweeps.Load(), int64(2))
}

func TestRevokerEmptyUserIsError(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	require.ErrorIs(t, revoker.BlockUser("", 0), ErrEmptyID)
	require.ErrorIs(t, revoker.InvalidateUser("", 1, 0, 0, 0), ErrEmptyID)
}
