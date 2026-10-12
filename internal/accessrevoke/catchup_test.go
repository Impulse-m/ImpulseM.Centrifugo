package accessrevoke

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSweepConnectionsDisconnectsRejectedOnly(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)
	cancel, errCh := runQueue(t, queue)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-2|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-3|"+testOtherUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	waitConnections(t, node, "user-2", 1)
	waitConnections(t, node, "user-3", 1)

	require.NoError(t, store.BlockUser("user-1", 0))
	require.NoError(t, store.RevokeSession(testOtherUUID, 0, 0))

	rejected := SweepConnections(node.Hub(), store, queue, allAccessChecks)
	require.Equal(t, 2, rejected)

	waitConnections(t, node, "user-1", 0)
	waitConnections(t, node, "user-3", 0)
	waitConnections(t, node, "user-2", 1)
}

func TestSweepConnectionsDisabledChecksRejectNothing(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	require.NoError(t, store.BlockUser("user-1", 0))

	rejected := SweepConnections(node.Hub(), store, queue, CheckOptions{})
	require.Equal(t, 0, rejected)
}

func TestCatchUpSweeperWaitsForCaughtUp(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)
	cancel, errCh := runQueue(t, queue)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	require.NoError(t, store.BlockUser("user-1", 0))

	caughtUp := make(chan struct{})
	sweeper := NewCatchUpSweeper(caughtUp, node.Hub(), store, queue, func() CheckOptions {
		return allAccessChecks
	})
	sweepErr := make(chan error, 1)
	go func() {
		sweepErr <- sweeper.Run(context.Background())
	}()

	// Before catch up the sweep does not run.
	require.Never(t, func() bool {
		return len(node.Hub().UserConnections("user-1")) == 0
	}, 200*time.Millisecond, 20*time.Millisecond)

	close(caughtUp)
	require.NoError(t, <-sweepErr)
	waitConnections(t, node, "user-1", 0)
}

func TestCatchUpSweeperStopsOnContextDone(t *testing.T) {
	node := newRevokeTestNode(t)
	sweeper := NewCatchUpSweeper(make(chan struct{}), node.Hub(), NewStore(time.Minute), NewDisconnectQueue(10, 10), func() CheckOptions {
		return allAccessChecks
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, sweeper.Run(ctx), context.Canceled)
}
