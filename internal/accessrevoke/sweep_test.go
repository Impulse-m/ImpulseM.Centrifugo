package accessrevoke

import (
	"context"
	"testing"
	"testing/synctest"
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
	require.NoError(t, store.RevokeSession(testOtherUUID, 0, 0, nil))

	rejected, _ := sweepConnections(node.Hub(), store, queue, allAccessChecks)
	require.Equal(t, 2, rejected)

	waitConnections(t, node, "user-1", 0)
	waitConnections(t, node, "user-3", 0)
	waitConnections(t, node, "user-2", 1)
}

// TestSweepRetriesDroppedDisconnects sweeps three rejected connections of one user while the queue holds one. The
// dropped disconnects are retried by user once the queue runs, so all three are disconnected.
func TestSweepRetriesDroppedDisconnects(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 1)
	NewRevokerWithMaxTTL(store, node.Hub(), queue, nil)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 3)
	require.NoError(t, store.RevokeSession(testUUID, 0, 0, nil))

	rejected, _ := sweepConnections(node.Hub(), store, queue, allAccessChecks)
	require.Equal(t, 3, rejected)

	cancel, errCh := runQueue(t, queue)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})
	waitConnections(t, node, "user-1", 0)
}

func TestSweepConnectionsDisabledChecksRejectNothing(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	require.NoError(t, store.BlockUser("user-1", 0))

	rejected, _ := sweepConnections(node.Hub(), store, queue, CheckOptions{})
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

func TestSweeperRemovesExpiredEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewStore(20 * time.Minute)
		t0 := time.Now().Unix()
		require.NoError(t, store.BlockUser("user-1", t0+100))

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- NewSweeper(store, 5*time.Millisecond).Run(ctx)
		}()
		synctest.Wait()

		time.Sleep(99 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, store.Sizes().Blocked)

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 0, store.Sizes().Blocked)

		cancel()
		require.ErrorIs(t, <-errCh, context.Canceled)
	})
}

func TestSweeperExportsStoreSizeAtScrape(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewStore(20 * time.Minute)
		t0 := time.Now().Unix()
		require.NoError(t, store.BlockUser("user-1", t0+1000))
		require.NoError(t, store.BlockUser("user-2", t0+1000))
		require.NoError(t, store.RevokeSession(testUUID, 0, t0+1000, nil))

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- NewSweeper(store, time.Second).Run(ctx)
		}()
		synctest.Wait()

		require.Equal(t, 2.0, storeSizeValue(t, "blocked"))
		require.Equal(t, 1.0, storeSizeValue(t, "session"))
		require.Equal(t, 0.0, storeSizeValue(t, "user"))

		require.NoError(t, store.BlockUser("user-3", t0+1000))
		require.Equal(t, 3.0, storeSizeValue(t, "blocked"))

		cancel()
		require.ErrorIs(t, <-errCh, context.Canceled)
		synctest.Wait()
		require.Equal(t, 0.0, storeSizeValue(t, "blocked"))
		require.Equal(t, 0.0, storeSizeValue(t, "session"))
		require.Equal(t, 0.0, storeSizeValue(t, "user"))
	})
}

// storeSizeValue returns the access_revoke store_size gauge of the kind from the test registry.
func storeSizeValue(t *testing.T, kind string) float64 {
	t.Helper()
	return labeledCounterValue(t, "centrifugo_access_revoke_store_size", "kind", kind)
}
