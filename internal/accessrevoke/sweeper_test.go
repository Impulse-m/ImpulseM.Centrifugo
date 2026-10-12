package accessrevoke

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSweeperRemovesExpiredEntries(t *testing.T) {
	var clock atomic.Int64
	clock.Store(testNow)
	store := NewStore(time.Minute)
	store.nowUnix = clock.Load
	require.NoError(t, store.BlockUser("user-1", testNow+100))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewSweeper(store, 5*time.Millisecond).Run(ctx)
	}()

	clock.Store(testNow + 200)
	require.Eventually(t, func() bool {
		return store.Sizes().Blocked == 0
	}, 5*time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}
