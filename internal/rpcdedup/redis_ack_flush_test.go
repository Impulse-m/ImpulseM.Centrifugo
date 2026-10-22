package rpcdedup

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// The ack flush of the shards runs in parallel: two stalled shards, each taking one IO timeout, cost the flush one timeout
// and not two. The fake clock of the bubble makes the stall free and exact.
func TestRedisAckFlushParallelShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const stall = time.Second
		groups := map[int][]rueidis.LuaExec{0: {{}}, 1: {{}}}
		start := time.Now()
		err := ackShards(context.Background(), groups, func(_ context.Context, _ int, _ []rueidis.LuaExec) error {
			time.Sleep(stall)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, stall, time.Since(start), "the shards are flushed at once")
	})
}

// The flush of every shard is attempted, even when one fails, and the flush returns an error of a failed shard.
func TestRedisAckFlushAttemptsEveryShard(t *testing.T) {
	groups := map[int][]rueidis.LuaExec{0: {{}}, 1: {{}}, 2: {{}}}
	attempted := make(chan int, len(groups))
	err := ackShards(context.Background(), groups, func(_ context.Context, index int, _ []rueidis.LuaExec) error {
		attempted <- index
		if index == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, attempted, len(groups))
}
