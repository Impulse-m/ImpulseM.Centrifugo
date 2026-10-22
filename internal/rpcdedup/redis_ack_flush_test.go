package rpcdedup

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

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

// Redis.Ack flushes its shards in parallel: two stalled shards, each bounded by the IO timeout, cost the flush one timeout
// and not two. The shards are nil stand-ins, since the flush of each shard is replaced by the test.
func TestRedisAckFlushParallelShardsOfAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = time.Second
		connector := redisshard.NewConnector(nil, nil)
		connector.SetConnect(func(_ []redisshard.RedisShardConfig) ([]*redisshard.RedisShard, error) {
			return make([]*redisshard.RedisShard, 2), nil
		})
		require.NoError(t, connector.Connect())
		backend := NewRedis("p", connector, timeout, nil)
		backend.ackFlush = func(callCtx context.Context, _ *redisshard.RedisShard, _ []rueidis.LuaExec) error {
			<-callCtx.Done()
			return callCtx.Err()
		}
		acks := []ScopeAcks{
			{Scope: scopeOnShard(t, 0, 2), MsgIDs: []uint64{1}},
			{Scope: scopeOnShard(t, 1, 2), MsgIDs: []uint64{2}},
		}

		start := time.Now()
		err := backend.Ack(context.Background(), acks)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, timeout, time.Since(start), "the stalled shards are flushed at once")
	})
}

// scopeOnShard returns a scope whose keys are on shard index of shards shards.
func scopeOnShard(t *testing.T, index int, shards int) string {
	t.Helper()
	for i := range 1000 {
		scope := fmt.Sprintf("scope-%d", i)
		if redisshard.ShardIndex(scope, shards) == index {
			return scope
		}
	}
	t.Fatalf("no scope maps to shard %d of %d", index, shards)
	return ""
}
