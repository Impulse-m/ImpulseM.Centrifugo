package redisshard

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeConnect is a ConnectFunc which fails its first failFor calls, then succeeds with nil shards.
type fakeConnect struct {
	calls   atomic.Int32
	failFor int32
}

func (f *fakeConnect) connect(configs []RedisShardConfig) ([]*RedisShard, error) {
	if n := f.calls.Add(1); n <= f.failFor {
		return nil, errors.New("connection refused")
	}
	return make([]*RedisShard, len(configs)), nil
}

func fakeConfigs(n int) []RedisShardConfig {
	configs := make([]RedisShardConfig, n)
	return configs
}

func TestConnectorSharesNothingBeforeConnect(t *testing.T) {
	c := NewConnector(fakeConfigs(2), nil)
	require.Nil(t, c.Shards())
}

func TestConnectorConnectIsNoOpWhenConnected(t *testing.T) {
	fake := &fakeConnect{}
	c := NewConnector(fakeConfigs(2), nil)
	c.SetConnect(fake.connect)
	require.NoError(t, c.Connect())
	require.Len(t, c.Shards(), 2)
	require.NoError(t, c.Connect())
	require.Equal(t, int32(1), fake.calls.Load())
}

func TestConnectorConnectFailureKeepsNothing(t *testing.T) {
	fake := &fakeConnect{failFor: 1}
	c := NewConnector(fakeConfigs(2), nil)
	c.SetConnect(fake.connect)
	require.Error(t, c.Connect())
	require.Nil(t, c.Shards())
	require.NoError(t, c.Connect())
	require.Len(t, c.Shards(), 2)
}

func TestConnectorLogsEveryAttempt(t *testing.T) {
	var attempts []error
	var shardCounts []int
	logFunc := func(err error, shards int) {
		attempts = append(attempts, err)
		shardCounts = append(shardCounts, shards)
	}
	fake := &fakeConnect{failFor: 1}
	c := NewConnector(fakeConfigs(3), logFunc)
	c.SetConnect(fake.connect)
	require.Error(t, c.Connect())
	require.NoError(t, c.Connect())
	require.Len(t, attempts, 2)
	require.Error(t, attempts[0])
	require.NoError(t, attempts[1])
	require.Equal(t, []int{0, 3}, shardCounts)
}

func TestConnectorRunConnectsAndClosesShardsWhenDone(t *testing.T) {
	fake := &fakeConnect{failFor: 2}
	c := NewConnector(fakeConfigs(1), nil)
	c.SetConnect(fake.connect)
	c.SetBackoff(time.Millisecond, 2*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, func() bool { return c.Shards() != nil }, 5*time.Second, time.Millisecond)
	require.Equal(t, int32(3), fake.calls.Load())

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context is done")
	}
	require.Nil(t, c.Shards())
}

func TestConnectorRunStopsRetryingWhenContextIsDone(t *testing.T) {
	fake := &fakeConnect{failFor: 1000}
	c := NewConnector(fakeConfigs(1), nil)
	c.SetConnect(fake.connect)
	c.SetBackoff(time.Millisecond, 2*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, func() bool { return fake.calls.Load() >= 3 }, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context is done")
	}
	calls := fake.calls.Load()
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, calls, fake.calls.Load())
}

func TestRetryDelayWithinJitterBounds(t *testing.T) {
	for i := 0; i < 1000; i++ {
		delay := retryDelay(time.Second)
		require.GreaterOrEqual(t, delay, 500*time.Millisecond)
		require.LessOrEqual(t, delay, time.Second)
	}
}
