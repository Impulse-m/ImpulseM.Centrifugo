//go:build integration

package rpcdedup

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// integrationBackendObserver counts the errors of a Redis backend of the integration tests, and the wake watchers which
// it runs.
type integrationBackendObserver struct {
	errors   atomic.Int64
	watchers atomic.Int64
}

// RedisCall implements BackendObserver.
func (o *integrationBackendObserver) RedisCall(float64) {}

// RedisError implements BackendObserver.
func (o *integrationBackendObserver) RedisError() {
	o.errors.Add(1)
}

// WakeWatching implements BackendObserver.
func (o *integrationBackendObserver) WakeWatching(delta int) {
	o.watchers.Add(int64(delta))
}

// integrationStreamLen returns the number of entries of the stream key, zero for a stream which does not exist.
func integrationStreamLen(t *testing.T, connector *redisshard.Connector, key string) int64 {
	t.Helper()
	n, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Xlen().Key(key).Build()
	}).AsInt64()
	require.NoError(t, err)
	return n
}

// integrationStreamEntries returns the entries of the stream key, in order.
func integrationStreamEntries(t *testing.T, connector *redisshard.Connector, key string) []rueidis.XRangeEntry {
	t.Helper()
	entries, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Xrange().Key(key).Start("-").End("+").Build()
	}).AsXRange()
	require.NoError(t, err)
	return entries
}

// integrationBlockedXReads returns the clients which block on an XREAD, as CLIENT LIST shows them.
func integrationBlockedXReads(t *testing.T, connector *redisshard.Connector) int {
	t.Helper()
	list, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().ClientList().Build()
	}).ToString()
	require.NoError(t, err)
	blocked := 0
	for _, line := range strings.Split(list, "\n") {
		var isXRead, isBlocked bool
		for _, field := range strings.Fields(line) {
			if field == "cmd=xread" {
				isXRead = true
			}
			if strings.HasPrefix(field, "flags=") && strings.Contains(field, "b") {
				isBlocked = true
			}
		}
		if isXRead && isBlocked {
			blocked++
		}
	}
	return blocked
}

// startIntegrationWatcher runs WatchWakes of nodeID on backend until the returned channel holds its result. The keys
// it wakes arrive on woken. The watcher stops when ctx is done or idle reports true.
func startIntegrationWatcher(ctx context.Context, backend *Redis, nodeID string, idle func() bool, woken chan<- Key) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- backend.WatchWakes(ctx, nodeID, func(key Key) {
			woken <- key
		}, idle)
	}()
	return done
}

func TestRedisWakeWritesStreamPerNode(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	key := Key{Scope: testScope("u1", "a"), MsgID: 40}

	written, err := backend.Wake(context.Background(), key, []string{"node-b", "node-c"})
	require.NoError(t, err)
	require.Equal(t, 2, written)
	for _, nodeID := range []string{"node-b", "node-c"} {
		stream := WakeStreamKey(prefix, nodeID)
		entries := integrationStreamEntries(t, connector, stream)
		require.Len(t, entries, 1)
		require.Equal(t, key.Scope, entries[0].FieldValues[redisWakeFieldScope])
		require.Equal(t, "40", entries[0].FieldValues[redisWakeFieldMsgID])

		ttl, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
			return c.B().Pttl().Key(stream).Build()
		}).AsInt64()
		require.NoError(t, err)
		require.Greater(t, ttl, int64(0))
		require.LessOrEqual(t, ttl, wakeStreamTTL.Milliseconds())
	}
}

func TestRedisWakeTrimsAtMaxLen(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	stream := WakeStreamKey(prefix, "node-b")
	for i := range 4200 {
		_, err := backend.Wake(context.Background(), Key{Scope: testScope("u1", "a"), MsgID: uint64(i)}, []string{"node-b"})
		require.NoError(t, err)
	}
	length := integrationStreamLen(t, connector, stream)
	require.Greater(t, length, int64(0))
	require.Less(t, length, int64(4500), "the stream is trimmed to about %d entries", wakeStreamMaxLen)
}

func TestRedisWatchWakesDelivers(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	backend.readBlock = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	woken := make(chan Key, 8)
	startIntegrationWatcher(ctx, backend, "node-b", func() bool { return false }, woken)

	key := Key{Scope: testScope("u1", "a"), MsgID: 41}
	_, err := backend.Wake(context.Background(), key, []string{"node-b"})
	require.NoError(t, err)
	select {
	case got := <-woken:
		require.Equal(t, key, got)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("the wake is not delivered within 100 ms")
	}
	require.Eventually(t, func() bool {
		return integrationStreamLen(t, connector, WakeStreamKey(prefix, "node-b")) == 0
	}, time.Second, 10*time.Millisecond, "the processed entry is deleted")
}

func TestRedisWatchWakesReadsFromStartOnRestart(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	backend.readBlock = 100 * time.Millisecond
	key := Key{Scope: testScope("u1", "a"), MsgID: 42}

	// The wake is written while no watcher runs: the watcher which starts later reads it from the start of the stream.
	_, err := backend.Wake(context.Background(), key, []string{"node-b"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	woken := make(chan Key, 8)
	startIntegrationWatcher(ctx, backend, "node-b", func() bool { return false }, woken)
	select {
	case got := <-woken:
		require.Equal(t, key, got)
	case <-time.After(time.Second):
		t.Fatal("the wake written before the watcher started is not delivered")
	}
}

func TestRedisWatchWakesStopsOnCancel(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	ctx, cancel := context.WithCancel(context.Background())
	woken := make(chan Key, 8)
	done := startIntegrationWatcher(ctx, backend, "node-b", func() bool { return false }, woken)

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("the watcher does not stop within 1 s of its cancel")
	}
}

func TestRedisWatchWakesStopsWhenIdle(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	backend.readBlock = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	woken := make(chan Key, 8)
	done := startIntegrationWatcher(ctx, backend, "node-b", func() bool { return true }, woken)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("the watcher does not stop within 500 ms of an empty read which reports it idle")
	}
	require.Eventually(t, func() bool {
		return integrationBlockedXReads(t, connector) == 0
	}, time.Second, 10*time.Millisecond, "no blocked XREAD is left after the idle stop")
}

func TestRedisWatchWakesSkipsMalformedEntry(t *testing.T) {
	prefix := uniqueRedisPrefix()
	observer := &integrationBackendObserver{}
	connector := newIntegrationConnector(t, prefix)
	backend := NewRedis(prefix, connector, 0, observer)
	backend.readBlock = 100 * time.Millisecond
	stream := WakeStreamKey(prefix, "node-b")
	_, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("x", "1").Build()
	}).ToString()
	require.NoError(t, err)

	key := Key{Scope: testScope("u1", "a"), MsgID: 43}
	_, err = backend.Wake(context.Background(), key, []string{"node-b"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	woken := make(chan Key, 8)
	startIntegrationWatcher(ctx, backend, "node-b", func() bool { return false }, woken)
	select {
	case got := <-woken:
		require.Equal(t, key, got, "the malformed entry wakes nothing, and the proper one is delivered")
	case <-time.After(time.Second):
		t.Fatal("the wake after a malformed entry is not delivered")
	}
	require.Equal(t, int64(1), observer.errors.Load(), "the malformed entry is counted once")
	require.Eventually(t, func() bool {
		return integrationStreamLen(t, connector, stream) == 0
	}, time.Second, 10*time.Millisecond, fmt.Sprintf("the entries of %s are deleted", stream))
}
