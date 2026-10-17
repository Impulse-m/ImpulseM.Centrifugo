//go:build integration

package rpcdedup

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// integrationRedisAddress is the Redis of integration tests, see docker-compose.yml.
const integrationRedisAddress = "localhost:6379"

// integrationMaxWaiters is the waiter cap which the integration tests pass to begin: the default of the config.
const integrationMaxWaiters = 64

// uniqueRedisPrefix returns a key prefix which no other test run uses, so tests do not share state.
func uniqueRedisPrefix() string {
	return fmt.Sprintf("rpcdedup%d", time.Now().UnixNano())
}

// newIntegrationConnector returns a connected connector of the integration Redis. Its keys of prefix are deleted,
// and it is closed, when the test ends.
func newIntegrationConnector(t *testing.T, prefix string) *redisshard.Connector {
	t.Helper()
	configs, err := redisshard.ParseRedisShardConfigs(configtypes.Redis{Address: []string{integrationRedisAddress}})
	require.NoError(t, err)
	connector := redisshard.NewConnector(configs, nil)
	require.NoError(t, connector.Connect())
	t.Cleanup(connector.Close)
	t.Cleanup(func() {
		deleteIntegrationKeys(connector, prefix)
	})
	return connector
}

// deleteIntegrationKeys deletes the keys of prefix, found by SCAN, on the shards of connector.
func deleteIntegrationKeys(connector *redisshard.Connector, prefix string) {
	ctx := context.Background()
	for _, shard := range connector.Shards() {
		var cursor uint64
		for {
			entry, err := shard.RunOp(func(c rueidis.Client) rueidis.RedisResult {
				return c.Do(ctx, c.B().Scan().Cursor(cursor).Match(prefix+"*").Count(100).Build())
			}).AsScanEntry()
			if err != nil {
				return
			}
			if len(entry.Elements) > 0 {
				shard.RunOp(func(c rueidis.Client) rueidis.RedisResult {
					return c.Do(ctx, c.B().Del().Key(entry.Elements...).Build())
				})
			}
			cursor = entry.Cursor
			if cursor == 0 {
				break
			}
		}
	}
}

// integrationDo runs a command on the first shard of connector, and returns its result.
func integrationDo(connector *redisshard.Connector, build func(c rueidis.Client) rueidis.Completed) rueidis.RedisResult {
	return connector.Shards()[0].RunOp(func(c rueidis.Client) rueidis.RedisResult {
		return c.Do(context.Background(), build(c))
	})
}

// integrationField returns the field of a record, and false when it is missing.
func integrationField(t *testing.T, connector *redisshard.Connector, key string, field string) (string, bool) {
	t.Helper()
	value, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Hget().Key(key).Field(field).Build()
	}).ToString()
	if rueidis.IsRedisNil(err) {
		return "", false
	}
	require.NoError(t, err)
	return value, true
}

// integrationPTTL returns the remaining time of key in milliseconds.
func integrationPTTL(t *testing.T, connector *redisshard.Connector, key string) int64 {
	t.Helper()
	ms, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Pttl().Key(key).Build()
	}).AsInt64()
	require.NoError(t, err)
	return ms
}

// integrationExists reports whether key exists.
func integrationExists(t *testing.T, connector *redisshard.Connector, key string) bool {
	t.Helper()
	n, err := integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Exists().Key(key).Build()
	}).AsInt64()
	require.NoError(t, err)
	return n == 1
}

// newIntegrationBackend returns a Redis backend of a connected connector with prefix.
func newIntegrationBackend(t *testing.T, prefix string) (*Redis, *redisshard.Connector) {
	t.Helper()
	connector := newIntegrationConnector(t, prefix)
	return NewRedis(prefix, connector, 0, nil), connector
}

// integrationTTL is the inflight TTL of the tests: a method timeout of 5 seconds and the grace.
const integrationTTL = 5*time.Second + InflightGrace

func TestRedisBackendBeginCompleteLifecycle(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 1}
	fingerprint := Fingerprint("users.get", []byte("x"))
	other := Fingerprint("users.get", []byte("y"))

	begin, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginMiss, begin.State)

	begin, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginInFlight, begin.State)
	require.Equal(t, "node-a", begin.Owner)

	begin, err = backend.Begin(ctx, key, other, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginMismatch, begin.State)

	result, err := backend.Complete(ctx, key, "node-a", Record{Reply: []byte("ok")}, "", time.Minute)
	require.NoError(t, err)
	require.True(t, result.Owned)

	begin, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginDone, begin.State)
	require.Equal(t, []byte("ok"), begin.Record.Reply)

	begin, err = backend.Begin(ctx, key, other, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginMismatch, begin.State)
}

func TestRedisBackendTombstoneAnswersAnyFingerprint(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 2}

	cancel, err := backend.Cancel(ctx, key, testCancelled, time.Minute)
	require.NoError(t, err)
	require.Equal(t, CancelTombstone, cancel.State)

	begin, err := backend.Begin(ctx, key, Fingerprint("users.put", []byte("z")), "node-a", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginCancelled, begin.State)
	require.Equal(t, [32]byte{}, begin.Record.Fingerprint)
	require.Equal(t, testCancelled.Code, begin.Record.Err.Code)
	require.Equal(t, testCancelled.Message, begin.Record.Err.Message)
}

func TestRedisBackendCompleteByAnotherNodeIsRefused(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 3}
	fingerprint := Fingerprint("users.get", []byte("x"))

	begin, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginMiss, begin.State)

	result, err := backend.Complete(ctx, key, "node-b", Record{Reply: []byte("stolen")}, "", time.Minute)
	require.NoError(t, err)
	require.False(t, result.Owned)

	// The in-flight record is untouched: the owner still runs the call.
	begin, err = backend.Begin(ctx, key, fingerprint, "node-c", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginInFlight, begin.State)
	require.Equal(t, "node-a", begin.Owner)
}

func TestRedisBackendCompleteAbandonedDeletesRecord(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 4}
	fingerprint := Fingerprint("users.get", []byte("x"))

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	result, err := backend.Complete(ctx, key, "node-a", Record{Abandoned: true}, "", time.Minute)
	require.NoError(t, err)
	require.True(t, result.Owned)
	require.False(t, integrationExists(t, connector, RedisKey(prefix, key)))

	// A duplicate runs at once: the key is free again.
	begin, err := backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginMiss, begin.State)
}

func TestRedisBackendBeginRegistersWaiter(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 20}
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)
	_, found := integrationField(t, connector, redisKey, "w")
	require.False(t, found, "the owner's own begin registers no waiter")

	begin, err := backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)
	require.Equal(t, BeginInFlight, begin.State)
	waiters, _ := integrationField(t, connector, redisKey, "w")
	require.Equal(t, "node-b", waiters)

	_, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)
	waiters, _ = integrationField(t, connector, redisKey, "w")
	require.Equal(t, "node-b", waiters, "a node is listed once")

	_, err = backend.Begin(ctx, key, fingerprint, "node-c", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)
	waiters, _ = integrationField(t, connector, redisKey, "w")
	require.Equal(t, "node-b node-c", waiters)

	_, err = backend.Begin(ctx, key, fingerprint, "node-d", integrationTTL, 0)
	require.NoError(t, err)
	waiters, _ = integrationField(t, connector, redisKey, "w")
	require.Equal(t, "node-b node-c", waiters, "a begin without register does not register")
}

func TestRedisBackendBeginWithoutRegisterWritesNoWaiter(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 21}
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	begin, err := backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginInFlight, begin.State)
	_, found := integrationField(t, connector, redisKey, "w")
	require.False(t, found)
}

func TestRedisBackendBeginWaiterCapIsFull(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 22}
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	_, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 2)
	require.NoError(t, err)
	_, err = backend.Begin(ctx, key, fingerprint, "node-c", integrationTTL, 2)
	require.NoError(t, err)

	// The list holds two nodes: node-d is beyond it, and is told so without being listed.
	begin, err := backend.Begin(ctx, key, fingerprint, "node-d", integrationTTL, 2)
	require.NoError(t, err)
	require.Equal(t, BeginInFlight, begin.State)
	require.True(t, begin.WaitersFull)
	waiters, _ := integrationField(t, connector, redisKey, "w")
	require.Equal(t, "node-b node-c", waiters)

	// A node which is listed already is not full, whatever the cap.
	begin, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 2)
	require.NoError(t, err)
	require.False(t, begin.WaitersFull)
}

func TestRedisBackendCompleteReturnsAndDropsWaiters(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 23}
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	_, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)
	_, err = backend.Begin(ctx, key, fingerprint, "node-c", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)

	// Another node does not own the record: nothing is written, and the waiters stay.
	result, err := backend.Complete(ctx, key, "node-b", Record{Reply: []byte("stolen")}, "", time.Minute)
	require.NoError(t, err)
	require.False(t, result.Owned)
	require.Empty(t, result.Waiters)
	_, found := integrationField(t, connector, redisKey, "w")
	require.True(t, found)

	result, err = backend.Complete(ctx, key, "node-a", Record{Reply: []byte("ok")}, "", time.Minute)
	require.NoError(t, err)
	require.True(t, result.Owned)
	require.Equal(t, []string{"node-b", "node-c"}, result.Waiters)
	_, found = integrationField(t, connector, redisKey, "w")
	require.False(t, found, "a done record drops its waiters")
	_, found = integrationField(t, connector, redisKey, "n")
	require.False(t, found)
}

func TestRedisBackendCompleteAbandonedReturnsWaiters(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 24}
	fingerprint := Fingerprint("users.get", []byte("x"))

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	_, err = backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, integrationMaxWaiters)
	require.NoError(t, err)

	result, err := backend.Complete(ctx, key, "node-a", Record{Abandoned: true}, "", time.Minute)
	require.NoError(t, err)
	require.True(t, result.Owned)
	require.Equal(t, []string{"node-b"}, result.Waiters)
	require.False(t, integrationExists(t, connector, RedisKey(prefix, key)))
}

func TestRedisBackendCompleteKeepsDoneRecordWithTTL(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 5}
	fingerprint := Fingerprint("users.get", []byte("x"))
	redisKey := RedisKey(prefix, key)

	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	duplicate, err := backend.Begin(ctx, key, fingerprint, "node-b", integrationTTL, 0)
	require.NoError(t, err)
	require.Equal(t, BeginInFlight, duplicate.State)
	// The in-flight TTL is the method timeout plus the grace.
	inflightPTTL := integrationPTTL(t, connector, redisKey)
	require.Greater(t, inflightPTTL, (5 * time.Second).Milliseconds())
	require.LessOrEqual(t, inflightPTTL, (5*time.Second + InflightGrace).Milliseconds())

	result, err := backend.Complete(ctx, key, "node-a", Record{Reply: []byte("ok")}, "", time.Minute)
	require.NoError(t, err)
	require.True(t, result.Owned)

	state, found := integrationField(t, connector, redisKey, "s")
	require.True(t, found)
	require.Equal(t, "d", state)
	_, owned := integrationField(t, connector, redisKey, "n")
	require.False(t, owned, "a done record has no owner")
	donePTTL := integrationPTTL(t, connector, redisKey)
	require.Greater(t, donePTTL, int64(0))
	require.LessOrEqual(t, donePTTL, time.Minute.Milliseconds())
}

func TestRedisBackendUncachedCompleteKeepsNoReply(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 6}
	redisKey := RedisKey(prefix, key)

	_, err := backend.Begin(ctx, key, Fingerprint("users.get", []byte("x")), "node-a", integrationTTL, 0)
	require.NoError(t, err)
	result, err := backend.Complete(ctx, key, "node-a", Record{Reply: []byte("big"), Uncached: true}, "", time.Minute)
	require.NoError(t, err)
	require.True(t, result.Owned)

	uncached, found := integrationField(t, connector, redisKey, "u")
	require.True(t, found)
	require.Equal(t, "1", uncached)
	_, hasReply := integrationField(t, connector, redisKey, "r")
	require.False(t, hasReply)

	poll, err := backend.Poll(ctx, key)
	require.NoError(t, err)
	require.Equal(t, PollDone, poll.State)
	require.True(t, poll.Record.Uncached)
}

func TestRedisBackendCancelResults(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	fingerprint := Fingerprint("users.get", []byte("x"))

	// Nothing runs: a tombstone.
	nothing := Key{Scope: testScope("u1", "a"), MsgID: 7}
	res, err := backend.Cancel(ctx, nothing, testCancelled, time.Minute)
	require.NoError(t, err)
	require.Equal(t, CancelTombstone, res.State)

	// A call runs on node-a: the owner is returned, and nothing is written.
	running := Key{Scope: testScope("u1", "a"), MsgID: 8}
	_, err = backend.Begin(ctx, running, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	res, err = backend.Cancel(ctx, running, testCancelled, time.Minute)
	require.NoError(t, err)
	require.Equal(t, CancelInFlight, res.State)
	require.Equal(t, "node-a", res.Owner)

	// A done call is left as it is.
	done := Key{Scope: testScope("u1", "a"), MsgID: 9}
	_, err = backend.Begin(ctx, done, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	_, err = backend.Complete(ctx, done, "node-a", Record{Reply: []byte("ok")}, "", time.Minute)
	require.NoError(t, err)
	res, err = backend.Cancel(ctx, done, testCancelled, time.Minute)
	require.NoError(t, err)
	require.Equal(t, CancelDone, res.State)
}

func TestRedisBackendAckDeletesDoneAndTombstonesOnly(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	scope := testScope("u1", "a")
	fingerprint := Fingerprint("users.get", []byte("x"))
	done := Key{Scope: scope, MsgID: 10}
	tombstone := Key{Scope: scope, MsgID: 11}
	inflight := Key{Scope: scope, MsgID: 12}

	_, err := backend.Begin(ctx, done, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	_, err = backend.Complete(ctx, done, "node-a", Record{Reply: []byte("ok")}, "", time.Minute)
	require.NoError(t, err)
	_, err = backend.Cancel(ctx, tombstone, testCancelled, time.Minute)
	require.NoError(t, err)
	_, err = backend.Begin(ctx, inflight, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)

	require.NoError(t, backend.Ack(ctx, []ScopeAcks{{Scope: scope, MsgIDs: []uint64{10, 11, 12}}}))
	require.False(t, integrationExists(t, connector, RedisKey(prefix, done)))
	require.False(t, integrationExists(t, connector, RedisKey(prefix, tombstone)))
	require.True(t, integrationExists(t, connector, RedisKey(prefix, inflight)))
}

func TestRedisBackendCancelledWithoutErrorIsMalformed(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 14}
	fingerprint := Fingerprint("users.get", []byte("x"))

	// A cancelled key with no error, as a write of another client under the prefix leaves it. It is not answered as a
	// success with an empty reply: the begin and the poll reply are malformed.
	integrationDo(connector, func(c rueidis.Client) rueidis.Completed {
		return c.B().Hset().Key(RedisKey(prefix, key)).FieldValue().FieldValue("s", "c").Build()
	})
	_, err := backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.ErrorIs(t, err, errMalformedReply)
	_, err = backend.Poll(ctx, key)
	require.ErrorIs(t, err, errMalformedReply)
}

func TestRedisBackendPollStates(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, _ := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 13}
	fingerprint := Fingerprint("users.get", []byte("x"))

	poll, err := backend.Poll(ctx, key)
	require.NoError(t, err)
	require.Equal(t, PollMissing, poll.State)

	_, err = backend.Begin(ctx, key, fingerprint, "node-a", integrationTTL, 0)
	require.NoError(t, err)
	poll, err = backend.Poll(ctx, key)
	require.NoError(t, err)
	require.Equal(t, PollInFlight, poll.State)

	_, err = backend.Complete(ctx, key, "node-a", Record{Reply: []byte("ok")}, "", time.Minute)
	require.NoError(t, err)
	poll, err = backend.Poll(ctx, key)
	require.NoError(t, err)
	require.Equal(t, PollDone, poll.State)
	require.Equal(t, []byte("ok"), poll.Record.Reply)
}

func TestRedisBackendNotConnected(t *testing.T) {
	prefix := uniqueRedisPrefix()
	configs, err := redisshard.ParseRedisShardConfigs(configtypes.Redis{Address: []string{integrationRedisAddress}})
	require.NoError(t, err)
	connector := redisshard.NewConnector(configs, nil)
	backend := NewRedis(prefix, connector, 0, nil)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 14}

	require.False(t, backend.Connected())
	_, err = backend.Begin(ctx, key, Fingerprint("users.get", []byte("x")), "node-a", integrationTTL, 0)
	require.ErrorIs(t, err, redisshard.ErrNotConnected)

	require.NoError(t, connector.Connect())
	require.True(t, backend.Connected())
	connector.Close()
	require.False(t, backend.Connected())
	_, err = backend.Begin(ctx, key, Fingerprint("users.get", []byte("x")), "node-a", integrationTTL, 0)
	require.ErrorIs(t, err, redisshard.ErrNotConnected)
}

func TestRedisBackendKeyFormatInRedis(t *testing.T) {
	prefix := uniqueRedisPrefix()
	backend, connector := newIntegrationBackend(t, prefix)
	ctx := context.Background()
	key := Key{Scope: testScope("u1", "a"), MsgID: 15}

	_, err := backend.Begin(ctx, key, Fingerprint("users.get", []byte("x")), "node-a", integrationTTL, 0)
	require.NoError(t, err)
	// The key is the prefix, the hash tag of the scope and the decimal msg_id.
	require.True(t, integrationExists(t, connector, prefix+".rpc.{"+key.Scope+"}."+strconv.FormatUint(key.MsgID, 10)))
}
