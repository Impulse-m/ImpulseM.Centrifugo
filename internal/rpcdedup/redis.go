package rpcdedup

import (
	"context"
	"strconv"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/redis/rueidis"
	"golang.org/x/sync/errgroup"
)

// InflightGrace is the time an in-flight record is kept beyond the method timeout of its call. The owner
// completes the record within the method timeout, so the grace only matters when the owner node died.
const InflightGrace = 10 * time.Second

// BeginState is the state of a key which Backend.Begin found, or started.
type BeginState uint8

const (
	// BeginMiss means the key was free, and the call was started by the caller.
	BeginMiss BeginState = iota
	// BeginDone means the key holds an answer, which BeginResult.Record carries.
	BeginDone
	// BeginInFlight means the call runs on the node BeginResult.Owner.
	BeginInFlight
	// BeginMismatch means the key holds a call with another fingerprint.
	BeginMismatch
	// BeginCancelled means the key holds a tombstone, which answers any fingerprint. BeginResult.Record carries the
	// cancelled error, and no fingerprint.
	BeginCancelled
)

// BeginResult is the result of Backend.Begin.
type BeginResult struct {
	State  BeginState
	Record Record
	Owner  string
	// WaitersFull is set with BeginInFlight when the caller asked to be listed as a waiter and the record lists the
	// maximum of waiter nodes already. The caller is not listed, and must not wait.
	WaitersFull bool
}

// CancelState is the state of a key which Backend.Cancel found, or left.
type CancelState uint8

const (
	// CancelTombstone means nothing was held, and the key now holds the cancelled error.
	CancelTombstone CancelState = iota
	// CancelInFlight means the call runs on the node CancelResult.Owner.
	CancelInFlight
	// CancelDone means the key holds an answer, which is left as it is.
	CancelDone
)

// CancelResult is the result of Backend.Cancel.
type CancelResult struct {
	State CancelState
	Owner string
}

// PollState is the state of a key which Backend.Poll found.
type PollState uint8

const (
	// PollInFlight means the call still runs.
	PollInFlight PollState = iota
	// PollDone means the call is done, and PollResult.Record is its answer.
	PollDone
	// PollCancelled means the key holds a tombstone, which answers any fingerprint. PollResult.Record carries the
	// cancelled error, and no fingerprint.
	PollCancelled
	// PollMissing means the key is gone: the owner abandoned the call, or died, and its record expired.
	PollMissing
)

// PollResult is the result of Backend.Poll.
type PollResult struct {
	State  PollState
	Record Record
}

// ScopeAcks are the msg_ids of one scope which the client acknowledged.
type ScopeAcks struct {
	Scope  string
	MsgIDs []uint64
}

// CompleteResult is the result of Backend.Complete.
type CompleteResult struct {
	// Owned is whether this node owned the in-flight record, so the write took effect.
	Owned bool
	// Waiters are the ids of the nodes which registered as waiters of the record, when it was owned. Empty otherwise.
	Waiters []string
}

// Backend is the shared tier of a Shared store. Redis implements it; tests use a fake. Complete takes errJSON, the
// encoded error of rec, which the caller encodes once. Begin with maxWaiters above zero lists nodeID as a waiter of a
// call which runs on another node, in a list of at most maxWaiters nodes, which the owner wakes when the record is
// written. Wake writes a wake for key to the wake stream of each of nodes. WatchWakes reads the wakes of nodeID.
type Backend interface {
	Begin(ctx context.Context, key Key, fingerprint [32]byte, nodeID string, inflightTTL time.Duration, maxWaiters int) (BeginResult, error)
	Complete(ctx context.Context, key Key, nodeID string, rec Record, errJSON string, ttl time.Duration) (CompleteResult, error)
	// Wake writes a wake for key to the wake stream of each of nodes, on the shard of key. It returns how many wakes were
	// written, and the first error of the others.
	Wake(ctx context.Context, key Key, nodes []string) (int, error)
	// WatchWakes reads the wakes of nodeID on every shard and calls wake for each, until ctx is done or idle reports
	// true at an empty read. It returns when every shard loop returned.
	WatchWakes(ctx context.Context, nodeID string, wake func(Key), idle func() bool) error
	Cancel(ctx context.Context, key Key, failure *centrifuge.Error, ttl time.Duration) (CancelResult, error)
	Poll(ctx context.Context, key Key) (PollResult, error)
	Ack(ctx context.Context, acks []ScopeAcks) error
	Connected() bool
}

// BackendObserver observes the Redis calls of the shared tier: the duration of every call, and every failed one, and
// the wake watchers which are reading. Methods may be called from any goroutine.
type BackendObserver interface {
	RedisCall(seconds float64)
	RedisError()
	// WakeWatching is called with +1 when a wake watcher of a shard starts reading, and -1 when it stops.
	WakeWatching(delta int)
}

// Redis is the Backend over the shards of a Connector. Every record of a scope is on the shard of the scope, and
// every script touches the keys of one scope only.
type Redis struct {
	prefix    string
	connector *redisshard.Connector
	timeout   time.Duration
	observer  BackendObserver
	// readBlock is the block of a wake read, wakeReadBlock unless a test shortens it.
	readBlock time.Duration
	// errorLog limits the log of the wake watchers' read errors to one per redisshard.ErrorLogInterval.
	errorLog *tools.IntervalLimiter
	// ackFlush runs the acks of one shard group, see Redis.Ack. It is flushAckShard unless a test replaces it.
	ackFlush func(callCtx context.Context, shard *redisshard.RedisShard, execs []rueidis.LuaExec) error
}

// NewRedis returns the Redis backend of the keys with prefix. The connector may not be connected yet: calls made
// while it is not connected fail with redisshard.ErrNotConnected. ioTimeout bounds each call, zero means the
// default. observer may be nil.
func NewRedis(prefix string, connector *redisshard.Connector, ioTimeout time.Duration, observer BackendObserver) *Redis {
	if ioTimeout <= 0 {
		ioTimeout = redisshard.DefaultIOTimeout
	}
	r := &Redis{
		prefix:    prefix,
		connector: connector,
		timeout:   ioTimeout,
		observer:  observer,
		readBlock: wakeReadBlock,
		errorLog:  tools.NewIntervalLimiter(redisshard.ErrorLogInterval),
	}
	r.ackFlush = r.flushAckShard
	return r
}

// Begin implements Backend with the begin script. maxWaiters is the cap of the waiter list, zero for none.
func (r *Redis) Begin(ctx context.Context, key Key, fingerprint [32]byte, nodeID string, inflightTTL time.Duration, maxWaiters int) (BeginResult, error) {
	var result BeginResult
	err := r.exec(ctx, key.Scope, func(callCtx context.Context, shard *redisshard.RedisShard) error {
		args := []string{string(fingerprint[:]), nodeID, redisTTLMillis(inflightTTL), strconv.Itoa(maxWaiters)}
		res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return beginScript.Exec(callCtx, client, []string{RedisKey(r.prefix, key)}, args)
		})
		parts, err := replyStrings(res)
		if err != nil {
			return err
		}
		result, err = parseBegin(parts)
		return err
	})
	return result, err
}

// Complete implements Backend with the complete script. It returns Owned false, and no error, when this node does not own
// the in-flight record. That is not a failed call, so this method does not count it as a Redis error: a retried write finds
// the record done by its first attempt, whose reply was lost. The caller counts the ownership it lost on its first write.
func (r *Redis) Complete(ctx context.Context, key Key, nodeID string, rec Record, errJSON string, ttl time.Duration) (CompleteResult, error) {
	reply, uncached := recordArgs(rec)
	abandoned := "0"
	if rec.Abandoned {
		abandoned = "1"
	}
	args := []string{nodeID, reply, errJSON, uncached, redisTTLMillis(ttl), abandoned}
	var result CompleteResult
	err := r.exec(ctx, key.Scope, func(callCtx context.Context, shard *redisshard.RedisShard) error {
		res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return completeScript.Exec(callCtx, client, []string{RedisKey(r.prefix, key)}, args)
		})
		parts, err := replyStrings(res)
		if err != nil {
			return err
		}
		result, err = parseComplete(parts)
		return err
	})
	if err != nil {
		return CompleteResult{}, err
	}
	return result, nil
}

// Cancel implements Backend with the cancel script.
func (r *Redis) Cancel(ctx context.Context, key Key, failure *centrifuge.Error, ttl time.Duration) (CancelResult, error) {
	args := []string{encodeError(failure), redisTTLMillis(ttl)}
	var result CancelResult
	err := r.exec(ctx, key.Scope, func(callCtx context.Context, shard *redisshard.RedisShard) error {
		res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return cancelScript.Exec(callCtx, client, []string{RedisKey(r.prefix, key)}, args)
		})
		parts, err := replyStrings(res)
		if err != nil {
			return err
		}
		result, err = parseCancel(parts)
		return err
	})
	return result, err
}

// Poll implements Backend with HMGET of the fields the watcher reads.
func (r *Redis) Poll(ctx context.Context, key Key) (PollResult, error) {
	var result PollResult
	err := r.exec(ctx, key.Scope, func(callCtx context.Context, shard *redisshard.RedisShard) error {
		res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(callCtx, client.B().Hmget().Key(RedisKey(r.prefix, key)).Field(
				redisFieldState, redisFieldReply, redisFieldError, redisFieldUncached,
			).Build())
		})
		parts, err := replyStrings(res)
		if err != nil {
			return err
		}
		if len(parts) != 4 {
			return errMalformedReply
		}
		result, err = parsePoll(parts[0], parts[1], parts[2], parts[3])
		return err
	})
	return result, err
}

// Ack implements Backend. The acks of each shard are one pipeline, with one ack script per scope. A failed shard
// is counted, and the first error is returned; the records of the failed keys expire at their TTL. While the
// connector is not connected, each call counts one error, so an outage counts once per flush. The shards are flushed
// in parallel, so a Redis which accepts connections but does not answer delays each flush by one IO timeout, however
// many shards stall; the shutdown flush waits for it too.
func (r *Redis) Ack(ctx context.Context, acks []ScopeAcks) error {
	shards := r.connector.Shards()
	if shards == nil {
		r.countError()
		return redisshard.ErrNotConnected
	}
	groups := make(map[int][]rueidis.LuaExec)
	for i := range acks {
		if len(acks[i].MsgIDs) == 0 {
			continue
		}
		index := redisshard.ShardIndex(acks[i].Scope, len(shards))
		groups[index] = append(groups[index], ackExec(r.prefix, acks[i]))
	}
	return ackShards(ctx, groups, func(ctx context.Context, index int, execs []rueidis.LuaExec) error {
		return r.call(ctx, shards[index], func(callCtx context.Context, shard *redisshard.RedisShard) error {
			return r.ackFlush(callCtx, shard, execs)
		})
	})
}

// flushAckShard runs the ack script of each scope of execs on shard, as one pipeline, and returns the first failed
// command.
func (r *Redis) flushAckShard(callCtx context.Context, shard *redisshard.RedisShard, execs []rueidis.LuaExec) error {
	results := shard.RunMulti(func(client rueidis.Client) []rueidis.RedisResult {
		return ackScript.ExecMulti(callCtx, client, execs...)
	})
	for i := range results {
		if err := results[i].Error(); err != nil {
			return err
		}
	}
	return nil
}

// ackShards runs flush for each shard group of groups, in a goroutine of its own, and returns when all have returned.
// It returns the error of one of the groups which failed, or nil. Each flush bounds its own time, so the flushes of
// stalled shards overlap.
func ackShards(
	ctx context.Context,
	groups map[int][]rueidis.LuaExec,
	flush func(ctx context.Context, index int, execs []rueidis.LuaExec) error,
) error {
	var eg errgroup.Group
	for index, execs := range groups {
		eg.Go(func() error {
			return flush(ctx, index, execs)
		})
	}
	return eg.Wait()
}

// Connected reports whether the connector has shards.
func (r *Redis) Connected() bool {
	return r.connector.Shards() != nil
}

// ackExec returns the ack script call of the keys of one scope.
func ackExec(prefix string, acks ScopeAcks) rueidis.LuaExec {
	keys := make([]string, len(acks.MsgIDs))
	for i, msgID := range acks.MsgIDs {
		keys[i] = RedisKey(prefix, Key{Scope: acks.Scope, MsgID: msgID})
	}
	return rueidis.LuaExec{Keys: keys}
}

// exec runs op on the shard of scope. A scope on no shard, which means the connector is not connected, is a
// Redis error.
func (r *Redis) exec(ctx context.Context, scope string, op func(callCtx context.Context, shard *redisshard.RedisShard) error) error {
	shard, err := r.shardOf(scope)
	if err != nil {
		r.countError()
		return err
	}
	return r.call(ctx, shard, op)
}

// call runs op on shard with the IO timeout. It observes the duration of the call, and counts a failure.
func (r *Redis) call(ctx context.Context, shard *redisshard.RedisShard, op func(callCtx context.Context, shard *redisshard.RedisShard) error) error {
	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	start := time.Now()
	err := op(callCtx, shard)
	if r.observer != nil {
		r.observer.RedisCall(time.Since(start).Seconds())
	}
	if err != nil {
		r.countError()
		return err
	}
	return nil
}

// shardOf returns the shard of scope, or redisshard.ErrNotConnected while the connector has no shards.
func (r *Redis) shardOf(scope string) (*redisshard.RedisShard, error) {
	shards := r.connector.Shards()
	if shards == nil {
		return nil, redisshard.ErrNotConnected
	}
	return shards[redisshard.ShardIndex(scope, len(shards))], nil
}

// countError counts a failed Redis call.
func (r *Redis) countError() {
	if r.observer != nil {
		r.observer.RedisError()
	}
}

// replyStrings returns the elements of an array reply as strings. A nil element is "", which is how a missing
// field of a record reads.
func replyStrings(res rueidis.RedisResult) ([]string, error) {
	msgs, err := res.ToArray()
	if err != nil {
		return nil, err
	}
	parts := make([]string, len(msgs))
	for i := range msgs {
		if msgs[i].IsNil() {
			continue
		}
		s, err := msgs[i].ToString()
		if err != nil {
			return nil, err
		}
		parts[i] = s
	}
	return parts, nil
}
