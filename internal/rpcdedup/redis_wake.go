package rpcdedup

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/redis/rueidis"
	"github.com/rs/zerolog/log"
)

// The wake streams of a node. A stream is trimmed to about wakeStreamMaxLen entries, and it expires wakeStreamTTL after
// its last write, so a stream which nobody reads, or which belongs to a node that restarted, is bounded in memory and
// in time. A wake watcher reads at most wakeReadCount entries per read, blocks for wakeReadBlock on an empty stream,
// and backs off from wakeRetryMin to wakeRetryMax after a failed read.
const (
	wakeStreamTTL    = 60 * time.Second
	wakeStreamMaxLen = 4096
	wakeReadCount    = 256
	wakeReadBlock    = 5 * time.Second
	wakeRetryMin     = 200 * time.Millisecond
	wakeRetryMax     = 5 * time.Second
)

// Wake implements Backend with one pipelined call on the shard of key. Per node it adds the wake to the stream of the
// node, with XADD trimmed to wakeStreamMaxLen, and sets the stream to expire wakeStreamTTL after the write. It returns
// how many nodes were written and the first error. The call is not retried: a lost wake costs its wait one read at
// the check.
func (r *Redis) Wake(ctx context.Context, key Key, nodes []string) (int, error) {
	if len(nodes) == 0 {
		return 0, nil
	}
	maxLen := strconv.Itoa(wakeStreamMaxLen)
	ttl := wakeStreamTTL.Milliseconds()
	msgID := strconv.FormatUint(key.MsgID, 10)
	written := 0
	err := r.exec(ctx, key.Scope, func(callCtx context.Context, shard *redisshard.RedisShard) error {
		results := shard.RunMulti(func(client rueidis.Client) []rueidis.RedisResult {
			cmds := make(rueidis.Commands, 0, 2*len(nodes))
			for _, nodeID := range nodes {
				stream := WakeStreamKey(r.prefix, nodeID)
				cmds = append(cmds,
					client.B().Xadd().Key(stream).Maxlen().Almost().Threshold(maxLen).Id("*").FieldValue().FieldValue(
						redisWakeFieldScope, key.Scope,
					).FieldValue(redisWakeFieldMsgID, msgID).Build(),
					client.B().Pexpire().Key(stream).Milliseconds(ttl).Build(),
				)
			}
			return client.DoMulti(callCtx, cmds...)
		})
		var firstErr error
		for i := range nodes {
			err := results[2*i].Error()
			if err == nil {
				err = results[2*i+1].Error()
			}
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			written++
		}
		return firstErr
	})
	return written, err
}

// WatchWakes implements Backend with one watchShard per shard of the connector, and waits for them. A shard is resolved
// by its index at every read, so a reconnect which rebuilt the shards is followed. It returns
// redisshard.ErrNotConnected when the connector has no shards at the start.
func (r *Redis) WatchWakes(ctx context.Context, nodeID string, wake func(Key), idle func() bool) error {
	shards := r.connector.Shards()
	if shards == nil {
		r.countError()
		return redisshard.ErrNotConnected
	}
	stream := WakeStreamKey(r.prefix, nodeID)
	var wg sync.WaitGroup
	for index := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.watchShard(ctx, index, stream, wake, idle)
		}()
	}
	wg.Wait()
	return nil
}

// watchShard is the loop of the wake watcher of one shard. It reads the stream from a cursor, which is the last entry it
// processed, and deletes the entries it processed. An empty read, which is a block that timed out, ends the loop when
// idle reports true; a failed read is counted, logged at most once per interval, and read again from the start of the
// stream after a backoff. The loop ends when ctx is done. It is bracketed by the gauge of the wake watchers.
func (r *Redis) watchShard(ctx context.Context, index int, stream string, wake func(Key), idle func() bool) {
	if r.observer != nil {
		r.observer.WakeWatching(1)
		defer r.observer.WakeWatching(-1)
	}
	cursor := "0-0"
	retry := newRetryBackOff(wakeRetryMin, wakeRetryMax)
	for ctx.Err() == nil {
		shard, err := r.watchedShard(index)
		var entries []wakeEntry
		if err == nil {
			entries, err = r.readWakes(ctx, shard, stream, cursor)
		}
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			r.countError()
			r.logWatchError(err)
			cursor = "0-0"
			timer := time.NewTimer(retry.NextBackOff())
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		case len(entries) == 0:
			cursor = "0-0"
			retry.Reset()
			if idle() {
				return
			}
		default:
			retry.Reset()
			for _, entry := range entries {
				if !entry.malformed {
					wake(entry.key)
				}
			}
			cursor = entries[len(entries)-1].id
			r.deleteWakes(ctx, shard, stream, entries)
		}
	}
}

// readWakes makes one blocking XREAD of the stream of shard from cursor. It returns the entries read, none for an empty
// read. A malformed entry is returned with its malformed flag and counted as a Redis error; any other failure is
// returned as the error.
func (r *Redis) readWakes(ctx context.Context, shard *redisshard.RedisShard, stream string, cursor string) ([]wakeEntry, error) {
	readCtx, cancel := context.WithTimeout(ctx, r.readBlock+r.timeout)
	defer cancel()
	res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(readCtx, client.B().Xread().Count(wakeReadCount).Block(r.readBlock.Milliseconds()).Streams().Key(stream).Id(cursor).Build())
	})
	if rueidis.IsRedisNil(res.Error()) {
		return nil, nil
	}
	streams, err := res.AsXRead()
	if err != nil {
		return nil, err
	}
	entries := parseWakeEntries(streams)
	for _, entry := range entries {
		if entry.malformed {
			r.countError()
			break
		}
	}
	return entries, nil
}

// deleteWakes deletes the entries which watchShard processed from the stream of shard. A failed delete is counted and not
// retried: the entries expire with the stream, and a wake read again is one read of a wait which may be over.
func (r *Redis) deleteWakes(ctx context.Context, shard *redisshard.RedisShard, stream string, entries []wakeEntry) {
	ids := make([]string, len(entries))
	for i := range entries {
		ids[i] = entries[i].id
	}
	_ = r.call(ctx, shard, func(callCtx context.Context, s *redisshard.RedisShard) error {
		return s.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(callCtx, client.B().Xdel().Key(stream).Id(ids...).Build())
		}).Error()
	})
}

// watchedShard returns the shard of index, or redisshard.ErrNotConnected while the connector has no such shard.
func (r *Redis) watchedShard(index int) (*redisshard.RedisShard, error) {
	shards := r.connector.Shards()
	if index >= len(shards) {
		return nil, redisshard.ErrNotConnected
	}
	return shards[index], nil
}

// logWatchError logs a failed read of a wake watcher at most once per redisshard.ErrorLogInterval.
func (r *Redis) logWatchError(err error) {
	if r.errorLog.Allow() {
		log.Error().Err(err).Msg("exactly-once RPC: a wake watcher failed to read its wakes, it reads again after a backoff")
	}
}
