package accessrevoke

import (
	"context"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
	"golang.org/x/time/rate"
)

// disconnectTerminalCode is the lowest code of a terminal disconnect, which tells the client not to reconnect. A
// queued disconnect is replaced by a terminal one, so a terminal disconnect is never lost to a reconnectable one.
const disconnectTerminalCode uint32 = 4500

// Connection is a local client connection which can be disconnected and indexed by its ID. Satisfied by
// *centrifuge.Client. BeginDisconnect starts the close and Closing reports whether a close has begun, so the queue never
// disconnects a connection twice and never spends a rate token on one which is closing already.
type Connection interface {
	ID() string
	BeginDisconnect(disconnect centrifuge.Disconnect) bool
	Closing() bool
}

var _ Connection = (*centrifuge.Client)(nil)

// queuedDisconnect is the disconnect of a connection in the queue, and the reason it is counted with.
type queuedDisconnect struct {
	disconnect centrifuge.Disconnect
	reason     Reason
}

// retryKey names what a dropped disconnect was for: the connections of a user (user set), or the indexed connections of
// a session without a user (user empty). A retry re-reads the store, so the key carries no reason and no disconnect.
// The memory of the kept keys is bounded by the dropped disconnects, one key per user or session.
type retryKey struct {
	user    string
	session [16]byte
}

// DisconnectQueue drains live disconnects caused by access revocation at a limited rate. The queue is bounded and holds
// each connection at most once. It never queues or disconnects a connection whose close has begun. A connection which is
// queued keeps the first disconnect it was queued with, except that a terminal disconnect replaces a reconnectable one.
// A disconnect which finds the queue full is dropped and counted, and its key is kept. The queue goroutine re-evaluates
// kept keys through the store once the queue is at most half full: one key per drained item while the queue holds items,
// and the next key in a row while it is empty. The refresh check rejects a connection within the token lifetime in any case.
// A key whose retry pass dropped a disconnect again is held until the queue is at or below half of its size, so a user
// with many connections is re-scanned once per half drain of the queue, not once per dequeue.
// The kept keys are not capped: a key is one user or session, so the keys cost no more than the dropped disconnects, and
// the retry walks only the connections of the key, never the whole node.
type DisconnectQueue struct {
	items   chan Connection
	limiter *rate.Limiter

	mu       sync.Mutex
	inflight map[string]queuedDisconnect
	retry    map[retryKey]struct{}
	held     map[retryKey]struct{}
	retrier  func(key retryKey)
}

// NewDisconnectQueue creates a queue which disconnects at most ratePerSecond connections per second
// and holds at most size pending disconnects.
func NewDisconnectQueue(ratePerSecond int, size int) *DisconnectQueue {
	return &DisconnectQueue{
		items:   make(chan Connection, size),
		limiter: rate.NewLimiter(rate.Limit(ratePerSecond), 1),
	}
}

// enqueueKeyed adds a disconnect of key without blocking. A connection which is closing or already queued is not queued
// again, and the call returns true. It returns false when the queue is full, in which case the disconnect is dropped and
// key is kept for a retry in the same critical section as the full queue check, so the queue goroutine cannot drain the
// queue between the drop and the record.
func (q *DisconnectQueue) enqueueKeyed(
	key retryKey,
	connection Connection,
	disconnect centrifuge.Disconnect,
	reason Reason,
) bool {
	return q.enqueue(key, connection, disconnect, reason, false)
}

// requeueRetry is enqueueKeyed for a disconnect found by a retry pass of key. A drop by a retry pass is not a new dropped
// disconnect, since the first drop counted it, and key is held until the queue is at or below half of its size.
func (q *DisconnectQueue) requeueRetry(
	key retryKey,
	connection Connection,
	disconnect centrifuge.Disconnect,
	reason Reason,
) {
	q.enqueue(key, connection, disconnect, reason, true)
}

// enqueue adds the disconnect without blocking. When the queue is full it counts the drop unless retrying is set, and it
// keeps key for a retry, held when retrying is set.
func (q *DisconnectQueue) enqueue(
	key retryKey,
	connection Connection,
	disconnect centrifuge.Disconnect,
	reason Reason,
	retrying bool,
) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if connection.Closing() {
		return true
	}
	id := connection.ID()
	if queued, ok := q.inflight[id]; ok {
		if !isTerminal(queued.disconnect) && isTerminal(disconnect) {
			q.inflight[id] = queuedDisconnect{disconnect: disconnect, reason: reason}
		}
		return true
	}
	select {
	case q.items <- connection:
		if q.inflight == nil {
			q.inflight = make(map[string]queuedDisconnect)
		}
		q.inflight[id] = queuedDisconnect{disconnect: disconnect, reason: reason}
		return true
	default:
	}
	if !retrying {
		metrics.AccessRevokeDisconnectsDroppedTotal.Inc()
	}
	q.keepRetryLocked(key, retrying)
	return false
}

// keepRetryLocked keeps key for a retry, in the held set when held is set. A key which is already held stays held. It is
// called with mu held.
func (q *DisconnectQueue) keepRetryLocked(key retryKey, held bool) {
	if held {
		delete(q.retry, key)
		q.held = addRetryKey(q.held, key)
		return
	}
	if _, isHeld := q.held[key]; isHeld {
		return
	}
	q.retry = addRetryKey(q.retry, key)
}

// releaseHeldLocked moves the held keys to the retry set once the queue is at or below half of its size. It is called
// with mu held.
func (q *DisconnectQueue) releaseHeldLocked() {
	if len(q.held) == 0 || len(q.items) > cap(q.items)/2 {
		return
	}
	for key := range q.held {
		q.retry = addRetryKey(q.retry, key)
	}
	q.held = nil
}

// addRetryKey adds key to keys, creating the set when it is nil, and returns the set.
func addRetryKey(keys map[retryKey]struct{}, key retryKey) map[retryKey]struct{} {
	if keys == nil {
		keys = make(map[retryKey]struct{})
	}
	keys[key] = struct{}{}
	return keys
}

// pending reports whether the connection is closing or queued, so a pass which re-evaluates it has nothing to queue.
func (q *DisconnectQueue) pending(connection Connection) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, queued := q.inflight[connection.ID()]
	return queued || connection.Closing()
}

// fill returns the number of queued disconnects. The fill gauge reads it at each scrape.
func (q *DisconnectQueue) fill() int {
	return len(q.items)
}

// setRetrier sets the function which re-evaluates a key of a dropped disconnect. It is called by the queue goroutine.
func (q *DisconnectQueue) setRetrier(retrier func(key retryKey)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.retrier = retrier
}

// Run disconnects queued connections at the configured rate until ctx is done. A connection which is closing when it is
// dequeued is skipped without a rate token. While it runs, the fill gauge reads the queue.
func (q *DisconnectQueue) Run(ctx context.Context) error {
	metrics.BindAccessRevokeDisconnectQueue(q.fill)
	defer metrics.BindAccessRevokeDisconnectQueue(nil)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case connection := <-q.items:
			if !connection.Closing() {
				err := q.limiter.Wait(ctx)
				if err != nil {
					return err
				}
			}
			q.begin(connection)
		}
	}
}

// begin starts the close of a dequeued connection with the disconnect it is queued with, then removes it from the queued
// set and re-evaluates kept keys. Both happen under mu, so a terminal disconnect which arrives meanwhile is not lost. The
// connection leaves the set only after its close has begun, so it is never queued again while it is closing.
func (q *DisconnectQueue) begin(connection Connection) {
	q.mu.Lock()
	id := connection.ID()
	queued := q.inflight[id]
	delete(q.inflight, id)
	if len(q.inflight) == 0 {
		q.inflight = nil
	}
	if !connection.Closing() && connection.BeginDisconnect(queued.disconnect) {
		metrics.AccessRevokeDisconnectsTotal.WithLabelValues(string(queued.reason)).Inc()
	}
	q.releaseHeldLocked()
	q.mu.Unlock()
	q.retryKept()
}

// retryKept re-evaluates kept keys on the queue goroutine. It runs after each dequeue, and only while the queue is at most
// half full. It takes one key and runs its pass; when the pass left an item in the queue it stops, since that item's
// dequeue carries the next retry. While the queue is empty it takes the next key, so a key which queued nothing does not
// wait for an enqueue. A key comes back through a drop, which needs a full queue, or through the release of a held key at
// half, so a key which is held does not run while the queue is above half. The drain of an empty queue is not paced by
// the rate: its work is bounded by the kept keys, which the dropped disconnects bound.
func (q *DisconnectQueue) retryKept() {
	if len(q.items) > cap(q.items)/2 {
		return
	}
	for {
		q.mu.Lock()
		key, retrier, ok := q.takeRetryLocked()
		q.mu.Unlock()
		if !ok {
			return
		}
		retrier(key)
		if len(q.items) > 0 {
			return
		}
	}
}

// takeRetryLocked removes and returns one kept key with the retrier. It reports false when there is no key or no retrier.
// The set is released when its last key is taken, so a saturated queue does not keep its map for the life of the process.
func (q *DisconnectQueue) takeRetryLocked() (retryKey, func(retryKey), bool) {
	if q.retrier == nil {
		return retryKey{}, nil, false
	}
	for key := range q.retry {
		delete(q.retry, key)
		if len(q.retry) == 0 {
			q.retry = nil
		}
		return key, q.retrier, true
	}
	return retryKey{}, nil, false
}

// isTerminal reports whether the disconnect is in the terminal range, so the client must not reconnect.
func isTerminal(disconnect centrifuge.Disconnect) bool {
	return disconnect.Code >= disconnectTerminalCode
}
