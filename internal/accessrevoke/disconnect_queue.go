package accessrevoke

import (
	"context"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
	"golang.org/x/time/rate"
)

// Connection is a local client connection which can be disconnected and indexed by its ID. Satisfied by
// *centrifuge.Client. BeginDisconnect starts the close and Closing reports whether a close has begun, so the queue never
// disconnects a connection twice and never spends a rate token on one which is closing already.
type Connection interface {
	ID() string
	BeginDisconnect(disconnect centrifuge.Disconnect) bool
	Closing() bool
}

var _ Connection = (*centrifuge.Client)(nil)

type disconnectItem struct {
	connection Connection
	disconnect centrifuge.Disconnect
	reason     Reason
}

// retryKey names what a dropped disconnect was for: the connections of a user (user set), or the indexed connections of
// a session without a user (user empty). A retry re-reads the store, so the key carries no reason and no disconnect.
type retryKey struct {
	user    string
	session [16]byte
}

// DisconnectQueue drains live disconnects caused by access revocation at a limited rate. The queue is bounded and holds
// each connection at most once. It never queues or disconnects a connection whose close has begun. A disconnect which
// finds the queue full is dropped and counted, and its key is kept. The queue goroutine re-evaluates kept keys through
// the store once the queue is at most half full: one key per drained item while the queue holds items, and the next key
// in a row while it is empty. The refresh check rejects a connection within the token lifetime in any case.
type DisconnectQueue struct {
	items   chan disconnectItem
	limiter *rate.Limiter

	mu       sync.Mutex
	inflight map[string]struct{}
	retry    map[retryKey]struct{}
	retrier  func(key retryKey)
}

// NewDisconnectQueue creates a queue which disconnects at most ratePerSecond connections per second
// and holds at most size pending disconnects.
func NewDisconnectQueue(ratePerSecond int, size int) *DisconnectQueue {
	return &DisconnectQueue{
		items:   make(chan disconnectItem, size),
		limiter: rate.NewLimiter(rate.Limit(ratePerSecond), 1),
	}
}

// Enqueue adds a disconnect without blocking. A connection which is closing or already queued is not queued again, and
// the call returns true. It returns false when the queue is full, in which case the disconnect is dropped.
func (q *DisconnectQueue) Enqueue(connection Connection, disconnect centrifuge.Disconnect, reason Reason) bool {
	return q.enqueue(connection, disconnect, reason, nil)
}

// enqueueKeyed is Enqueue for a disconnect of key. A drop records key for a retry in the same critical section as the full
// queue check, so the queue goroutine cannot drain the queue between the drop and the record.
func (q *DisconnectQueue) enqueueKeyed(
	key retryKey,
	connection Connection,
	disconnect centrifuge.Disconnect,
	reason Reason,
) bool {
	return q.enqueue(connection, disconnect, reason, &key)
}

// enqueue adds the disconnect without blocking and records key, when it is not nil, if the queue is full.
func (q *DisconnectQueue) enqueue(
	connection Connection,
	disconnect centrifuge.Disconnect,
	reason Reason,
	key *retryKey,
) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if connection.Closing() {
		return true
	}
	id := connection.ID()
	_, queued := q.inflight[id]
	if queued {
		return true
	}
	select {
	case q.items <- disconnectItem{connection: connection, disconnect: disconnect, reason: reason}:
		if q.inflight == nil {
			q.inflight = make(map[string]struct{})
		}
		q.inflight[id] = struct{}{}
		return true
	default:
	}
	metrics.AccessRevokeDisconnectsDroppedTotal.Inc()
	if key != nil {
		if q.retry == nil {
			q.retry = make(map[retryKey]struct{})
		}
		q.retry[*key] = struct{}{}
	}
	return false
}

// setRetrier sets the function which re-evaluates a key of a dropped disconnect. It is called by the queue goroutine.
func (q *DisconnectQueue) setRetrier(retrier func(key retryKey)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.retrier = retrier
}

// Run disconnects queued connections at the configured rate until ctx is done. A connection which is closing when it is
// dequeued is skipped without a rate token.
func (q *DisconnectQueue) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item := <-q.items:
			if !item.connection.Closing() {
				err := q.limiter.Wait(ctx)
				if err != nil {
					return err
				}
				if item.connection.BeginDisconnect(item.disconnect) {
					metrics.AccessRevokeDisconnectsTotal.WithLabelValues(string(item.reason)).Inc()
				}
			}
			q.finish(item.connection.ID())
		}
	}
}

// finish removes a dequeued connection from the queued set, then re-evaluates kept keys. The connection leaves the set
// only after its close has begun, so it is never queued again while it is closing.
func (q *DisconnectQueue) finish(id string) {
	q.mu.Lock()
	delete(q.inflight, id)
	q.mu.Unlock()
	q.retryKept()
}

// retryKept re-evaluates kept keys on the queue goroutine. It runs after each dequeue, and only while the queue is at most
// half full. It takes one key and runs its pass; when the pass left an item in the queue it stops, since that item's
// dequeue carries the next retry. While the queue is empty it takes the next key, so a key which queued nothing does not
// wait for an enqueue. A key comes back only through a drop, which needs a full queue, and a full queue stops the loop.
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
func (q *DisconnectQueue) takeRetryLocked() (retryKey, func(retryKey), bool) {
	if q.retrier == nil {
		return retryKey{}, nil, false
	}
	for key := range q.retry {
		delete(q.retry, key)
		return key, q.retrier, true
	}
	return retryKey{}, nil, false
}
