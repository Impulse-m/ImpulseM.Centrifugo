package accessrevoke

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
	"golang.org/x/time/rate"
)

// Connection is a local client connection which can be disconnected and indexed by its ID. Satisfied by
// *centrifuge.Client.
type Connection interface {
	ID() string
	Disconnect(disconnect ...centrifuge.Disconnect)
}

type disconnectItem struct {
	connection Connection
	disconnect centrifuge.Disconnect
	reason     Reason
}

// DisconnectQueue drains live disconnects caused by access revocation at a limited rate.
// The queue is bounded: disconnects enqueued while it is full are dropped and counted. The
// refresh check still rejects such connections within the token lifetime.
type DisconnectQueue struct {
	items   chan disconnectItem
	limiter *rate.Limiter
}

// NewDisconnectQueue creates a queue which disconnects at most ratePerSecond connections per second
// and holds at most size pending disconnects.
func NewDisconnectQueue(ratePerSecond int, size int) *DisconnectQueue {
	return &DisconnectQueue{
		items:   make(chan disconnectItem, size),
		limiter: rate.NewLimiter(rate.Limit(ratePerSecond), 1),
	}
}

// Enqueue adds a disconnect without blocking. It returns false when the queue is full, in which
// case the disconnect is dropped.
func (q *DisconnectQueue) Enqueue(connection Connection, disconnect centrifuge.Disconnect, reason Reason) bool {
	select {
	case q.items <- disconnectItem{connection: connection, disconnect: disconnect, reason: reason}:
		return true
	default:
		metrics.AccessRevokeDisconnectsDroppedTotal.Inc()
		return false
	}
}

// Run disconnects queued connections at the configured rate until ctx is done.
func (q *DisconnectQueue) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item := <-q.items:
			err := q.limiter.Wait(ctx)
			if err != nil {
				return err
			}
			item.connection.Disconnect(item.disconnect)
			metrics.AccessRevokeDisconnectsTotal.WithLabelValues(string(item.reason)).Inc()
		}
	}
}
