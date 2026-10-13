package accessrevoke

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/rs/zerolog/log"
)

// SweepConnections checks every local connection against the store and enqueues a disconnect for each
// rejected one. It returns the number of rejected connections. Disconnects dropped by a full queue are
// counted by the queue and still rejected by the refresh check within the token lifetime.
func SweepConnections(hub Hub, store *Store, queue *DisconnectQueue, opts CheckOptions) int {
	var rejected int
	for _, client := range hub.Connections() {
		disconnect, reason, revoked := CheckConnection(store, client, opts, stageSweep)
		if !revoked {
			continue
		}
		rejected++
		queue.Enqueue(client, disconnect, reason)
	}
	return rejected
}

// CatchUpSweeper runs one SweepConnections after the source caught up following a fail-open start.
// Connections accepted while the source was still behind may hold tokens which were revoked in the
// meantime, and they are not checked by the connect path.
type CatchUpSweeper struct {
	caughtUp <-chan struct{}
	hub      Hub
	store    *Store
	queue    *DisconnectQueue
	options  func() CheckOptions
}

// NewCatchUpSweeper creates a CatchUpSweeper. The caughtUp channel is closed by the source once it
// catches up after fail-open. The options function is called at sweep time, so config reloads apply.
func NewCatchUpSweeper(
	caughtUp <-chan struct{},
	hub Hub,
	store *Store,
	queue *DisconnectQueue,
	options func() CheckOptions,
) *CatchUpSweeper {
	return &CatchUpSweeper{
		caughtUp: caughtUp,
		hub:      hub,
		store:    store,
		queue:    queue,
		options:  options,
	}
}

// Run waits for the caught up signal and sweeps once. It returns when the sweep is done, or with
// ctx error if ctx is done before that.
func (s *CatchUpSweeper) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.caughtUp:
	}
	rejected := SweepConnections(s.hub, s.store, s.queue, s.options())
	log.Info().Int("rejected", rejected).Msg("access revocation catch-up sweep done")
	return nil
}

// Sweeper periodically removes expired entries from the Store and updates store size metrics.
type Sweeper struct {
	store    *Store
	interval time.Duration
}

// NewSweeper creates a Sweeper which runs every interval.
func NewSweeper(store *Store, interval time.Duration) *Sweeper {
	return &Sweeper{
		store:    store,
		interval: interval,
	}
}

// Run sweeps the Store every interval until ctx is done.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.store.Sweep()
			sizes := s.store.Sizes()
			metrics.AccessRevokeStoreSize.WithLabelValues("blocked").Set(float64(sizes.Blocked))
			metrics.AccessRevokeStoreSize.WithLabelValues("session").Set(float64(sizes.Sessions))
			metrics.AccessRevokeStoreSize.WithLabelValues("user").Set(float64(sizes.Users))
		}
	}
}
