package accessrevoke

import (
	"context"

	"github.com/rs/zerolog/log"
)

// stageSweep is the rejects metric stage of the catch-up sweep.
const stageSweep = "sweep"

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
