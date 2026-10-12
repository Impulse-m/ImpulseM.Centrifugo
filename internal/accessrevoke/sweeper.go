package accessrevoke

import (
	"context"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

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
