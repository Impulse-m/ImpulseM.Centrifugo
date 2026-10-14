package rpcregistry

import (
	"context"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/service"
)

// Service returns a service which polls the registry file when poll_interval is set, and closes the
// connections of the current targets when it stops, at process shutdown. Connections of retired
// targets are closed by their drain timers.
func (r *Registry) Service() service.Service {
	return closeOnStop{registry: r}
}

// closeOnStop is the service returned by Service.
type closeOnStop struct {
	registry *Registry
}

// Run runs the file poll, if any, until ctx is done. Then it waits for the poll to stop, so no reload
// runs while the connections are closed.
func (s closeOnStop) Run(ctx context.Context) error {
	var polling sync.WaitGroup
	if interval := s.registry.pollInterval(); interval > 0 {
		polling.Add(1)
		go func() {
			defer polling.Done()
			s.registry.poll(ctx, interval)
		}()
	}
	<-ctx.Done()
	polling.Wait()
	closeTargets(s.registry.currentTargets())
	return nil
}

// currentTargets returns the targets of the published table.
func (r *Registry) currentTargets() []*Target {
	t := r.current.Load()
	targets := make([]*Target, 0, len(t.targets))
	for _, target := range t.targets {
		targets = append(targets, target)
	}
	return targets
}
