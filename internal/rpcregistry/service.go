package rpcregistry

import (
	"context"

	"github.com/centrifugal/centrifugo/v6/internal/service"
)

// Service returns a service which closes the connections of the current targets when it stops,
// at process shutdown. Connections of retired targets are closed by their drain timers.
func (r *Registry) Service() service.Service {
	return closeOnStop{registry: r}
}

// closeOnStop is the service returned by Service.
type closeOnStop struct {
	registry *Registry
}

// Run waits until ctx is done, then closes the connections of the current targets.
func (s closeOnStop) Run(ctx context.Context) error {
	<-ctx.Done()
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
