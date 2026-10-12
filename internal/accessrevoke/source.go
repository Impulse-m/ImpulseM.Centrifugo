package accessrevoke

import (
	"context"
)

// Source delivers access revocation events of the cluster to this node. The source applies events
// locally with a Revoker, and the node can use its readiness to gate new connections.
type Source interface {
	// Run reads events until ctx is done. It is a service.Service.
	Run(ctx context.Context) error
	// Ready is true once the source caught up with the log after start, or after the startup timeout.
	Ready() bool
	// Lagging is true when the source is behind the log for longer than the lag threshold.
	Lagging() bool
}
