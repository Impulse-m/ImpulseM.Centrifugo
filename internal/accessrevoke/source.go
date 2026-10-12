package accessrevoke

import (
	"context"

	"github.com/centrifugal/centrifuge"
)

// ErrorNotReady is returned from connecting while the access revocation source has not caught up yet,
// or lags with reject_connect_on_lag. It is temporary, so clients retry the connection later.
var ErrorNotReady = &centrifuge.Error{
	Code:      1503,
	Message:   "not ready",
	Temporary: true,
}

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
