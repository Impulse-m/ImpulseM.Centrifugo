package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ChannelStateOldestFailingBatch holds the time, in Unix seconds, when the oldest channel state batch that
// the channel state proxy still rejects was first sent; 0 when no batch fails. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var ChannelStateOldestFailingBatch prometheus.Gauge
