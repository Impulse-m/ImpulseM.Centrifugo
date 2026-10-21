package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AccessRevokeStoreSizes is the number of entries of the access revocation store by kind.
type AccessRevokeStoreSizes struct {
	Blocked  int
	Sessions int
	Users    int
}

// accessRevokeStoreSizes is the reader of the store sizes, bound by the Sweeper while it runs.
var accessRevokeStoreSizes boundReader[AccessRevokeStoreSizes]

// BindAccessRevokeStoreSizes makes the store size gauges read the sizes by read at each scrape. A nil read unbinds
// them, and unbound gauges read 0.
func BindAccessRevokeStoreSizes(read func() AccessRevokeStoreSizes) {
	accessRevokeStoreSizes.Bind(read)
}

// AccessRevokeSourceState is the readiness of the access revocation source.
type AccessRevokeSourceState struct {
	Ready      bool
	Lagging    bool
	LagRecords int64
}

// accessRevokeSource is the reader of the source state, bound by the source while it runs.
var accessRevokeSource boundReader[AccessRevokeSourceState]

// BindAccessRevokeSource makes the source gauges read the state by read at each scrape. A nil read unbinds them, and
// unbound gauges read 0.
func BindAccessRevokeSource(read func() AccessRevokeSourceState) {
	accessRevokeSource.Bind(read)
}

// boolToFloat returns 1 for true and 0 for false, as a gauge value.
func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// withLabel returns a copy of labels, which may be nil, with one more label pair.
func withLabel(labels prometheus.Labels, name string, value string) prometheus.Labels {
	copied := make(prometheus.Labels, len(labels)+1)
	for key, labelValue := range labels {
		copied[key] = labelValue
	}
	copied[name] = value
	return copied
}
