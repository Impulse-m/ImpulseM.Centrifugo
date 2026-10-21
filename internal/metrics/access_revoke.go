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

// withLabel returns a copy of labels, which may be nil, with one more label pair.
func withLabel(labels prometheus.Labels, name string, value string) prometheus.Labels {
	copied := make(prometheus.Labels, len(labels)+1)
	for key, labelValue := range labels {
		copied[key] = labelValue
	}
	copied[name] = value
	return copied
}
