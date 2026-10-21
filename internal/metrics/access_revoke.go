package metrics

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
