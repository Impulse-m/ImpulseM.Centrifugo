package jwks

// Stop stops the background work of the manager's cache, which is the cleanup of expired keys. Call it when
// the manager is discarded, for example after a config reload replaces it. The manager stays usable: lookups
// still work, the expired keys are only no longer removed. Stop is idempotent.
func (m *Manager) Stop() error {
	return m.cache.Stop()
}
