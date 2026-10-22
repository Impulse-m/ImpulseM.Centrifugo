package rpcregistry

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPollDropsSnapshotOvertakenByReload verifies that a poll does not apply a file it read before a reload
// which was applied while the read was in flight. The reload is newer than the snapshot, so the snapshot is
// dropped and the next tick decides.
func TestPollDropsSnapshotOvertakenByReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, time.Hour))

	r.reloadMu.Lock()
	read := r.source
	r.reloadMu.Unlock()

	// The poll reads the content which has the list method.
	replaceRegistryFile(t, path, registryContent("user.list"))
	snap, err := snapshotFile(path, 0)
	require.NoError(t, err)

	// A reload applies newer content while the poll's read is in flight.
	replaceRegistryFile(t, path, registryContent("user.hash"))
	reloadWith(t, r, pollingRegistry(path, time.Hour))

	p := &poller{registry: r}
	p.apply(path, read, snap)

	require.Nil(t, r.Lookup("user.list"), "a snapshot which a reload overtook must not be applied")
	require.NotNil(t, r.Lookup("user.hash"), "the reload stays")
}
