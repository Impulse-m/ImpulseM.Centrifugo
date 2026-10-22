package rpcregistry

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPollRefusesOversizedRegistryFile verifies that a poll does not read a registry file over the poll
// size limit, and keeps the current registry. The file is valid, so only the limit refuses it.
func TestPollRefusesOversizedRegistryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, time.Hour))
	before := r.Lookup("user.get")
	require.NotNil(t, before)

	// A comment line padded past 16 MiB keeps the file valid, and a valid file would be applied.
	oversized := registryContent("user.get", "user.list") + "# " + strings.Repeat("x", 16<<20) + "\n"
	replaceRegistryFile(t, path, oversized)

	p := &poller{registry: r}
	p.check()

	require.Nil(t, r.Lookup("user.list"), "an oversized registry file must not be applied")
	require.Same(t, before, r.Lookup("user.get"))
}

// TestSnapshotFileReadStopsAtLimit verifies that a read with a limit refuses a file which is over the limit when
// it is read, even if its stat was taken while it was smaller, and reads a file within the limit whole.
func TestSnapshotFileReadStopsAtLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, strings.Repeat("x", 64))

	_, err := snapshotFile(path, 63)
	require.Error(t, err, "a file over the limit must be refused")

	snap, err := snapshotFile(path, 64)
	require.NoError(t, err)
	require.Len(t, snap.data, 64)
}
