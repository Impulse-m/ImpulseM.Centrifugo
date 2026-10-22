package rpcregistry

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
