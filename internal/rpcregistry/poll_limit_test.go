package rpcregistry

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// oversizedRegistryContent is a valid registry file of user.get and user.list, padded past 16 MiB by a comment line.
// The padding keeps the file valid, so only a size limit can refuse it.
func oversizedRegistryContent() string {
	return registryContent("user.get", "user.list") + "# " + strings.Repeat("x", 16<<20) + "\n"
}

// TestPollRefusesOversizedRegistryFile verifies that a poll with poll_max_file_size does not read a registry file over
// the limit, and keeps the current registry.
func TestPollRefusesOversizedRegistryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	cfg := pollingRegistry(path, time.Hour)
	cfg.PollMaxFileSize = 16 << 20
	reloadWith(t, r, cfg)
	before := r.Lookup("user.get")
	require.NotNil(t, before)

	replaceRegistryFile(t, path, oversizedRegistryContent())

	p := &poller{registry: r}
	p.check()

	require.Nil(t, r.Lookup("user.list"), "an oversized registry file must not be applied")
	require.Same(t, before, r.Lookup("user.get"))
}

// TestPollWithoutSizeLimitAppliesLargeFile verifies that a poll with no poll_max_file_size, the default, reads a
// registry file of any size, as a poll did before the limit existed.
func TestPollWithoutSizeLimitAppliesLargeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, time.Hour))

	replaceRegistryFile(t, path, oversizedRegistryContent())

	p := &poller{registry: r}
	p.check()

	require.NotNil(t, r.Lookup("user.list"), "a registry file over 16 MiB is applied when no limit is set")
}
