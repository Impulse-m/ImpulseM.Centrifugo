package rpcregistry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// registryContent returns a registry file with one target and the given methods.
func registryContent(methods ...string) string {
	var b strings.Builder
	b.WriteString("version: 1\ntargets:\n  - name: users\n    endpoint: dns:///users.example:9000\nmethods:\n")
	for _, method := range methods {
		b.WriteString("  - method: " + method + "\n")
		b.WriteString("    grpc_method: /example.v1.UserService/Get\n")
		b.WriteString("    target: users\n")
		b.WriteString("    auth_class: anonymous\n")
	}
	return b.String()
}

// replaceRegistryFile replaces the registry file by a rename, as a ConfigMap update does, so the
// poller never reads a file which is only partly written. On Windows the rename fails while the
// poller has the file open for a read, so it is retried until it succeeds.
func replaceRegistryFile(t *testing.T, path string, content string) {
	t.Helper()
	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, []byte(content), 0o600))
	require.Eventually(t, func() bool {
		return os.Rename(tmp, path) == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// pollingRegistry returns an enabled registry config which reads path and checks it every interval.
func pollingRegistry(path string, interval time.Duration) configtypes.RPCRegistry {
	return configtypes.RPCRegistry{
		Enabled:      true,
		File:         path,
		PollInterval: configtypes.Duration(interval),
	}
}

// useReloadCounter replaces the reloads counter for one test. It must be called before New, which
// resolves the counter children.
func useReloadCounter(t *testing.T) *prometheus.CounterVec {
	t.Helper()
	previous := metrics.RPCRegistryReloadsTotal
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_registry_reloads_total",
	}, []string{"trigger", "result"})
	metrics.RPCRegistryReloadsTotal = counter
	t.Cleanup(func() {
		metrics.RPCRegistryReloadsTotal = previous
	})
	return counter
}

// reloadCount returns the value of one reloads counter child.
func reloadCount(counter *prometheus.CounterVec, trigger string, result string) float64 {
	return testutil.ToFloat64(counter.WithLabelValues(trigger, result))
}

// runService runs the service of r until the test ends, and waits for it to stop.
func runService(t *testing.T, r *Registry) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.Service().Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

// registryFileMethod returns the name of the method which the registry of version i has.
func registryFileMethod(i int) string {
	return fmt.Sprintf("user.v%d", i)
}

func TestPollerAppliesRewrittenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, 10*time.Millisecond))
	runService(t, r)
	require.NotNil(t, r.Lookup("user.get"))

	replaceRegistryFile(t, path, registryContent("user.get", "user.list"))
	require.Eventually(t, func() bool {
		return r.Lookup("user.list") != nil
	}, 5*time.Second, 10*time.Millisecond)
	require.NotNil(t, r.Lookup("user.get"))
}

func TestPollerIgnoresTouchWithSameContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	counter := useReloadCounter(t)
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, 5*time.Millisecond))
	runService(t, r)

	touched := time.Now().Add(time.Hour)
	require.NoError(t, os.Chtimes(path, touched, touched))
	// The poller re-baselines the stat of the file, and does not reload it.
	require.Eventually(t, func() bool {
		r.reloadMu.Lock()
		defer r.reloadMu.Unlock()
		return r.source.modTime.Unix() == touched.Unix()
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, float64(0), reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultOK))
	require.Equal(t, float64(0), reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultError))
	require.NotNil(t, r.Lookup("user.get"))
}

func TestPollerKeepsOldTableOnInvalidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	counter := useReloadCounter(t)
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, 5*time.Millisecond))
	runService(t, r)

	// The method refers to a target which is not in the file.
	replaceRegistryFile(t, path, "version: 1\nmethods:\n  - method: user.get\n    grpc_method: /example.v1.UserService/Get\n    target: missing\n    auth_class: anonymous\n")
	require.Eventually(t, func() bool {
		return reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultError) == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.NotNil(t, r.Lookup("user.get"))

	// The same broken content is not loaded and reported again.
	require.Never(t, func() bool {
		return reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultError) > 1
	}, 100*time.Millisecond, 5*time.Millisecond)

	replaceRegistryFile(t, path, registryContent("user.get", "user.list"))
	require.Eventually(t, func() bool {
		return r.Lookup("user.list") != nil
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, float64(1), reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultError))
	require.Equal(t, float64(1), reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultOK))
}

func TestTransientDialFailureIsRetriedWithoutContentChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	counter := useReloadCounter(t)
	var failing atomic.Bool
	r := New()
	r.dial = func(target *Target) (io.Closer, error) {
		if target.Config.Name == "catalog" && failing.Swap(false) {
			return nil, errors.New("dial refused")
		}
		return &fakeConn{}, nil
	}
	reloadWith(t, r, pollingRegistry(path, 5*time.Millisecond))
	runService(t, r)

	// The new file adds a target, whose first dial fails. The content does not change after that.
	failing.Store(true)
	replaceRegistryFile(t, path, "version: 1\n"+
		"targets:\n"+
		"  - name: users\n"+
		"    endpoint: dns:///users.example:9000\n"+
		"  - name: catalog\n"+
		"    endpoint: dns:///catalog.example:9000\n"+
		"methods:\n"+
		"  - method: user.get\n"+
		"    grpc_method: /example.v1.UserService/Get\n"+
		"    target: users\n"+
		"    auth_class: anonymous\n"+
		"  - method: catalog.get\n"+
		"    grpc_method: /example.v1.CatalogService/Get\n"+
		"    target: catalog\n"+
		"    auth_class: anonymous\n")

	require.Eventually(t, func() bool {
		return r.Lookup("catalog.get") != nil
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, float64(1), reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultError))
	require.Equal(t, float64(1), reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultOK))
}

func TestPollIntervalZeroStartsNoPoller(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, 0))
	require.Equal(t, time.Duration(0), r.pollInterval())
	runService(t, r)

	replaceRegistryFile(t, path, registryContent("user.get", "user.list"))
	require.Never(t, func() bool {
		return r.Lookup("user.list") != nil
	}, 200*time.Millisecond, 10*time.Millisecond)
}

func TestPollerDoesNotReloadContentAppliedBySignal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	counter := useReloadCounter(t)
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, 5*time.Millisecond))

	// A signal reload applies the rewritten file. The poller takes that content as its baseline.
	replaceRegistryFile(t, path, registryContent("user.get", "user.list"))
	apply, _, err := r.PrepareReload(pollingRegistry(path, 5*time.Millisecond))
	require.NoError(t, err)
	apply()
	runService(t, r)

	require.Never(t, func() bool {
		return reloadCount(counter, metrics.RPCRegistryReloadTriggerPoll, metrics.RPCRegistryResultOK) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
	require.Equal(t, float64(1), reloadCount(counter, metrics.RPCRegistryReloadTriggerSignal, metrics.RPCRegistryResultOK))
	require.NotNil(t, r.Lookup("user.list"))
}

func TestSignalReloadRacingWithPollerEndsOnLastContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	replaceRegistryFile(t, path, registryContent("user.get"))
	r := newRegistryWithDialer(t, &fakeDialer{})
	reloadWith(t, r, pollingRegistry(path, time.Millisecond))
	runService(t, r)

	// Each signal reload reads the file under the reload lock, as the poller does, so the last
	// reload applies the last content, and the poller can only apply that content after it.
	const versions = 20
	for i := 0; i < versions; i++ {
		replaceRegistryFile(t, path, registryContent(registryFileMethod(i)))
		apply, _, err := r.PrepareReload(pollingRegistry(path, time.Millisecond))
		require.NoError(t, err)
		apply()
	}

	require.NotNil(t, r.Lookup(registryFileMethod(versions-1)))
	require.Nil(t, r.Lookup(registryFileMethod(versions-2)))
	require.Nil(t, r.Lookup("user.get"))
}
