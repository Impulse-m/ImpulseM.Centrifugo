package jwtverify

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// jwksCacheRunFrame is the frame of the goroutine which cleans the TTL cache of a JWKS manager.
const jwksCacheRunFrame = "internal/jwks.(*TTLCache).run.func1"

// countJWKSCacheGoroutines returns how many goroutines run the cleanup of a JWKS manager cache.
func countJWKSCacheGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), jwksCacheRunFrame)
		}
		buf = make([]byte, 2*len(buf))
	}
}

func TestJWKSCacheGoroutineStopsOnReload(t *testing.T) {
	const reloads = 10
	config := VerifierConfig{JWKSPublicEndpoint: "https://example.com/.well-known/jwks.json"}

	base := countJWKSCacheGoroutines()
	verifier, err := NewTokenVerifierJWT(config, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return countJWKSCacheGoroutines() == base+1
	}, 5*time.Second, 10*time.Millisecond, "the new manager cache must run")

	for i := 0; i < reloads; i++ {
		require.NoError(t, verifier.Reload(config))
	}
	require.Eventually(t, func() bool {
		return countJWKSCacheGoroutines() == base+1
	}, 5*time.Second, 10*time.Millisecond, "only the current manager cache must run after reloads")
}

func TestJWKSCacheGoroutineStopsOnDiscardedReload(t *testing.T) {
	config := VerifierConfig{JWKSPublicEndpoint: "https://example.com/.well-known/jwks.json"}

	base := countJWKSCacheGoroutines()
	verifier, err := NewTokenVerifierJWT(config, nil)
	require.NoError(t, err)

	// A prepared config which is discarded must not leave its cache running, and must not touch the verifier.
	_, discard, err := verifier.PrepareReload(config)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return countJWKSCacheGoroutines() == base+2
	}, 5*time.Second, 10*time.Millisecond, "the prepared manager cache must run")
	discard()
	require.Eventually(t, func() bool {
		return countJWKSCacheGoroutines() == base+1
	}, 5*time.Second, 10*time.Millisecond, "the discarded manager cache must stop")

	// The verifier keeps its own manager, which is still running.
	apply, discardAfterApply, err := verifier.PrepareReload(config)
	require.NoError(t, err)
	apply()
	discardAfterApply()
	require.Eventually(t, func() bool {
		return countJWKSCacheGoroutines() == base+1
	}, 5*time.Second, 10*time.Millisecond, "discard after apply must not stop the applied manager cache")
}
