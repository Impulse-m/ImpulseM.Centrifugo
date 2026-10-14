package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFingerprintDependsOnMethodAndData(t *testing.T) {
	require.Equal(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.get", []byte("x")))
	require.NotEqual(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.get", []byte("y")))
	require.NotEqual(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.set", []byte("x")))
	require.NotEqual(t, Fingerprint("a", []byte("bc")), Fingerprint("ab", []byte("c")))
}

func TestFingerprintAllocs(t *testing.T) {
	data := []byte(`{"name":"user"}`)
	allocs := testing.AllocsPerRun(1000, func() {
		_ = Fingerprint("users.set", data)
	})
	require.Zero(t, allocs)
}
