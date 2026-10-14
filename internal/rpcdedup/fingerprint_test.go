package rpcdedup

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFingerprintDependsOnMethodAndData(t *testing.T) {
	require.Equal(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.get", []byte("x")))
	require.NotEqual(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.get", []byte("y")))
	require.NotEqual(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.set", []byte("x")))
	require.NotEqual(t, Fingerprint("a", []byte("bc")), Fingerprint("ab", []byte("c")))
}

// TestFingerprintIsLengthPrefixedSha256 pins the bytes hashed: the same length-prefixed encoding of the method and
// the data, which the streaming hash must keep.
func TestFingerprintIsLengthPrefixedSha256(t *testing.T) {
	data := []byte(`{"name":"user"}`)
	encoded := appendPartBytes(appendPartString(nil, "users.set"), data)
	require.Equal(t, sha256.Sum256(encoded), Fingerprint("users.set", data))
}

func TestFingerprintAllocs(t *testing.T) {
	small := []byte(`{"name":"user"}`)
	large := make([]byte, 64<<10)
	allocs := testing.AllocsPerRun(1000, func() {
		_ = Fingerprint("users.set", small)
	})
	require.Zero(t, allocs)
	allocs = testing.AllocsPerRun(100, func() {
		_ = Fingerprint("users.set", large)
	})
	require.Zero(t, allocs)
}

// BenchmarkFingerprintLarge measures a 64 KiB payload, which Fingerprint hashes without a copy.
func BenchmarkFingerprintLarge(b *testing.B) {
	data := make([]byte, 64<<10)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		_ = Fingerprint("users.set", data)
	}
}
