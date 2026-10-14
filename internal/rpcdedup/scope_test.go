package rpcdedup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopeOfIsBoundToUserAndID(t *testing.T) {
	var id []byte = []byte("install_0123456789abcdef")
	require.NotEqual(t, ScopeOf("i", "42", id), ScopeOf("i", "43", id))
	require.NotEqual(t, ScopeOf("i", "42", id), ScopeOf("s", "42", id))
	require.NotEqual(t, ScopeOf("i", "42", id), ScopeOf("i", "", id))
	require.Equal(t, ScopeOf("i", "42", id), ScopeOf("i", "42", id))
}

// A user id which contains the separator must not make two different (user, id) pairs equal.
func TestScopeOfIsNotForgeableByUserID(t *testing.T) {
	require.NotEqual(t, ScopeOf("i", "a:b", []byte("c")), ScopeOf("i", "a", []byte("b:c")))
	require.NotEqual(t, ScopeOf("i", "a", []byte("bc")), ScopeOf("i", "ab", []byte("c")))
}

func TestFingerprintDependsOnMethodAndData(t *testing.T) {
	require.Equal(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.get", []byte("x")))
	require.NotEqual(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.get", []byte("y")))
	require.NotEqual(t, Fingerprint("users.get", []byte("x")), Fingerprint("users.set", []byte("x")))
	require.NotEqual(t, Fingerprint("a", []byte("bc")), Fingerprint("ab", []byte("c")))
}
