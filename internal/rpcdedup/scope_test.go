package rpcdedup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A kind which ScopeOf does not build, and a user with the anonymous kind, have no scope: nothing is tracked under a
// scope which scopeUser and scopeAnonymous cannot read.
func TestScopeOfRejectsOtherKinds(t *testing.T) {
	id := []byte("install_0123456789abcdef")
	require.Empty(t, ScopeOf("xy", "42", id))
	require.Empty(t, ScopeOf("x", "42", id))
	require.Empty(t, ScopeOf("", "42", id))
	require.Empty(t, ScopeOf("a", "42", id))
	require.NotEmpty(t, ScopeOf("a", "", id))
	require.NotEmpty(t, ScopeOf("s", "42", id))
	require.NotEmpty(t, ScopeOf("i", "42", id))
}

func TestScopeOfIsBoundToUserAndID(t *testing.T) {
	id := []byte("install_0123456789abcdef")
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

// The scope of an authenticated user has the shape kind:usertag:hash, and the anonymous one kind:hash.
func TestScopeOfShape(t *testing.T) {
	scope := ScopeOf("s", "42", []byte("session"))
	require.Len(t, scope, authenticatedScopeLen)
	require.True(t, strings.HasPrefix(scope, "s:"))
	require.Equal(t, scopeUser(scope), scope[2:18])
	require.False(t, scopeAnonymous(scope))

	anonymous := ScopeOf(anonymousKind, "", []byte("install_0123456789abcdef"))
	require.Len(t, anonymous, anonymousScopeLen)
	require.True(t, strings.HasPrefix(anonymous, "a:"))
	require.Empty(t, scopeUser(anonymous))
	require.True(t, scopeAnonymous(anonymous))
}

// The user tag of a scope is the same for all scopes of one user, and differs between users.
func TestScopeOfUserTagIsPerUser(t *testing.T) {
	first := ScopeOf("s", "42", []byte("a"))
	second := ScopeOf("i", "42", []byte("b"))
	other := ScopeOf("s", "43", []byte("a"))
	require.Equal(t, scopeUser(first), scopeUser(second))
	require.NotEqual(t, scopeUser(first), scopeUser(other))
}

func TestScopeUserAndAnonymousShapes(t *testing.T) {
	authenticated := ScopeOf("i", "42", []byte("install"))
	anonymous := ScopeOf(anonymousKind, "", []byte("install"))

	require.Equal(t, authenticated[2:18], scopeUser(authenticated))
	require.False(t, scopeAnonymous(authenticated))
	require.Empty(t, scopeUser(anonymous))
	require.True(t, scopeAnonymous(anonymous))

	// A malformed scope has no user, and is not anonymous.
	for _, malformed := range []string{
		"",
		"s",
		"s:0123456789abcdef",
		authenticated[:len(authenticated)-1],
		authenticated + "0",
		"x" + authenticated[1:],
		strings.ToUpper(authenticated),
		authenticated[:2] + strings.Repeat("g", 16) + authenticated[18:],
		anonymous[:len(anonymous)-1],
		"b" + anonymous[1:],
	} {
		require.Empty(t, scopeUser(malformed), malformed)
		require.False(t, scopeAnonymous(malformed), malformed)
	}
}

// Hashing a short scope or fingerprint costs one allocation: the scope string itself.
func TestScopeOfAllocs(t *testing.T) {
	id := []byte("install_0123456789abcdef")
	allocs := testing.AllocsPerRun(1000, func() {
		_ = ScopeOf("i", "42", id)
	})
	require.LessOrEqual(t, allocs, 1.0)
}
