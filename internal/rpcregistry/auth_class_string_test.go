package rpcregistry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthClassStringRoundTrips(t *testing.T) {
	for _, class := range []AuthClass{AuthClassAnonymous, AuthClassPending2FA, AuthClassAuthenticated, AuthClassBot} {
		parsed, ok := ParseAuthClass(class.String())
		require.True(t, ok)
		require.Equal(t, class, parsed)
	}
	require.Empty(t, AuthClass(0).String())
}
