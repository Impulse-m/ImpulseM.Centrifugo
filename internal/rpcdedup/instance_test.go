package rpcdedup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidInstanceLength(t *testing.T) {
	require.False(t, ValidInstance(strings.Repeat("a", 21)))
	require.True(t, ValidInstance(strings.Repeat("a", 22)))
	require.True(t, ValidInstance(strings.Repeat("a", 64)))
	require.False(t, ValidInstance(strings.Repeat("a", 65)))
}

func TestValidInstanceCharacters(t *testing.T) {
	require.True(t, ValidInstance("AZaz09_-AZaz09_-AZaz09_-"))
	require.False(t, ValidInstance("0123456789abcdef0123:5"))
	require.False(t, ValidInstance("0123456789abcdef012345 "))
	require.False(t, ValidInstance(""))
}
