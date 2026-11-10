package origin

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchOriginIsCaseInsensitive(t *testing.T) {
	checker, err := NewPatternChecker([]string{"https://*.Example.com"})
	require.NoError(t, err)

	require.True(t, checker.MatchOrigin("https://push.example.com"))
	require.True(t, checker.MatchOrigin("HTTPS://PUSH.EXAMPLE.COM"))
	require.False(t, checker.MatchOrigin("http://push.example.com"))
	require.False(t, checker.MatchOrigin("https://push.example.org"))
}

func TestMatchOriginStarAllowsAnyOrigin(t *testing.T) {
	checker, err := NewPatternChecker([]string{"*"})
	require.NoError(t, err)

	require.True(t, checker.MatchOrigin("https://anything.example:8443"))
}

func TestMatchOriginEmptyListMatchesNothing(t *testing.T) {
	checker, err := NewPatternChecker(nil)
	require.NoError(t, err)

	require.False(t, checker.MatchOrigin("https://push.example.com"))
}
