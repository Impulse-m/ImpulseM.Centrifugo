package fcm

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestInjectionBoundCoversInjection checks that the pre-copy bound of injectMessage is never below the growth that the
// injection makes, for each addition and for tokens that the JSON encoder escapes.
func TestInjectionBoundCoversInjection(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	expireAt := now.Unix() + 3600

	testCases := []struct {
		name       string
		message    string
		token      string
		collapseID string
		expireAt   int64
	}{
		{name: "token only", message: `{"data":{"x":"1"}}`, token: "device-token"},
		{name: "collapse id", message: `{"data":{"x":"1"}}`, token: "device-token", collapseID: "collapse"},
		{name: "expiry", message: `{"data":{"x":"1"}}`, token: "device-token", expireAt: expireAt},
		{name: "everything", message: `{"android":{"priority":"high"}}`, token: "t", collapseID: "c", expireAt: expireAt},
		{name: "escaped token", message: `{}`, token: "a\"b\\c\n<d>&", collapseID: "x\"y", expireAt: expireAt},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			injected, err := injectMessage([]byte(testCase.message), testCase.token, testCase.collapseID, testCase.expireAt, now)
			require.NoError(t, err)

			bound := injectionBound(testCase.token, testCase.collapseID, testCase.expireAt)
			require.LessOrEqual(t, len(injected), len(testCase.message)+bound)
		})
	}
}

// TestFitsInjectedRefusesOversizedMessage checks the pre-copy check: a message over the limit once the token is added
// is refused, and a message that fits is accepted.
func TestFitsInjectedRefusesOversizedMessage(t *testing.T) {
	const token = "device-token"

	// The token alone adds an exact number of bytes, so the boundary is exact.
	exactFit := maxMessageBytes - injectionBound(token, "", 0)
	require.True(t, fitsInjected([]byte(strings.Repeat("a", exactFit)), token, "", 0))
	require.False(t, fitsInjected([]byte(strings.Repeat("a", exactFit+1)), token, "", 0))
	require.False(t, fitsInjected([]byte(strings.Repeat("a", maxMessageBytes+1)), "", "", 0))
}
