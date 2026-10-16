package push

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScrubReplacesEveryToken(t *testing.T) {
	tokens := []string{"abc123", "def456"}
	text := "device abc123 refused, then abc123 again and def456 too"

	require.Equal(t, "device <token> refused, then <token> again and <token> too", Scrub(text, tokens))
}

func TestScrubReplacesALongerTokenWhichHasAnotherTokenAsPrefix(t *testing.T) {
	text := "refused abcdef and abc"

	require.Equal(t, "refused <token> and <token>", Scrub(text, []string{"abc", "abcdef"}))
	require.Equal(t, "refused <token> and <token>", Scrub(text, []string{"abcdef", "abc"}))
}

func TestScrubLeavesOtherTextUnchanged(t *testing.T) {
	text := `{"code":"UNREGISTERED","message":"the registration is gone"}`

	require.Equal(t, text, Scrub(text, []string{"abc123"}))
}

func TestScrubIgnoresEmptyTokens(t *testing.T) {
	text := "status 503 from the provider"

	require.Equal(t, text, Scrub(text, []string{"", ""}))
	require.Equal(t, "status 503 from the <token>", Scrub("status 503 from the provider", []string{"", "provider"}))
}

func TestScrubWithoutTokensReturnsTheText(t *testing.T) {
	require.Equal(t, "", Scrub("", nil))
	require.Equal(t, "plain", Scrub("plain", []string{}))
}
