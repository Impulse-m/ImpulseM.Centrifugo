package push

import (
	"fmt"
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

func TestScrubberReplacesEveryTextWithOneReplacer(t *testing.T) {
	scrubber := NewScrubber([]string{"abc123", "def456"})

	require.Equal(t, "refused <token>", scrubber.Replace("refused abc123"))
	require.Equal(t, "status 503 from the provider", scrubber.Replace("status 503 from the provider"))
	require.Equal(t, "<token> and <token>", scrubber.Replace("def456 and abc123"))
}

func BenchmarkScrubberReplaceWithoutMatch(b *testing.B) {
	tokens := make([]string, 500)
	for index := range tokens {
		tokens[index] = fmt.Sprintf("%064x", index)
	}
	scrubber := NewScrubber(tokens)
	text := `{"code":"UNREGISTERED","message":"the registration is gone"}`

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		scrubber.Replace(text)
	}
}
