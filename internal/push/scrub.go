package push

import (
	"slices"
	"strings"
)

// tokenMarker is what Scrub writes in place of a token.
const tokenMarker string = "<token>"

// Scrub replaces every token of a batch which occurs in text with a marker. It is applied to every error string of a
// provider call before it is logged, measured or emitted, because a provider may echo the token it refused. Empty
// tokens are ignored, and a text without tokens is returned unchanged.
func Scrub(text string, tokens []string) string {
	present := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		present = append(present, token)
	}
	if len(present) == 0 {
		return text
	}
	// The longest token comes first: a replacer tries its pairs in argument order at each position, so a token which
	// is a prefix of another one must not be tried before it.
	slices.SortFunc(present, func(a string, b string) int {
		return len(b) - len(a)
	})
	pairs := make([]string, 0, 2*len(present))
	for _, token := range present {
		pairs = append(pairs, token, tokenMarker)
	}
	return strings.NewReplacer(pairs...).Replace(text)
}
