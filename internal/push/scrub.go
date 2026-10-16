package push

import (
	"strings"
)

// tokenMarker is what Scrub writes in place of a token.
const tokenMarker string = "<token>"

// Scrub replaces every token of a batch which occurs in text with a marker. It is applied to every error string of a
// provider call before it is logged, measured or emitted, because a provider may echo the token it refused. Empty
// tokens are ignored, and a text without tokens is returned unchanged.
func Scrub(text string, tokens []string) string {
	pairs := make([]string, 0, 2*len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		pairs = append(pairs, token, tokenMarker)
	}
	if len(pairs) == 0 {
		return text
	}
	return strings.NewReplacer(pairs...).Replace(text)
}
