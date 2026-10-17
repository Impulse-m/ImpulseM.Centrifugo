package push

import (
	"slices"
	"strings"
)

// tokenMarker is what Scrubber writes in place of a token.
const tokenMarker string = "<token>"

// Scrubber replaces the device tokens of a batch which occur in a text with a marker. A batch builds one Scrubber and
// applies it to every error string of its provider calls before the string is logged, measured or emitted, because a
// provider may echo the token it refused. Only the bytes of a token are matched, so a caller passes each token in every
// encoding the provider may echo it in, such as the raw form and its JSON-escaped or URL-encoded form.
type Scrubber struct {
	tokens   []string
	replacer *strings.Replacer
}

// NewScrubber returns a Scrubber for tokens. Empty tokens are ignored. The replacer is built here, once per batch.
func NewScrubber(tokens []string) *Scrubber {
	present := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		present = append(present, token)
	}
	// The longest token comes first: a replacer tries its pairs in argument order at each position, so a token which is
	// a prefix of another one must not be tried before it.
	slices.SortFunc(present, func(a string, b string) int {
		return len(b) - len(a)
	})
	pairs := make([]string, 0, 2*len(present))
	for _, token := range present {
		pairs = append(pairs, token, tokenMarker)
	}
	var replacer *strings.Replacer
	if len(present) > 0 {
		replacer = strings.NewReplacer(pairs...)
	}
	return &Scrubber{
		tokens:   present,
		replacer: replacer,
	}
}

// Replace returns text with every token replaced by the marker. A text without a token is returned unchanged, and that
// common case does not allocate.
func (s *Scrubber) Replace(text string) string {
	if s.replacer == nil {
		return text
	}
	for _, token := range s.tokens {
		if strings.Contains(text, token) {
			return s.replacer.Replace(text)
		}
	}
	return text
}

// Scrub replaces every token of a batch which occurs in text with a marker. It builds a Scrubber for each call, so a
// batch which scrubs many texts should call NewScrubber once and Replace for each text. Empty tokens are ignored, and a
// text without tokens is returned unchanged.
func Scrub(text string, tokens []string) string {
	return NewScrubber(tokens).Replace(text)
}
