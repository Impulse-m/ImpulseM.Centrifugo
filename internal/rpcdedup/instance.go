package rpcdedup

import (
	"regexp"
)

// instanceRe matches a valid install instance: 16 to 64 characters of letters, digits, underscore and dash.
var instanceRe *regexp.Regexp = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

// ValidInstance reports whether instance is a valid install instance. The instance is the dedup scope of a
// connection without a token session. Any other value is not used as a scope.
func ValidInstance(instance string) bool {
	return instanceRe.MatchString(instance)
}
