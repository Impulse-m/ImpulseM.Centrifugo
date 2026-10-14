package rpcdedup

import (
	"regexp"
)

// instanceRe matches a valid install instance: 22 to 64 characters of letters, digits, underscore and dash. 22
// characters of base64url carry 128 bits, the least for a secret which is a bearer token of its scope.
var instanceRe *regexp.Regexp = regexp.MustCompile(`^[A-Za-z0-9_-]{22,64}$`)

// ValidInstance reports whether instance is a valid install instance. The instance is the dedup scope of a
// connection without a token session. Any other value is not used as a scope.
func ValidInstance(instance string) bool {
	return instanceRe.MatchString(instance)
}
