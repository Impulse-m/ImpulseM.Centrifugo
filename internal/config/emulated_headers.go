package config

import (
	"strings"
)

// EmulatedHeaderSet returns the lower-case union of the names of lists, as a set. It holds the emulated_headers of
// the proxies or of the registry targets which a connection's headers are kept for.
func EmulatedHeaderSet(lists ...[]string) map[string]struct{} {
	names := make(map[string]struct{})
	for _, list := range lists {
		for _, name := range list {
			names[strings.ToLower(name)] = struct{}{}
		}
	}
	return names
}
