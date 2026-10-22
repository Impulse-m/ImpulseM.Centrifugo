package client

import (
	"strings"
)

// keepEmulatedHeaders returns a copy of the connect headers whose names are listed in the emulated_headers of a proxy,
// as proxyNames holds them in lower case, or which allowed reports for a registry target. It returns nil when none is.
func keepEmulatedHeaders(
	headers map[string]string,
	proxyNames map[string]struct{},
	allowed func(name string) bool,
) map[string]string {
	var kept map[string]string
	for name, value := range headers {
		lower := strings.ToLower(name)
		_, inProxy := proxyNames[lower]
		if !inProxy && !allowed(lower) {
			continue
		}
		if kept == nil {
			kept = make(map[string]string)
		}
		kept[name] = value
	}
	return kept
}
