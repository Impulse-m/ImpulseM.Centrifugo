package client

import (
	"context"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/clientcontext"
)

// setConnectEmulatedHeaders keeps the connect headers which a proxy or a registry target forwards in the connection
// context, when one of them may forward any. It sets them on newCtx, or on ctx when newCtx is nil, and returns the
// context to use. It returns newCtx unchanged when no header is kept.
func (h *Handler) setConnectEmulatedHeaders(ctx context.Context, newCtx context.Context, headers map[string]string) context.Context {
	// Only keep emulated headers in the connection context when some proxy may actually forward them - otherwise they
	// would be retained for the whole connection lifetime for nothing.
	if !h.proxyMap.UsesEmulatedHeaders && !h.rpcRegistry.UsesEmulatedHeaders() {
		return newCtx
	}
	kept := keepEmulatedHeaders(headers, h.proxyMap.EmulatedHeaderNames, h.rpcRegistry.EmulatedHeaderAllowed)
	if len(kept) == 0 {
		return newCtx
	}
	if newCtx == nil {
		newCtx = ctx
	}
	return clientcontext.SetEmulatedHeadersToContext(newCtx, kept)
}

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
