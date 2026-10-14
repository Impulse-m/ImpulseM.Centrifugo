package clientaddr

import (
	"context"
	"net/http"
	"net/netip"

	"github.com/justinas/alice"
	"google.golang.org/grpc/peer"
)

type addrKey struct{}

// Middleware resolves the client address of each request with r and stores it in the request
// context. A request whose address is unknown is passed on without a value.
func Middleware(r *Resolver) alice.Constructor {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			addr := r.Resolve(req.RemoteAddr, req.Header)
			if addr.IsValid() {
				req = req.WithContext(withAddr(req.Context(), addr))
			}
			next.ServeHTTP(w, req)
		})
	}
}

// WithPeer stores the address of the gRPC peer in ctx. The peer of a connection on a PROXY protocol
// port is the PROXY source. Without a peer address ctx is returned unchanged.
func WithPeer(ctx context.Context) context.Context {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ctx
	}
	addr, ok := parseAddr(p.Addr.String())
	if !ok {
		return ctx
	}
	return withAddr(ctx, addr)
}

// FromContext returns the client address stored by Middleware or WithPeer.
func FromContext(ctx context.Context) (netip.Addr, bool) {
	addr, ok := ctx.Value(addrKey{}).(netip.Addr)
	return addr, ok
}

// ThrottleKey returns the key an auth throttle counts a request under. It is the resolved address
// when the middleware ran, and the legacy rule otherwise.
func ThrottleKey(r *http.Request) string {
	if addr, ok := FromContext(r.Context()); ok {
		return addr.String()
	}
	return legacyThrottleKey(r)
}

// LogAddr returns the address written to the request log. It is the resolved address when the
// middleware ran, and the legacy rule otherwise.
func LogAddr(r *http.Request) string {
	if addr, ok := FromContext(r.Context()); ok {
		return addr.String()
	}
	return legacyLogAddr(r)
}

func withAddr(ctx context.Context, addr netip.Addr) context.Context {
	return context.WithValue(ctx, addrKey{}, addr)
}
