package clientaddr

import (
	"context"
	"net"
	"net/http"
	"net/netip"

	"github.com/justinas/alice"
	"google.golang.org/grpc/peer"
)

type stateKey struct{}

// state is what the middleware leaves in the context. addr is the zero value when the address
// is unknown; the state itself is still present, which tells an enabled feature from a disabled one.
type state struct {
	addr netip.Addr
}

// Middleware resolves the client address of each request with r and stores the result in the
// request context. A request whose address is unknown is stored without an address.
func Middleware(r *Resolver) alice.Constructor {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			addr := r.Resolve(req.RemoteAddr, req.Header)
			next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), stateKey{}, state{addr: addr})))
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
	st, _ := ctx.Value(stateKey{}).(state)
	return st.addr, st.addr.IsValid()
}

// ThrottleKey returns the key an auth throttle counts a request under. With the feature enabled it
// is the resolved address, or the socket host when the address is unknown. With the feature
// disabled, where no middleware ran, it is the legacy rule.
func ThrottleKey(r *http.Request) string {
	if host, ok := enabledHost(r); ok {
		return host
	}
	return legacyThrottleKey(r)
}

// LogAddr returns the address written to the request log, with the same rules as ThrottleKey.
func LogAddr(r *http.Request) string {
	if host, ok := enabledHost(r); ok {
		return host
	}
	return legacyLogAddr(r)
}

// enabledHost returns the address the middleware stored, or the socket host when it stored none.
// The second result is false when the middleware did not run.
func enabledHost(r *http.Request) (string, bool) {
	st, ok := r.Context().Value(stateKey{}).(state)
	if !ok {
		return "", false
	}
	if st.addr.IsValid() {
		return st.addr.String(), true
	}
	return socketHost(r.RemoteAddr), true
}

// socketHost returns the host of a remote address without its port.
func socketHost(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func withAddr(ctx context.Context, addr netip.Addr) context.Context {
	return context.WithValue(ctx, stateKey{}, state{addr: addr})
}
