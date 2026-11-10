package webpush

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"
)

// Timeouts of the provider connection: the dial of the TCP connection and the TLS handshake each have their own.
const (
	dialTimeout         = 10 * time.Second
	tlsHandshakeTimeout = 10 * time.Second
)

// errAddressBlocked is the error of a connection attempt that the guard refused. The request result never carries it: the
// refusal is recorded on the request, and the result is address_blocked.
var errAddressBlocked error = errors.New("webpush connection refused by the address guard")

// refusedPrefixes are the ranges the guard refuses on top of the ones that PRO lists: the carrier-grade NAT range, the
// reserved and benchmark ranges, and the IPv6 forms that embed an IPv4 address.
var refusedPrefixes []netip.Prefix = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("::/96"),
}

// refusedAddress reports whether the guard refuses a connection to address. An IPv4-mapped address is judged as the IPv4
// address it maps, and a zone does not change the range. An invalid address is refused.
func refusedAddress(address netip.Addr) bool {
	if !address.IsValid() {
		return true
	}
	address = address.WithZone("").Unmap()
	if address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsUnspecified() || address.IsMulticast() {
		return true
	}
	for _, prefix := range refusedPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

// refusedAttempt reports whether the guard refuses a connection attempt to attempt, which is the "ip:port" address the
// dialer gives its Control hook. An attempt that does not parse is refused.
func refusedAttempt(attempt string) bool {
	addressPort, err := netip.ParseAddrPort(attempt)
	if err != nil {
		return true
	}
	return refusedAddress(addressPort.Addr())
}

// addressGuard records whether the guard refused a connection of one request. It travels in the request context, as the
// write trace does, so the result can name the refusal whichever error the transport returns.
type addressGuard struct {
	refused atomic.Bool
}

// guardKey is the context key of the address guard of a request.
type guardKey struct{}

// Context returns ctx with the guard, so the dialer of the request can record a refusal on it.
func (g *addressGuard) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, guardKey{}, g)
}

// Refused reports whether the guard refused a connection of the request.
func (g *addressGuard) Refused() bool {
	return g.refused.Load()
}

// refuse records a refusal on the guard of a request. A nil guard records nothing.
func (g *addressGuard) refuse() {
	if g != nil {
		g.refused.Store(true)
	}
}

// guardOf returns the address guard that ctx carries, or nil when it carries none.
func guardOf(ctx context.Context) *addressGuard {
	guard, _ := ctx.Value(guardKey{}).(*addressGuard)
	return guard
}

// dialContext dials a connection of one request. The dialer has no proxy and no resolver of its own, and its Control hook
// checks the address of every connection attempt, so the address that is checked is the one the connection is made to. A
// refused attempt records the refusal on the guard of the request, and the dial fails.
func dialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	guard := guardOf(ctx)
	dialer := net.Dialer{
		Timeout: dialTimeout,
		Control: func(_ string, attempt string, _ syscall.RawConn) error {
			if !refusedAttempt(attempt) {
				return nil
			}
			guard.refuse()
			return errAddressBlocked
		},
	}
	return dialer.DialContext(ctx, network, address)
}
