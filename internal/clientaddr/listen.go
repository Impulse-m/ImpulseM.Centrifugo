package clientaddr

import (
	"net"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/pires/go-proxyproto"
)

// Listen is net.Listen for a client-facing port. When client_address.proxy_protocol is enabled, the
// listener reads a PROXY protocol header from each connection before handing the connection on, so
// a TLS handshake, an HTTP request or a gRPC frame always starts after the header.
func Listen(network, addr string, cfg configtypes.ClientAddress) (net.Listener, error) {
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	wrapped, err := Wrap(ln, cfg)
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	return wrapped, nil
}

// ListenHTTP is Listen for an HTTP server port. PROXY protocol applies only to a client-facing port,
// so the other ports are listened on as they were before the feature.
func ListenHTTP(addr string, cfg configtypes.ClientAddress, clientFacing bool) (net.Listener, error) {
	if !clientFacing {
		cfg.ProxyProtocol.Enabled = false
	}
	return Listen("tcp", addr, cfg)
}

// Wrap returns ln with PROXY protocol handling when cfg enables it, and ln unchanged otherwise.
// Peers in trusted_proxies must send a header, and other peers are rejected or have their header
// ignored, as untrusted_peers says.
func Wrap(ln net.Listener, cfg configtypes.ClientAddress) (net.Listener, error) {
	if !cfg.Enabled || !cfg.ProxyProtocol.Enabled {
		return ln, nil
	}
	resolver, err := New(cfg)
	if err != nil {
		return nil, err
	}
	untrusted := proxyproto.REJECT
	if cfg.ProxyProtocol.UntrustedPeers == "ignore" {
		untrusted = proxyproto.IGNORE
	}
	policy, err := proxyproto.PolicyFromRanges(resolver.ranges, proxyproto.REQUIRE, untrusted)
	if err != nil {
		return nil, err
	}
	return &proxyproto.Listener{
		Listener:          ln,
		ConnPolicy:        policy,
		ReadHeaderTimeout: cfg.ProxyProtocol.ReadHeaderTimeout.ToDuration(),
	}, nil
}
