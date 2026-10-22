package provider

import (
	"net/http"
	"time"
)

// Keepalive and idle settings of the provider connections. A ping goes out after a connection is silent for
// pingInterval, and the connection closes when the ping is not answered within pingTimeout. An idle connection
// closes after idleConnTimeout, so its pings end. The settings apply per connection, and a provider keeps a few
// connections per credential and environment on a node.
const (
	pingInterval    = 15 * time.Second
	pingTimeout     = 15 * time.Second
	idleConnTimeout = 90 * time.Second
)

// NewTransport returns the HTTP transport of a provider client: HTTP/2 is always attempted, with the keepalive
// settings above.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   idleConnTimeout,
		HTTP2: &http.HTTP2Config{
			SendPingTimeout: pingInterval,
			PingTimeout:     pingTimeout,
		},
	}
}
