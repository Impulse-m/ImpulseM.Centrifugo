package provider

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
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
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   idleConnTimeout,
		HTTP2: &http.HTTP2Config{
			SendPingTimeout: pingInterval,
			PingTimeout:     pingTimeout,
		},
	}
}

// WriteTrace records whether the request of one attempt was written. The HTTP transport reports the write through
// httptrace.ClientTrace.WroteRequest, and that hook is the only source of the written bit. The trace is set per attempt.
type WriteTrace struct {
	written atomic.Bool
}

// Trace returns ctx with the hook that records the write of the attempt's request.
func (t *WriteTrace) Trace(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				t.written.Store(true)
			}
		},
	})
}

// Written reports whether the request of the attempt was written.
func (t *WriteTrace) Written() bool {
	return t.written.Load()
}
