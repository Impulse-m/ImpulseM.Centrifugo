package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc"
)

// errChannelEventsNullAnswer is returned when a channel events proxy answers the JSON null value. A null answer
// carries no result, so it is not taken as delivered.
var errChannelEventsNullAnswer = errors.New("channel events proxy answered null")

// isChannelEventsNullAnswer reports whether the body of an HTTP answer is the JSON null value.
func isChannelEventsNullAnswer(body []byte) bool {
	return bytes.Equal(bytes.TrimSpace(body), []byte("null"))
}

// channelEventsHTTP is the HTTP transport shared by the channel events proxies: channel state and cache empty.
// The map remove proxies keep their own copy of this setup, so the upstream files stay unchanged.
type channelEventsHTTP struct {
	config     Config
	httpCaller HTTPCaller
}

// newChannelEventsHTTP creates the HTTP transport of a channel events proxy. logEntity names the proxy in
// the trace log.
func newChannelEventsHTTP(p Config, logEntity string) (channelEventsHTTP, error) {
	httpClient, err := proxyHTTPClient(p, logEntity)
	if err != nil {
		return channelEventsHTTP{}, fmt.Errorf("error creating HTTP client: %w", err)
	}
	return channelEventsHTTP{
		config:     p,
		httpCaller: NewHTTPCaller(httpClient),
	}, nil
}

// Protocol ...
func (t channelEventsHTTP) Protocol() string {
	return "http"
}

// Timeout ...
func (t channelEventsHTTP) Timeout() time.Duration {
	return t.config.Timeout.ToDuration()
}

// channelEventsGRPC is the gRPC transport shared by the channel events proxies: channel state and cache empty.
type channelEventsGRPC struct {
	config Config
	client proxyproto.CentrifugoProxyClient
}

// newChannelEventsGRPC dials the gRPC endpoint of p and returns the transport of a channel events proxy.
func newChannelEventsGRPC(name string, p Config) (channelEventsGRPC, error) {
	host, err := getGrpcHost(p.Endpoint)
	if err != nil {
		return channelEventsGRPC{}, fmt.Errorf("error getting grpc host: %v", err)
	}
	dialOpts, err := getDialOpts(name, p)
	if err != nil {
		return channelEventsGRPC{}, fmt.Errorf("error creating GRPC dial options: %v", err)
	}
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return channelEventsGRPC{}, fmt.Errorf("error connecting to GRPC proxy server: %v", err)
	}
	return channelEventsGRPC{
		config: p,
		client: proxyproto.NewCentrifugoProxyClient(conn),
	}, nil
}

// Protocol ...
func (t channelEventsGRPC) Protocol() string {
	return "grpc"
}

// Timeout ...
func (t channelEventsGRPC) Timeout() time.Duration {
	return t.config.Timeout.ToDuration()
}

// getChannelEventsProxy returns the HTTP proxy built by newHTTP or the gRPC proxy built by newGRPC, by the
// endpoint of p.
func getChannelEventsProxy[T any](
	name string,
	p Config,
	newHTTP func(Config) (T, error),
	newGRPC func(string, Config) (T, error),
) (T, error) {
	normalizeProxyHeaderNames(&p)
	if isHttpEndpoint(p.Endpoint) {
		return newHTTP(p)
	}
	return newGRPC(name, p)
}
