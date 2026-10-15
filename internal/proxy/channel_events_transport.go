package proxy

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"google.golang.org/grpc"
)

// newHTTPProxyCaller creates the HTTP caller of a channel events proxy. logEntity names the proxy in
// the trace log. The map remove constructors keep their own copy of this setup, so the upstream files stay
// unchanged.
func newHTTPProxyCaller(p Config, logEntity string) (HTTPCaller, error) {
	httpClient, err := proxyHTTPClient(p, logEntity)
	if err != nil {
		return nil, fmt.Errorf("error creating HTTP client: %w", err)
	}
	return NewHTTPCaller(httpClient), nil
}

// newGRPCProxyClient dials the gRPC endpoint of p and returns the proxy client of a channel events proxy.
func newGRPCProxyClient(name string, p Config) (proxyproto.CentrifugoProxyClient, error) {
	host, err := getGrpcHost(p.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("error getting grpc host: %v", err)
	}
	dialOpts, err := getDialOpts(name, p)
	if err != nil {
		return nil, fmt.Errorf("error creating GRPC dial options: %v", err)
	}
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to GRPC proxy server: %v", err)
	}
	return proxyproto.NewCentrifugoProxyClient(conn), nil
}
