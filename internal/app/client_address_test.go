package app

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/clientaddr"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/unigrpc/unistream"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// clientAddressTestConfig enables the feature with the loopback address trusted, so that the test
// client, which connects from loopback, is a trusted proxy.
func clientAddressTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.ClientAddress = configtypes.ClientAddress{
		Enabled:        true,
		TrustedProxies: []string{"127.0.0.1"},
		Header:         "x-forwarded-for",
	}
	return cfg
}

// capturingNode runs a node which sends the client context of every connect to the channel.
func capturingNode(t *testing.T) (*centrifuge.Node, <-chan context.Context) {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	connected := make(chan context.Context, 1)
	node.OnConnecting(func(ctx context.Context, _ centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		select {
		case connected <- ctx:
		default:
		}
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{}}, nil
	})
	return node, connected
}

// waitClientContext waits for the context of a connect.
func waitClientContext(t *testing.T, connected <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-connected:
		return ctx
	case <-time.After(5 * time.Second):
		t.Fatal("client did not connect")
		return nil
	}
}

// clientAddressMux serves the WebSocket and SSE handlers through Mux, with the client address
// middlewares built from cfg the way runHTTPServers builds them.
func clientAddressMux(t *testing.T, cfg config.Config, node *centrifuge.Node) *httptest.Server {
	t.Helper()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	clientMiddlewares, err := clientaddr.Middlewares(cfg.ClientAddress)
	require.NoError(t, err)
	mux := Mux(node, cfgContainer, nil, HandlerWebsocket|HandlerSSE, false, nil, func() bool { return true }, clientMiddlewares)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestSSEConnectSeesForwardedClientAddress(t *testing.T) {
	cfg := clientAddressTestConfig()
	node, connected := capturingNode(t)
	server := clientAddressMux(t, cfg, node)

	// cf_connect holds protocol commands, the same as a WebSocket frame.
	query := url.Values{"cf_connect": {`{"id":1,"connect":{"name":"test"}}`}}
	req, err := http.NewRequest(http.MethodGet, server.URL+cfg.SSE.HandlerPrefix+"?"+query.Encode(), nil)
	require.NoError(t, err)
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	addr, ok := clientaddr.FromContext(waitClientContext(t, connected))
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("198.51.100.7"), addr)
}

func TestWebSocketConnectSeesForwardedClientAddress(t *testing.T) {
	cfg := clientAddressTestConfig()
	node, connected := capturingNode(t)
	server := clientAddressMux(t, cfg, node)

	// Upgrade by hand: the 101 response body of net/http is the raw connection.
	req, err := http.NewRequest(http.MethodGet, server.URL+cfg.WebSocket.HandlerPrefix, nil)
	require.NoError(t, err)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	conn, ok := resp.Body.(io.ReadWriteCloser)
	require.True(t, ok)

	_, err = conn.Write(maskedTextFrame(t, []byte(`{"id":1,"connect":{"name":"test"}}`)))
	require.NoError(t, err)

	addr, ok := clientaddr.FromContext(waitClientContext(t, connected))
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("198.51.100.7"), addr)
}

// maskedTextFrame builds one client-to-server WebSocket text frame, which must be masked.
func maskedTextFrame(t *testing.T, payload []byte) []byte {
	t.Helper()
	require.Less(t, len(payload), 126, "frame payload too long for the short form")
	mask := [4]byte{0x12, 0x34, 0x56, 0x78}
	frame := []byte{0x81, 0x80 | byte(len(payload))}
	frame = append(frame, mask[:]...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	return frame
}

// proxyV2Header is a PROXY protocol v2 header for the client 198.51.100.7:4567.
func proxyV2Header() []byte {
	header := []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
	header = append(header, 0x21, 0x11) // PROXY command, TCP over IPv4
	header = binary.BigEndian.AppendUint16(header, 12)
	header = append(header, 198, 51, 100, 7)
	header = append(header, 203, 0, 113, 9)
	header = binary.BigEndian.AppendUint16(header, 4567)
	header = binary.BigEndian.AppendUint16(header, 443)
	return header
}

// freeLoopbackPort returns a port which was free a moment ago on loopback.
func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

func TestUniGRPCConnectSeesProxyClientAddress(t *testing.T) {
	cfg := clientAddressTestConfig()
	cfg.ClientAddress.ProxyProtocol = configtypes.ClientAddressProxyProtocol{
		Enabled:           true,
		UntrustedPeers:    "reject",
		ReadHeaderTimeout: configtypes.Duration(5 * time.Second),
	}
	cfg.UniGRPC.Address = "127.0.0.1"
	cfg.UniGRPC.Port = freeLoopbackPort(t)

	node, connected := capturingNode(t)
	// runGRPCUniServer listens through clientaddr.Listen and registers the stream interceptor.
	srv, err := runGRPCUniServer(cfg, node)
	require.NoError(t, err)
	t.Cleanup(srv.Stop)

	// The client writes the PROXY header itself, before gRPC's first bytes.
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(cfg.UniGRPC.Address, strconv.Itoa(cfg.UniGRPC.Port)))
		if err != nil {
			return nil, err
		}
		if _, err := conn.Write(proxyV2Header()); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return conn, nil
	}
	clientConn, err := grpc.NewClient("passthrough:///uni",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()

	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = unistream.NewCentrifugoUniStreamClient(clientConn).Consume(streamCtx, &unistream.ConnectRequest{})
	require.NoError(t, err)

	addr, ok := clientaddr.FromContext(waitClientContext(t, connected))
	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("198.51.100.7"), addr)
}
