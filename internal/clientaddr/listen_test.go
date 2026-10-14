package clientaddr

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/require"
)

// proxyV2Signature is the fixed 12-byte prefix of a PROXY protocol v2 header.
var proxyV2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

// proxyV1Header is a PROXY protocol v1 line: the client 198.51.100.7:4567 behind the proxy.
const proxyV1Header = "PROXY TCP4 198.51.100.7 203.0.113.9 4567 443\r\n"

// proxyV2Header is the same client as proxyV1Header, written as a PROXY protocol v2 binary header.
func proxyV2Header() []byte {
	header := append([]byte{}, proxyV2Signature...)
	header = append(header, 0x21, 0x11) // PROXY command, TCP over IPv4
	header = binary.BigEndian.AppendUint16(header, 12)
	header = append(header, 198, 51, 100, 7) // source address
	header = append(header, 203, 0, 113, 9)  // destination address
	header = binary.BigEndian.AppendUint16(header, 4567)
	header = binary.BigEndian.AppendUint16(header, 443)
	return header
}

// proxyListener listens on loopback with PROXY protocol and the given untrusted policy. The
// client dialling it is 127.0.0.1, which is trusted only when trusted lists it.
func proxyListener(t *testing.T, trusted []string, untrusted string) net.Listener {
	t.Helper()
	cfg := configtypes.ClientAddress{
		Enabled:        true,
		TrustedProxies: trusted,
		Header:         "x-forwarded-for",
		ProxyProtocol: configtypes.ClientAddressProxyProtocol{
			Enabled:           true,
			UntrustedPeers:    untrusted,
			ReadHeaderTimeout: configtypes.Duration(5 * time.Second),
		},
	}
	ln, err := Listen("tcp", "127.0.0.1:0", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// acceptAsync accepts one connection in the background.
func acceptAsync(t *testing.T, ln net.Listener) <-chan net.Conn {
	t.Helper()
	ch := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(ch)
			return
		}
		ch <- conn
	}()
	return ch
}

func dialWithBytes(t *testing.T, ln net.Listener, payload ...[]byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	for _, p := range payload {
		_, err = conn.Write(p)
		require.NoError(t, err)
	}
	return conn
}

func TestProxyProtocolV1FromTrustedPeer(t *testing.T) {
	ln := proxyListener(t, []string{"127.0.0.1"}, "reject")
	accepted := acceptAsync(t, ln)
	dialWithBytes(t, ln, []byte(proxyV1Header), []byte("ping"))

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))
	require.Equal(t, "198.51.100.7:4567", conn.RemoteAddr().String())
}

func TestProxyProtocolV2FromTrustedPeer(t *testing.T) {
	ln := proxyListener(t, []string{"127.0.0.1"}, "reject")
	accepted := acceptAsync(t, ln)
	dialWithBytes(t, ln, proxyV2Header(), []byte("ping"))

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))
	require.Equal(t, "198.51.100.7:4567", conn.RemoteAddr().String())
}

func TestProxyProtocolUntrustedPeerRejectsHeader(t *testing.T) {
	// The client 127.0.0.1 is not in trusted_proxies, so a PROXY header from it is refused.
	ln := proxyListener(t, []string{"10.0.0.1"}, "reject")
	accepted := acceptAsync(t, ln)
	dialWithBytes(t, ln, []byte(proxyV1Header), []byte("ping"))

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	_, err := conn.Read(make([]byte, 4))
	require.ErrorIs(t, err, proxyproto.ErrSuperfluousProxyHeader)
}

func TestProxyProtocolUntrustedPeerIgnoresHeader(t *testing.T) {
	// Under ignore the header is read and discarded: the spoofed source does not show up.
	ln := proxyListener(t, []string{"10.0.0.1"}, "ignore")
	accepted := acceptAsync(t, ln)
	dialWithBytes(t, ln, []byte(proxyV1Header), []byte("ping"))

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))
	require.Equal(t, "127.0.0.1", conn.RemoteAddr().(*net.TCPAddr).IP.String())
}

func TestProxyProtocolUntrustedPeerWithoutHeaderIsSocketAddress(t *testing.T) {
	// A direct client of a port shared with a balancer: no header, the socket address is the client.
	ln := proxyListener(t, []string{"10.0.0.1"}, "reject")
	accepted := acceptAsync(t, ln)
	dialWithBytes(t, ln, []byte("ping"))

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", conn.RemoteAddr().(*net.TCPAddr).IP.String())
}

func TestProxyProtocolTrustedPeerWithoutHeaderIsRejected(t *testing.T) {
	ln := proxyListener(t, []string{"127.0.0.1"}, "reject")
	accepted := acceptAsync(t, ln)
	dialWithBytes(t, ln, []byte("ping"))

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	_, err := conn.Read(make([]byte, 4))
	require.ErrorIs(t, err, proxyproto.ErrNoProxyProtocol)
}

func TestProxyProtocolTLSAfterHeader(t *testing.T) {
	ln := proxyListener(t, []string{"127.0.0.1"}, "reject")
	serverCfg, roots := selfSignedTLSConfig(t)
	tlsLn := tls.NewListener(ln, serverCfg)
	accepted := acceptAsync(t, tlsLn)

	raw := dialWithBytes(t, ln, proxyV2Header())
	clientConn := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	t.Cleanup(func() { _ = clientConn.Close() })
	go func() {
		_, _ = clientConn.Write([]byte("hello"))
	}()

	conn := <-accepted
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 5)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "hello", string(buf))
	require.Equal(t, "198.51.100.7:4567", conn.RemoteAddr().String())
}

func TestWrapDisabledReturnsListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	got, err := Wrap(ln, configtypes.ClientAddress{Enabled: true})
	require.NoError(t, err)
	require.Same(t, ln, got)

	got, err = Wrap(ln, configtypes.ClientAddress{ProxyProtocol: configtypes.ClientAddressProxyProtocol{Enabled: true}})
	require.NoError(t, err)
	require.Same(t, ln, got)
}

func TestListenWithoutProxyProtocolIsPlain(t *testing.T) {
	ln, err := Listen("tcp", "127.0.0.1:0", configtypes.ClientAddress{})
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	require.IsType(t, &net.TCPListener{}, ln)
}

// selfSignedTLSConfig returns a server TLS config with a certificate for localhost, generated at
// test time, and a pool which trusts exactly that certificate.
func selfSignedTLSConfig(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	}
	return serverCfg, roots
}
