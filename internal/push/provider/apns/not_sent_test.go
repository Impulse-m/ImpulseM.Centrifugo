package apns

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// silentListener returns the address of a TCP listener that accepts connections and never answers them, so that the TLS
// handshake never completes and no request is written.
func silentListener(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	return listener.Addr().String()
}

// TestAPNsHeldBeforeWriteIsNotSentWithDeadline checks that a call whose handshake never completes is not written: the
// target is not_sent, and the call deadline is its code.
func TestAPNsHeldBeforeWriteIsNotSentWithDeadline(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	client, err := NewClient(credential, "production", testBundleID, withHost("https://"+silentListener(t)))
	require.NoError(t, err)

	set := provider.NewSet()
	set.Add(provider.ProviderAPNs, client, 10)
	started := time.Now()
	result := set.Dispatch(context.Background(), 100*time.Millisecond, newTestJob(0, "tok", nil, "{}"))

	require.Less(t, time.Since(started), 5*time.Second)
	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeDeadline, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// TestAPNsConnectRefusedIsNotSentWithConnectFailed checks that a refused connection is a transport error before the
// write: the target is not_sent with connect_failed.
func TestAPNsConnectRefusedIsNotSentWithConnectFailed(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {})
	client := newTestClient(t, fake, credential)
	fake.server.Close()

	set := provider.NewSet()
	set.Add(provider.ProviderAPNs, client, 10)
	result := set.Dispatch(context.Background(), time.Minute, newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeConnectFailed, result.GetCode())
}
