package apns

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
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

// TestAPNsWrittenThenHeldIsUnanswered checks through the real client that a request the server read whole, and then
// held, is unanswered: the write reached httptrace, so the provider may have delivered the push.
func TestAPNsWrittenThenHeldIsUnanswered(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result := newTestClient(t, fake, credential).Send(ctx, newTestJob(0, "tok", nil, "{}"))

	require.Len(t, fake.recorded(), 1, "the server must have read the request before it held the answer")
	require.Equal(t, provider.StatusUnanswered, result.GetStatus())
	require.Empty(t, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// failedWrite is a transport that fails the write of every request the way a reset connection does: it reports the
// write error to the trace of the request, then returns the error. The trace is the one the client sets per attempt.
type failedWrite struct{}

// RoundTrip reports the write error to the write hook of the request, and returns it.
func (failedWrite) RoundTrip(request *http.Request) (*http.Response, error) {
	writeErr := errors.New("write: connection reset by peer")
	trace := httptrace.ContextClientTrace(request.Context())
	if trace != nil && trace.WroteRequest != nil {
		trace.WroteRequest(httptrace.WroteRequestInfo{Err: writeErr})
	}
	return nil, writeErr
}

// TestAPNsFailedWriteIsNotSentWithConnectFailed checks through the real client that a write which fails is not written:
// the target is not_sent with connect_failed, not unanswered, so a resend is not a duplicate of a delivered push.
func TestAPNsFailedWriteIsNotSentWithConnectFailed(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	client, err := NewClient(credential, "production", testBundleID, withRoundTripper(failedWrite{}))
	require.NoError(t, err)

	result := client.Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeConnectFailed, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// TestAPNsRequestBuildFailureIsRejectedWithoutCode checks that a request the client cannot build locally is a rejected
// item with no code. It is not a transport error, so it is not reported as not_sent with connect_failed.
func TestAPNsRequestBuildFailureIsRejectedWithoutCode(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	client, err := NewClient(credential, "production", testBundleID, withHost("://no-scheme"))
	require.NoError(t, err)

	result := client.Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Empty(t, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}
