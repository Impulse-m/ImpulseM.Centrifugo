package fcm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// TestFCMConnectRefusedIsNotSentWithConnectFailed checks that a refused connection to the send endpoint is a transport
// error before the write: the target is not_sent with connect_failed, and the token was fetched.
func TestFCMConnectRefusedIsNotSentWithConnectFailed(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))
	fake.server.Close()

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeConnectFailed, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// TestFCMWrittenThenHeldIsUnanswered checks through the real client that a request the server read whole, and then held,
// is unanswered: the write reached httptrace, so FCM may have delivered the message.
func TestFCMWrittenThenHeldIsUnanswered(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	})
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result := client.Send(ctx, newTestJob(0, `{"data":{}}`))

	bodies, _ := fake.recorded()
	require.Len(t, bodies, 1, "the server must have read the request before it held the answer")
	require.Equal(t, provider.StatusUnanswered, result.GetStatus())
	require.Empty(t, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// failedFCMWrite is a transport that fails the write of every request the way a reset connection does: it reports the
// write error to the trace of the request, then returns the error.
type failedFCMWrite struct{}

// RoundTrip reports the write error to the write hook of the request, and returns it.
func (failedFCMWrite) RoundTrip(request *http.Request) (*http.Response, error) {
	writeErr := errors.New("write: connection reset by peer")
	trace := httptrace.ContextClientTrace(request.Context())
	if trace != nil && trace.WroteRequest != nil {
		trace.WroteRequest(httptrace.WroteRequestInfo{Err: writeErr})
	}
	return nil, writeErr
}

// TestFCMFailedWriteIsNotSentWithConnectFailed checks through the real client that a write which fails is not written:
// the target is not_sent with connect_failed, not unanswered. The token is fetched from the fake token endpoint.
func TestFCMFailedWriteIsNotSentWithConnectFailed(t *testing.T) {
	tokens := newFakeToken(t, false)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMTransport(failedFCMWrite{}))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeConnectFailed, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// TestFCMTokenFetchAfterShutdownIsShutdown checks that a token fetch which fails because the node is shutting down is
// not_sent with shutdown, not with token_fetch, which names a provider failure.
func TestFCMTokenFetchAfterShutdownIsShutdown(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)

	processCtx, stop := context.WithCancel(context.Background())
	stop()
	client, err := NewClient(processCtx, newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))
	require.NoError(t, err)

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeShutdown, result.GetCode())
	bodies, _ := fake.recorded()
	require.Empty(t, bodies, "no send request is made when the token is not fetched")
}
