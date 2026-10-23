package fcm

import (
	"context"
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
