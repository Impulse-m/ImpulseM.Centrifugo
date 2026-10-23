package push

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// TestTooManyCallsErrorIsTemporaryWithRetryAfter checks the answer of a call over max_concurrent_requests: a temporary
// 111 whose metadata tells the caller to wait one second.
func TestTooManyCallsErrorIsTemporaryWithRetryAfter(t *testing.T) {
	refusal := tooManyCallsError()

	require.Equal(t, uint32(111), refusal.GetCode())
	require.True(t, refusal.GetTemporary())
	require.Equal(t, "1", refusal.GetMetadata()["x-centrifugo-retry-after"])
}

// TestBusySendAnswersRetryAfter checks the same answer from the send path: a call that finds the slots taken is refused
// with the metadata, and nothing is sent.
func TestBusySendAnswersRetryAfter(t *testing.T) {
	fake := &countingSender{}
	s := newTestSender(t, fake, func(cfg *configtypes.PushNotifications) {
		cfg.MaxConcurrentRequests = 1
	})
	release, busy := s.Admit()
	require.Nil(t, busy)
	defer release()

	resp := s.Send(context.Background(), validRequest())

	require.Equal(t, uint32(111), resp.GetError().GetCode())
	require.Equal(t, "1", resp.GetError().GetMetadata()["x-centrifugo-retry-after"])
	require.Zero(t, fake.calls.Load())
}
