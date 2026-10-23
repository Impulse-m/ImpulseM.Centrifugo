package push

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCallTimeoutClampsToDefault checks the deadline of a call with timeout_ms: zero is the default, a positive value
// below the default is the deadline, and a longer value is clamped to the default, not refused.
func TestCallTimeoutClampsToDefault(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	require.Equal(t, 3*time.Second, s.CallTimeout(3000))
	require.Equal(t, 25*time.Second, s.CallTimeout(25000))
	require.Equal(t, 25*time.Second, s.CallTimeout(60000))
}

// TestSendSetsWriteDeadlineToCallDeadline checks that the send sets the write deadline of the response to the deadline of
// the call, once the request is decoded.
func TestSendSetsWriteDeadlineToCallDeadline(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	var deadline time.Time
	ctx := WithWriteDeadline(context.Background(), func(set time.Time) {
		deadline = set
	})
	req := validRequest()
	req.TimeoutMs = 3000

	before := time.Now()
	resp := s.Send(ctx, req)

	require.Nil(t, resp.GetError())
	require.WithinDuration(t, before.Add(3*time.Second), deadline, time.Second)
}

// TestCallDeadlineCutsHeldCall checks that a call held by its provider ends at the call deadline: the response comes back
// at the deadline, not at the request timeout. The status of the held targets is checked by the provider tests, which
// drive the written bit of the real clients.
func TestCallDeadlineCutsHeldCall(t *testing.T) {
	fake := &countingSender{hold: true}
	s := newTestSender(t, fake, nil)

	req := validRequest()
	req.TimeoutMs = 200

	start := time.Now()
	resp := s.Send(context.Background(), req)
	elapsed := time.Since(start)

	require.Nil(t, resp.GetError())
	require.Less(t, elapsed, 5*time.Second)
	require.Len(t, resp.GetResult().GetResults(), 2)
}

// TestCallDeadlineCountsFromRequestStart checks that the deadline of a call counts from the start of the request, not
// from the moment the send runs: time the body takes to arrive is inside timeout_ms.
func TestCallDeadlineCountsFromRequestStart(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	var deadline time.Time
	ctx := WithWriteDeadline(context.Background(), func(set time.Time) {
		deadline = set
	})
	started := time.Now().Add(-2 * time.Second)
	req := validRequest()
	req.TimeoutMs = 3000

	resp := s.Send(WithAdmission(ctx, started), req)

	require.Nil(t, resp.GetError())
	require.WithinDuration(t, started.Add(3*time.Second), deadline, 500*time.Millisecond)
}
