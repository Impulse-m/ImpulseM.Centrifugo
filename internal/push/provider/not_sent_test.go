package provider

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// outcomeSender answers the way a provider does when a call gets no status line. When hold is set, it waits until its
// call context ends. It reports the written bit of its request through NoAnswerResult, and it signals entered when its
// call starts.
type outcomeSender struct {
	hold    bool
	written bool
	entered chan struct{}
}

// Send answers the job with the no-answer result of its call.
func (s *outcomeSender) Send(ctx context.Context, job *Job) *apiproto.PushItemResult {
	if s.entered != nil {
		s.entered <- struct{}{}
	}
	if s.hold {
		<-ctx.Done()
	}
	return NoAnswerResult(job, s.written, ctx)
}

// TestNotSentCodesByCause checks the status and the code of a target that got no status line, by where its call ended.
func TestNotSentCodesByCause(t *testing.T) {
	testCases := []struct {
		name    string
		sender  *outcomeSender
		timeout time.Duration
		status  string
		code    string
	}{
		{
			name:    "connect refused",
			sender:  &outcomeSender{},
			timeout: time.Minute,
			status:  StatusNotSent,
			code:    CodeConnectFailed,
		},
		{
			name:    "deadline before the write",
			sender:  &outcomeSender{hold: true},
			timeout: 50 * time.Millisecond,
			status:  StatusNotSent,
			code:    CodeDeadline,
		},
		{
			name:    "deadline after the write",
			sender:  &outcomeSender{hold: true, written: true},
			timeout: 50 * time.Millisecond,
			status:  StatusUnanswered,
			code:    "",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			set := NewSet()
			set.Add(ProviderAPNs, testCase.sender, 10)

			result := set.Dispatch(context.Background(), testCase.timeout, newTestJob(0, ProviderAPNs))

			require.Equal(t, testCase.status, result.GetStatus())
			require.Equal(t, testCase.code, result.GetCode())
			require.Zero(t, result.GetHttpStatus())
		})
	}
}

// TestNotSentNoSlotWhenSlotIsHeld checks that a target that gets no in-flight slot before its slot wait ends is not_sent
// with no_slot, and makes no call. The held call still answers when it is released.
func TestNotSentNoSlotWhenSlotIsHeld(t *testing.T) {
	sender := &outcomeSender{hold: true, written: true, entered: make(chan struct{}, 2)}
	set := NewSet()
	set.Add(ProviderAPNs, sender, 1)

	holdCtx, release := context.WithCancel(context.Background())
	done := make(chan *apiproto.PushItemResult, 1)
	go func() {
		done <- set.DispatchWindow(context.Background(), holdCtx, time.Minute, newTestJob(0, ProviderAPNs))
	}()
	<-sender.entered

	slot, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := set.DispatchWindow(slot, context.Background(), time.Minute, newTestJob(1, ProviderAPNs))

	require.Equal(t, StatusNotSent, result.GetStatus())
	require.Equal(t, CodeNoSlot, result.GetCode())
	require.Len(t, sender.entered, 0, "a target without a slot makes no call")

	release()
	require.Equal(t, StatusUnanswered, (<-done).GetStatus())
}

// TestNotSentShutdownEndsWait checks that the end of the node's shutdown context ends a call that has not been written,
// and that the target is then not_sent with shutdown, not deadline.
func TestNotSentShutdownEndsWait(t *testing.T) {
	shutdown, stop := context.WithCancel(context.Background())
	defer stop()

	sender := &outcomeSender{hold: true, entered: make(chan struct{}, 1)}
	set := NewSet().WithShutdown(shutdown)
	set.Add(ProviderAPNs, sender, 10)

	done := make(chan *apiproto.PushItemResult, 1)
	go func() {
		done <- set.DispatchWindow(shutdown, shutdown, time.Minute, newTestJob(0, ProviderAPNs))
	}()
	<-sender.entered
	stop()

	result := <-done
	require.Equal(t, StatusNotSent, result.GetStatus())
	require.Equal(t, CodeShutdown, result.GetCode())
}
