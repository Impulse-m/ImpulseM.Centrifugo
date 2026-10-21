package client

import (
	"testing"
	"time"

	"github.com/centrifugal/protocol"
)

// waitUnsubscribePush waits until the client is sent an unsubscribe push for the channel and returns it.
func (t *recordingTransport) waitUnsubscribePush(tb testing.TB, channel string) *protocol.Unsubscribe {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		t.mu.Lock()
		for _, reply := range t.replies {
			if reply.Push != nil && reply.Push.Channel == channel && reply.Push.Unsubscribe != nil {
				unsubscribe := reply.Push.Unsubscribe
				t.mu.Unlock()
				return unsubscribe
			}
		}
		t.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("unsubscribe push for channel %s not received", channel)
	return nil
}
