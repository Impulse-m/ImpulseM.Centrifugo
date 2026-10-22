package notify

import (
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func eventOf(op string, seq int) centrifuge.NotificationEvent {
	return centrifuge.NotificationEvent{Op: op, FromNodeID: strconv.Itoa(seq)}
}

func TestDeliveryHandlesEventsOfOneOpInOrder(t *testing.T) {
	const count = 500
	handled := make(chan int, count)
	d := newDelivery(map[string]centrifuge.NotificationHandler{
		"op": func(event centrifuge.NotificationEvent) {
			seq, _ := strconv.Atoi(event.FromNodeID)
			handled <- seq
		},
	}, 16, nil)

	for i := 0; i < count; i++ {
		d.deliver(eventOf("op", i))
	}
	for i := 0; i < count; i++ {
		select {
		case seq := <-handled:
			require.Equal(t, i, seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d was not handled", i)
		}
	}
}

func TestDeliverySlowHandlerDoesNotBlockOtherOp(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	fastHandled := make(chan struct{}, 1)
	d := newDelivery(map[string]centrifuge.NotificationHandler{
		"slow": func(centrifuge.NotificationEvent) {
			<-release
		},
		"fast": func(centrifuge.NotificationEvent) {
			fastHandled <- struct{}{}
		},
	}, 16, nil)

	d.deliver(eventOf("slow", 0))
	d.deliver(eventOf("fast", 0))
	select {
	case <-fastHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("event of a fast op was blocked by a slow op")
	}
}

func TestDeliveryWaitsForFullQueue(t *testing.T) {
	const count = 3
	gate := make(chan struct{})
	handled := make(chan int, count)
	d := newDelivery(map[string]centrifuge.NotificationHandler{
		"op": func(event centrifuge.NotificationEvent) {
			<-gate
			seq, _ := strconv.Atoi(event.FromNodeID)
			handled <- seq
		},
	}, 1, nil)

	pushed := make(chan struct{})
	go func() {
		for i := 0; i < count; i++ {
			d.deliver(eventOf("op", i))
		}
		close(pushed)
	}()

	// The handler holds the first event and the buffer holds the second, so the third must wait.
	select {
	case <-pushed:
		t.Fatal("publisher did not wait for a full queue, an event would be dropped")
	case <-time.After(100 * time.Millisecond):
	}

	close(gate)
	select {
	case <-pushed:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher is still blocked after the handler was released")
	}
	for i := 0; i < count; i++ {
		select {
		case seq := <-handled:
			require.Equal(t, i, seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d was not handled", i)
		}
	}
}

func TestDeliveryRunsInlineOpOnDeliveringGoroutine(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	d := newDelivery(map[string]centrifuge.NotificationHandler{
		accessrevoke.NotifyOp: func(centrifuge.NotificationEvent) {
			close(entered)
			<-release
		},
	}, 1, map[string]bool{accessrevoke.NotifyOp: true})

	returned := make(chan struct{})
	go func() {
		d.deliver(eventOf(accessrevoke.NotifyOp, 0))
		close(returned)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inline handler was not run")
	}
	select {
	case <-returned:
		t.Fatal("deliver returned before the inline handler finished, so the handler did not run on the delivering goroutine")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("deliver did not return after the inline handler finished")
	}
}

// TestDeliveryInlineOpReturnsWhileOtherOpStalled stalls the handler of a queued op, and delivers an inline op meanwhile.
// The inline op has no queue and shares none with the stalled op, so its delivery returns at once.
func TestDeliveryInlineOpReturnsWhileOtherOpStalled(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{}, 1)
	inlineHandled := make(chan struct{}, 1)
	d := newDelivery(map[string]centrifuge.NotificationHandler{
		"slow": func(centrifuge.NotificationEvent) {
			entered <- struct{}{}
			<-release
		},
		accessrevoke.NotifyOp: func(centrifuge.NotificationEvent) {
			inlineHandled <- struct{}{}
		},
	}, 1, map[string]bool{accessrevoke.NotifyOp: true})

	d.deliver(eventOf("slow", 0))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler of the queued op was not run")
	}

	returned := make(chan struct{})
	go func() {
		d.deliver(eventOf(accessrevoke.NotifyOp, 0))
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("inline op waited for the stalled handler of another op")
	}
	select {
	case <-inlineHandled:
	default:
		t.Fatal("inline op returned without running its handler")
	}
}

func TestDeliveryIgnoresOpWithoutHandler(t *testing.T) {
	d := newDelivery(map[string]centrifuge.NotificationHandler{}, 1, nil)
	require.NotPanics(t, func() {
		d.deliver(eventOf("unknown", 0))
	})
}

func TestDeliveryBestEffortOpDropsWhenQueueFull(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	handled := make(chan int, 3)
	inlineHandled := make(chan struct{}, 1)
	d := newDelivery(map[string]centrifuge.NotificationHandler{
		rpcdedup.CancelNotificationOp: func(event centrifuge.NotificationEvent) {
			if event.FromNodeID == "0" {
				entered <- struct{}{}
			}
			<-gate
			seq, _ := strconv.Atoi(event.FromNodeID)
			handled <- seq
		},
		accessrevoke.NotifyOp: func(centrifuge.NotificationEvent) {
			inlineHandled <- struct{}{}
		},
	}, 1, map[string]bool{accessrevoke.NotifyOp: true})

	// The handler holds the first event and the buffer holds the second, so the third is dropped at once.
	d.deliver(eventOf(rpcdedup.CancelNotificationOp, 0))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler of the best effort op was not run")
	}
	d.deliver(eventOf(rpcdedup.CancelNotificationOp, 1))

	returned := make(chan struct{})
	go func() {
		d.deliver(eventOf(rpcdedup.CancelNotificationOp, 2))
		d.deliver(eventOf(accessrevoke.NotifyOp, 0))
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher waited for a full queue of a best effort op")
	}
	select {
	case <-inlineHandled:
	default:
		t.Fatal("inline op after a dropped event did not run its handler")
	}

	close(gate)
	for i := 0; i < 2; i++ {
		select {
		case seq := <-handled:
			require.Equal(t, i, seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d was not handled", i)
		}
	}
	select {
	case seq := <-handled:
		t.Fatalf("event %d was handled after it was dropped", seq)
	case <-time.After(100 * time.Millisecond):
	}
}
