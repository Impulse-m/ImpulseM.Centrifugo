package accessrevoke

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

type fakeConnection struct {
	id          string
	mu          sync.Mutex
	disconnects []centrifuge.Disconnect
	times       []time.Time
}

// ID implements Connection.
func (c *fakeConnection) ID() string {
	return c.id
}

func (c *fakeConnection) Disconnect(disconnect ...centrifuge.Disconnect) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnects = append(c.disconnects, disconnect[0])
	c.times = append(c.times, time.Now())
}

func (c *fakeConnection) snapshot() ([]centrifuge.Disconnect, []time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]centrifuge.Disconnect(nil), c.disconnects...), append([]time.Time(nil), c.times...)
}

func runQueue(t *testing.T, queue *DisconnectQueue) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- queue.Run(ctx)
	}()
	return cancel, errCh
}

func TestDisconnectQueuePacesDisconnects(t *testing.T) {
	// 20 per second means one disconnect every 50ms.
	queue := NewDisconnectQueue(20, 100)
	conn := &fakeConnection{}
	for range 5 {
		require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	}

	cancel, errCh := runQueue(t, queue)
	defer cancel()

	require.Eventually(t, func() bool {
		disconnects, _ := conn.snapshot()
		return len(disconnects) == 5
	}, 5*time.Second, 5*time.Millisecond)

	disconnects, times := conn.snapshot()
	for _, disconnect := range disconnects {
		require.Equal(t, centrifuge.DisconnectExpired, disconnect)
	}
	// Four gaps of 50ms between five disconnects; allow some slack for timers.
	require.GreaterOrEqual(t, times[4].Sub(times[0]), 150*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestDisconnectQueueDropsWhenFull(t *testing.T) {
	// Queue is not drained, so the third disconnect does not fit.
	queue := NewDisconnectQueue(1000, 2)
	conn := &fakeConnection{}
	dropped := counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total")

	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.True(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))
	require.False(t, queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession))

	require.Equal(t, dropped+1, counterValue(t, "centrifugo_access_revoke_disconnects_dropped_total"))
	disconnects, _ := conn.snapshot()
	require.Empty(t, disconnects)
}

func TestDisconnectQueueRunReturnsOnContextDone(t *testing.T) {
	queue := NewDisconnectQueue(1000, 10)
	cancel, errCh := runQueue(t, queue)

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("queue Run did not return after context cancel")
	}
}
