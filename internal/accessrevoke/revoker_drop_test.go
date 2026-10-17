package accessrevoke

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRevokeSessionRequestsSweepWhenDisconnectDropped revokes the session of a connection while the disconnect queue
// has no room. The disconnect is dropped, and the queue does not retry it, so a sweep is requested to disconnect it.
func TestRevokeSessionRequestsSweepWhenDisconnectDropped(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 0)
	revoker := NewRevoker(store, node.Hub(), queue)
	swept := make(chan struct{}, 1)
	revoker.sweep = func() {
		select {
		case swept <- struct{}{}:
		default:
		}
	}
	connectTestClient(t, node, "42|s1|0|1000")

	require.NoError(t, revoker.RevokeSession("42", "s1", 1000, 0, nil))
	select {
	case <-swept:
	case <-time.After(5 * time.Second):
		t.Fatal("a dropped disconnect did not request a sweep")
	}
}

// TestRevokeSessionWithRoomRequestsNoSweep revokes the session of a connection while the disconnect queue has room.
// The disconnect is queued, so no sweep is requested.
func TestRevokeSessionWithRoomRequestsNoSweep(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)
	revoker := NewRevoker(store, node.Hub(), queue)
	swept := make(chan struct{}, 1)
	revoker.sweep = func() {
		select {
		case swept <- struct{}{}:
		default:
		}
	}
	connectTestClient(t, node, "42|s1|0|1000")

	require.NoError(t, revoker.RevokeSession("42", "s1", 1000, 0, nil))
	select {
	case <-swept:
		t.Fatal("a queued disconnect requested a sweep")
	case <-time.After(100 * time.Millisecond):
	}
}
