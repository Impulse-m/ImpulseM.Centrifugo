package accessrevoke

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// sessionIDForTest returns a UUID session ID for the test sessions with index i.
func sessionIDForTest(i int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
}

// sessionKeyForTest returns the session key of sessionIDForTest(i).
func sessionKeyForTest(i int) [16]byte {
	key, _ := Key(sessionIDForTest(i))
	return key
}

// TestSessionConnectionsConcurrentTrackUntrackRevoke runs track, untrack, revoke and reads of the index at the same
// time. Every connection leaves the index at the end, and nothing is lost in between.
func TestSessionConnectionsConcurrentTrackUntrackRevoke(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(0)
	revoker := newTestRevoker(t, node, store)
	index := store.SessionConnections()

	const workers = 8
	const rounds = 300
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := fmt.Sprintf("conn-%d", w)
			conn := &fakeConnection{}
			for r := 0; r < rounds; r++ {
				index.Track(id, conn, sessionKeyForTest(r%4))
				if r%3 == 0 {
					index.Untrack(id)
				}
			}
			index.Untrack(id)
		}(w)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			_ = revoker.RevokeSession("user-1", sessionIDForTest(r%4), 0, 0)
		}
	}()
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			_ = index.Connections(sessionKeyForTest(r % 4))
		}
	}()
	wg.Wait()
	require.Equal(t, 0, index.count())
}

// BenchmarkSessionConnectionsTrackUntrackParallel tracks and untracks connections from all CPUs at once, each with
// its own connection ID, spread over the session shards.
func BenchmarkSessionConnectionsTrackUntrackParallel(b *testing.B) {
	index := newSessionConnections()
	var seq atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		id := fmt.Sprintf("bench-%d", seq.Add(1))
		conn := &fakeConnection{}
		var n int
		for pb.Next() {
			index.Track(id, conn, sessionKeyForTest(n%64))
			index.Untrack(id)
			n++
		}
	})
}

func TestSessionConnectionsTrackAndUntrack(t *testing.T) {
	index := newSessionConnections()
	session, _ := Key(testUUID)
	otherSession, _ := Key(testOtherUUID)
	if otherSession == session {
		t.Fatal("test sessions must differ")
	}
	first := &fakeConnection{}
	second := &fakeConnection{}

	index.Track("first", first, session)
	index.Track("second", second, session)
	require.Equal(t, 2, index.count())
	require.Len(t, index.Connections(session), 2)

	index.Untrack("first")
	require.Equal(t, 1, index.count())
	require.Equal(t, []Connection{second}, index.Connections(session))

	// Tracking again moves the connection to the new session.
	index.Track("second", second, otherSession)
	require.Empty(t, index.Connections(session))
	require.Equal(t, []Connection{second}, index.Connections(otherSession))

	// A zero session removes the connection, and untracking an unknown connection does nothing.
	index.Track("second", second, [16]byte{})
	index.Untrack("unknown")
	require.Equal(t, 0, index.count())
	require.Empty(t, index.Connections(otherSession))
}

func TestRevokerRevokeSessionDisconnectsSessionConnections(t *testing.T) {
	tests := []struct {
		name   string
		userID string
	}{
		{name: "with user", userID: "user-1"},
		{name: "without user", userID: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newRevokeTestNode(t)
			store := NewStore(0)
			revoker := newTestRevoker(t, node, store)
			session, _ := Key(testUUID)
			pending := &fakeConnection{}
			store.SessionConnections().Track("pending", pending, session)

			require.NoError(t, revoker.RevokeSession(tt.userID, testUUID, 0, 0))
			require.Eventually(t, func() bool {
				disconnects, _ := pending.snapshot()
				return len(disconnects) == 1
			}, 5*time.Second, 5*time.Millisecond)
			disconnects, _ := pending.snapshot()
			require.Equal(t, centrifuge.DisconnectExpired, disconnects[0])
		})
	}
}
