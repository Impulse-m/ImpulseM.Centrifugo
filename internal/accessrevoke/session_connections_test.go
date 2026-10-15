package accessrevoke

import (
	"crypto/sha256"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// count returns the number of indexed connections, for the tests only.
func (s *SessionConnections) count() int {
	total := 0
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		total += len(shard.one)
		for _, members := range shard.more {
			total += len(members)
		}
		shard.mu.Unlock()
	}
	return total
}

// sessionIDForTest returns a UUID session ID for the test sessions with index i.
func sessionIDForTest(i int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
}

// sessionKeyForTest returns a session key for the test index i: the first 16 bytes of its SHA-256. The keys spread
// over the shards. Callers precompute them outside measured loops.
func sessionKeyForTest(i int) [16]byte {
	sum := sha256.Sum256([]byte(strconv.Itoa(i)))
	var key [16]byte
	copy(key[:], sum[:16])
	return key
}

// heapInUse returns the heap in use after two collections.
func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapInuse
}

// TestSessionShardsSpread checks that the keys of the test sessions land in many shards.
func TestSessionShardsSpread(t *testing.T) {
	index := newSessionConnections()
	seen := make(map[*sessionShard]struct{})
	for i := 0; i < 1000; i++ {
		seen[index.shardOf(sessionKeyForTest(i))] = struct{}{}
	}
	require.GreaterOrEqual(t, len(seen), 48)
}

// TestSessionConnectionsConcurrentTrackUntrackRevoke runs track, untrack, close, revoke and reads of the index at the
// same time. Every connection leaves the index at the end, and nothing is lost in between.
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
			conn := &fakeConnection{id: fmt.Sprintf("conn-%d", w)}
			var tracked Tracked
			for r := 0; r < rounds; r++ {
				index.Track(conn, &tracked, sessionKeyForTest(r%4))
				if r%3 == 0 {
					index.Untrack(conn, &tracked)
				}
			}
			index.Untrack(conn, &tracked)
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
// its own connection and record, over precomputed session keys spread over the shards.
func BenchmarkSessionConnectionsTrackUntrackParallel(b *testing.B) {
	index := newSessionConnections()
	keys := make([][16]byte, 64)
	for i := range keys {
		keys[i] = sessionKeyForTest(i)
	}
	var seq atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		conn := &fakeConnection{id: strconv.FormatInt(seq.Add(1), 10)}
		var tracked Tracked
		var n int
		for pb.Next() {
			index.Track(conn, &tracked, keys[n%len(keys)])
			index.Untrack(conn, &tracked)
			n++
		}
	})
}

// TestSessionConnectionsRetainedHeap tracks one million connections with distinct sessions and checks the heap they
// retain in the index. The connections and their Tracked are allocated before the first reading, so they are not
// counted.
func TestSessionConnectionsRetainedHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("tracks one million connections")
	}
	const count = 1000000
	conns := make([]*fakeConnection, count)
	tracked := make([]Tracked, count)
	keys := make([][16]byte, count)
	for i := range conns {
		conns[i] = &fakeConnection{id: strconv.Itoa(i)}
		keys[i] = sessionKeyForTest(i)
	}
	index := newSessionConnections()
	before := heapInUse()
	for i := range conns {
		index.Track(conns[i], &tracked[i], keys[i])
	}
	after := heapInUse()
	perConnection := float64(after-before) / count
	t.Logf("retained bytes per indexed connection: %.1f", perConnection)
	require.LessOrEqual(t, perConnection, 72.0)
	runtime.KeepAlive(conns)
	runtime.KeepAlive(tracked)
	runtime.KeepAlive(index)
}

func TestSessionConnectionsTrackAndUntrack(t *testing.T) {
	index := newSessionConnections()
	session, _ := Key(testUUID)
	otherSession, _ := Key(testOtherUUID)
	if otherSession == session {
		t.Fatal("test sessions must differ")
	}
	first := &fakeConnection{id: "first"}
	second := &fakeConnection{id: "second"}
	var firstTracked Tracked
	var secondTracked Tracked

	index.Track(first, &firstTracked, session)
	index.Track(second, &secondTracked, session)
	require.Equal(t, 2, index.count())
	require.Len(t, index.Connections(session), 2)

	index.Untrack(first, &firstTracked)
	require.Equal(t, 1, index.count())
	require.Equal(t, []Connection{second}, index.Connections(session))

	// Tracking again moves the connection to the new session.
	index.Track(second, &secondTracked, otherSession)
	require.Empty(t, index.Connections(session))
	require.Equal(t, []Connection{second}, index.Connections(otherSession))

	// A zero session removes the connection, and untracking an untracked connection does nothing.
	index.Track(second, &secondTracked, [16]byte{})
	index.Untrack(first, &firstTracked)
	require.Equal(t, 0, index.count())
	require.Empty(t, index.Connections(otherSession))
}

// TestSessionConnectionsTwoConnectionsMoveBack checks that a session with two connections is held in the map of
// several connections and moves back to the single map when one of them leaves.
func TestSessionConnectionsTwoConnectionsMoveBack(t *testing.T) {
	index := newSessionConnections()
	session := sessionKeyForTest(7)
	first := &fakeConnection{id: "first"}
	second := &fakeConnection{id: "second"}
	var firstTracked Tracked
	var secondTracked Tracked

	index.Track(first, &firstTracked, session)
	index.Track(second, &secondTracked, session)
	shard := index.shardOf(session)
	require.Contains(t, shard.more, session)
	require.NotContains(t, shard.one, session)
	require.ElementsMatch(t, []Connection{first, second}, index.Connections(session))

	index.Close(first, &firstTracked)
	require.NotContains(t, shard.more, session)
	require.Equal(t, []Connection{second}, index.Connections(session))
	require.Equal(t, second, shard.one[session])

	index.Untrack(second, &secondTracked)
	require.Empty(t, index.Connections(session))
	require.NotContains(t, shard.one, session)
}

// TestSessionConnectionsClosedIsNotTracked checks that a closed connection is removed and a later track does nothing.
func TestSessionConnectionsClosedIsNotTracked(t *testing.T) {
	index := newSessionConnections()
	session := sessionKeyForTest(3)
	conn := &fakeConnection{id: "closed"}
	var tracked Tracked

	index.Track(conn, &tracked, session)
	index.Close(conn, &tracked)
	require.Equal(t, 0, index.count())
	index.Track(conn, &tracked, session)
	require.Equal(t, 0, index.count())
}

func TestRevokerRevokeSessionDisconnectsSessionConnections(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(0)
	revoker := newTestRevoker(t, node, store)
	session, _ := Key(testUUID)
	pending := &fakeConnection{id: "pending"}
	var tracked Tracked
	store.SessionConnections().Track(pending, &tracked, session)

	require.NoError(t, revoker.RevokeSession("user-1", testUUID, 0, 0))
	require.Eventually(t, func() bool {
		disconnects, _ := pending.snapshot()
		return len(disconnects) == 1
	}, 5*time.Second, 5*time.Millisecond)
	disconnects, _ := pending.snapshot()
	require.Equal(t, centrifuge.DisconnectExpired, disconnects[0])
}

// TestRevokerRevokeSessionWithoutUserLeavesIndexToSweep revokes the session of an indexed connection with an empty user.
// The sweep checks every local connection, the indexed ones too, so the revoker enqueues nothing for the index and only
// requests the sweep.
func TestRevokerRevokeSessionWithoutUserLeavesIndexToSweep(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(0)
	revoker := newTestRevoker(t, node, store)
	swept := make(chan struct{}, 1)
	revoker.sweep = func() {
		select {
		case swept <- struct{}{}:
		default:
		}
	}
	session, _ := Key(testUUID)
	indexed := &fakeConnection{id: "indexed"}
	var tracked Tracked
	store.SessionConnections().Track(indexed, &tracked, session)

	require.NoError(t, revoker.RevokeSession("", testUUID, 0, 0))
	select {
	case <-swept:
	case <-time.After(5 * time.Second):
		t.Fatal("an empty user did not request the sweep")
	}
	require.Never(t, func() bool {
		disconnects, _ := indexed.snapshot()
		return len(disconnects) > 0
	}, 100*time.Millisecond, 5*time.Millisecond)
}
