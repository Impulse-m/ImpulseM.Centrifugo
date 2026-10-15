package accessrevoke

import (
	"hash/maphash"
	"sync"
)

// indexShards is the number of shards of SessionConnections. It is a power of two, so a shard is picked with a mask.
const indexShards = 64

// Tracked is the part of a connection's state which the session index owns. The server embeds it in the record it
// attaches to a connection, so the index finds the session of the connection without a lookup. mu orders a track
// against a close of the same connection. The zero value is a connection which is not indexed.
type Tracked struct {
	mu      sync.Mutex
	closed  bool
	session [16]byte // the session the connection is indexed under; zero when it is not indexed
}

// Closed reports whether the connection of t is closed: it has left the index for good.
func (t *Tracked) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// SessionConnections indexes local connections which carry a token session, whether or not they have a user. Such a
// connection is not in the user index of the hub, so session revocation reaches it only through this index. A
// connection is in the index while it has a session, and leaves it when it loses the session, gets closed or
// disconnects.
//
// The index is sharded by session. Each shard holds the sessions which hash to it in two maps: one for the sessions
// with exactly one connection, and more for the sessions with two or more, keyed by connection ID. A lookup checks
// more and then one, so Connections is one or two map lookups in one shard. Track holds the lock of the connection's
// Tracked while it moves the connection between two shards, one shard at a time, so no operation holds two shard locks.
//
// Measured on a 16-thread Windows machine (Intel Core i5-12600K, go1.26 windows/amd64, shared with other work): 67.8 to
// 67.9 bytes retained per indexed connection with 1M connections, one session each, after a GC
// (TestSessionConnectionsRetainedHeap). BenchmarkSessionConnectionsTrackUntrackParallel runs at 58 to 425 ns/op with 0
// allocs/op, the spread being the load of the machine.
type SessionConnections struct {
	seed   maphash.Seed
	shards [indexShards]sessionShard
}

// sessionShard holds the sessions of one shard: one connection in one, and two or more in more.
type sessionShard struct {
	mu   sync.Mutex
	one  map[[16]byte]Connection
	more map[[16]byte]map[string]Connection
}

// newSessionConnections creates an empty index. The maps of the shards are made on the first connection of a shard,
// so an index which holds nothing allocates nothing.
func newSessionConnections() *SessionConnections {
	return &SessionConnections{seed: maphash.MakeSeed()}
}

// Track moves the connection into session. A zero session removes it. Track does nothing once the connection is
// closed. Track is called on connect, and on every refresh and rebind of a connection which has no user.
func (s *SessionConnections) Track(conn Connection, t *Tracked, session [16]byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.session == session {
		return
	}
	s.leave(conn, t)
	if session == ([16]byte{}) {
		return
	}
	s.shardOf(session).add(session, conn)
	t.session = session
}

// Untrack removes the connection from the index. The connection can be tracked again.
func (s *SessionConnections) Untrack(conn Connection, t *Tracked) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s.leave(conn, t)
}

// Close removes the connection from the index and marks it closed, so a later Track does nothing. It is called on
// disconnect.
func (s *SessionConnections) Close(conn Connection, t *Tracked) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	s.leave(conn, t)
}

// Connections returns the indexed connections of session. It is one lookup in one shard, and a second lookup only
// for a session with one connection.
func (s *SessionConnections) Connections(session [16]byte) []Connection {
	shard := s.shardOf(session)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if members, ok := shard.more[session]; ok {
		out := make([]Connection, 0, len(members))
		for _, conn := range members {
			out = append(out, conn)
		}
		return out
	}
	if conn, ok := shard.one[session]; ok {
		return []Connection{conn}
	}
	return nil
}

// shardOf returns the shard of session. The hash is taken over all bytes, because session IDs of different versions
// share their first byte.
func (s *SessionConnections) shardOf(session [16]byte) *sessionShard {
	return &s.shards[maphash.Bytes(s.seed, session[:])&(indexShards-1)]
}

// leave removes the connection from the session t is indexed under, and clears the session. The caller holds t.mu.
func (s *SessionConnections) leave(conn Connection, t *Tracked) {
	if t.session == ([16]byte{}) {
		return
	}
	s.shardOf(t.session).remove(t.session, conn)
	t.session = [16]byte{}
}

// add puts conn into session. The caller holds no lock; add takes the shard lock.
func (shard *sessionShard) add(session [16]byte, conn Connection) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if members, ok := shard.more[session]; ok {
		members[conn.ID()] = conn
		return
	}
	first, ok := shard.one[session]
	if !ok {
		if shard.one == nil {
			shard.one = make(map[[16]byte]Connection)
		}
		shard.one[session] = conn
		return
	}
	if shard.more == nil {
		shard.more = make(map[[16]byte]map[string]Connection)
	}
	shard.more[session] = map[string]Connection{first.ID(): first, conn.ID(): conn}
	delete(shard.one, session)
}

// remove takes conn out of session. The caller holds no lock; remove takes the shard lock. When one of two connections
// goes, the other moves back to the one map.
func (shard *sessionShard) remove(session [16]byte, conn Connection) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if members, ok := shard.more[session]; ok {
		delete(members, conn.ID())
		if len(members) == 1 {
			if shard.one == nil {
				shard.one = make(map[[16]byte]Connection)
			}
			for _, last := range members {
				shard.one[session] = last
			}
			delete(shard.more, session)
		}
		return
	}
	if first, ok := shard.one[session]; ok && first.ID() == conn.ID() {
		delete(shard.one, session)
	}
}
