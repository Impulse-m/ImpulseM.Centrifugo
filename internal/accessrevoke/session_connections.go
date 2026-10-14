package accessrevoke

import (
	"hash/maphash"
	"sync"
)

// indexShards is the number of shards of each array of SessionConnections. It is a power of two, so a shard is picked
// with a mask.
const indexShards = 64

// SessionConnections indexes local connections which have no user and carry a token session. Such a connection is
// not in the user index of the hub, so session revocation reaches it only through this index. It holds every
// user-less connection with a token session, anonymous ones included. A connection leaves the index when it gets a
// user or disconnects.
//
// The index is sharded: the session of a connection is found by its ID, so the IDs are sharded by hash, and the
// connections are sharded by session key. An operation holds the lock of the ID shard while it updates the session
// shard. Nothing takes the locks in the other order, so the two kinds of shard cannot deadlock.
type SessionConnections struct {
	seed          maphash.Seed
	idShards      [indexShards]connectionIDShard
	sessionShards [indexShards]sessionConnectionShard
}

// connectionIDShard holds the session of each connection indexed under its ID. The session is kept here and not read
// from the connection: a rebind sets the identity of the connection before the index moves, and Core sets the identity
// of a refresh only when its reply has an expiry, so the connection does not always hold the indexed session.
// BenchmarkSessionConnectionsTrackUntrackParallel runs at 1256 ns/op, 64 B/op and 2 allocs/op with the flat map of the
// session shards, and at 14521 ns/op, 364 B/op and 3 allocs/op with a map per session. Both were measured on a 16-thread
// Windows machine.
type connectionIDShard struct {
	mu        sync.Mutex
	sessionOf map[string][16]byte
}

// sessionConnectionShard holds the connections of its sessions in one map, keyed by session and connection ID. The
// map holds no per-session map: an anonymous token session has one connection, so a map per session costs more than
// the connection it holds. A session is found by scanning its shard, so a revocation costs one shard, which is 1/64
// of the index. The retained heap is 451.6 bytes per connection with a map per session, and 177.0 bytes per connection
// with this flat map. Both were measured with 1M connections, one session each, after a GC.
type sessionConnectionShard struct {
	mu    sync.Mutex
	conns map[sessionConnKey]Connection
}

// sessionConnKey is the key of a connection in a session shard.
type sessionConnKey struct {
	session [16]byte
	id      string
}

// newSessionConnections creates an empty index.
func newSessionConnections() *SessionConnections {
	s := &SessionConnections{seed: maphash.MakeSeed()}
	for i := range s.idShards {
		s.idShards[i].sessionOf = make(map[string][16]byte)
	}
	for i := range s.sessionShards {
		s.sessionShards[i].conns = make(map[sessionConnKey]Connection)
	}
	return s
}

// Track sets the session of the connection with id to session, and adds the connection to the index. A zero
// session removes it. Track is called on connect, and on every refresh and rebind of a connection without a user.
func (s *SessionConnections) Track(id string, conn Connection, session [16]byte) {
	shard := s.idShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if old, ok := shard.sessionOf[id]; ok {
		s.removeFromSession(old, id)
		delete(shard.sessionOf, id)
	}
	if session == ([16]byte{}) {
		return
	}
	shard.sessionOf[id] = session
	s.addToSession(session, id, conn)
}

// Untrack removes the connection with id from the index. It is called on disconnect and when the connection
// gets a user.
func (s *SessionConnections) Untrack(id string) {
	shard := s.idShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	old, ok := shard.sessionOf[id]
	if !ok {
		return
	}
	delete(shard.sessionOf, id)
	s.removeFromSession(old, id)
}

// Connections returns the indexed connections of session. It scans the session shard, see sessionConnectionShard.
func (s *SessionConnections) Connections(session [16]byte) []Connection {
	shard := s.sessionShard(session)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	var out []Connection
	for key, conn := range shard.conns {
		if key.session == session {
			out = append(out, conn)
		}
	}
	return out
}

// idShard returns the shard of the connection ID.
func (s *SessionConnections) idShard(id string) *connectionIDShard {
	return &s.idShards[maphash.String(s.seed, id)&(indexShards-1)]
}

// sessionShard returns the shard of the session key.
func (s *SessionConnections) sessionShard(session [16]byte) *sessionConnectionShard {
	return &s.sessionShards[session[0]&(indexShards-1)]
}

// addToSession adds the connection with id to session. The caller holds the ID shard lock.
func (s *SessionConnections) addToSession(session [16]byte, id string, conn Connection) {
	shard := s.sessionShard(session)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.conns[sessionConnKey{session: session, id: id}] = conn
}

// removeFromSession removes the connection with id from session. The caller holds the ID shard lock.
func (s *SessionConnections) removeFromSession(session [16]byte, id string) {
	shard := s.sessionShard(session)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.conns, sessionConnKey{session: session, id: id})
}
