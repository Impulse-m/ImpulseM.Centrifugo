package accessrevoke

import (
	"sync"
)

// SessionConnections indexes local connections which have no user by the session of their token. The
// user index of the hub does not hold such connections, so session revocation reaches them only through
// this index. Only connections with a token session are kept, which in practice are the pending ones:
// they are short-lived, so the index stays small.
type SessionConnections struct {
	mu sync.Mutex
	// bySession holds the connections of each session, by connection ID.
	bySession map[[16]byte]map[string]Connection
	// sessionOf holds the session each indexed connection is stored under.
	sessionOf map[string][16]byte
}

// newSessionConnections creates an empty index.
func newSessionConnections() *SessionConnections {
	return &SessionConnections{
		bySession: make(map[[16]byte]map[string]Connection),
		sessionOf: make(map[string][16]byte),
	}
}

// Track sets the session of the connection with id to session. A zero session removes the connection
// from the index. Track is called on connect, and on every refresh and rebind of a connection without a user.
func (s *SessionConnections) Track(id string, conn Connection, session [16]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.untrackLocked(id)
	if session == ([16]byte{}) {
		return
	}
	connections, ok := s.bySession[session]
	if !ok {
		connections = make(map[string]Connection)
		s.bySession[session] = connections
	}
	connections[id] = conn
	s.sessionOf[id] = session
}

// Untrack removes the connection with id from the index. It is called on disconnect and when the
// connection gets a user.
func (s *SessionConnections) Untrack(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.untrackLocked(id)
}

// Connections returns the indexed connections of session.
func (s *SessionConnections) Connections(session [16]byte) []Connection {
	s.mu.Lock()
	defer s.mu.Unlock()
	connections := s.bySession[session]
	out := make([]Connection, 0, len(connections))
	for _, conn := range connections {
		out = append(out, conn)
	}
	return out
}

// Len returns the number of indexed connections.
func (s *SessionConnections) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessionOf)
}

// untrackLocked removes the connection with id from the index. Lock must be held.
func (s *SessionConnections) untrackLocked(id string) {
	session, ok := s.sessionOf[id]
	if !ok {
		return
	}
	delete(s.sessionOf, id)
	connections := s.bySession[session]
	delete(connections, id)
	if len(connections) == 0 {
		delete(s.bySession, session)
	}
}
