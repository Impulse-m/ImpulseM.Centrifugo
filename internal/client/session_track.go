package client

import (
	"hash/maphash"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"

	"github.com/centrifugal/centrifuge"
)

// sessionTrackStripeCount is the number of locks which serialize the refreshes of connections with no record. A
// connection picks its lock by a hash of its ID, so the locks cost a fixed amount per handler and nothing per connection.
const sessionTrackStripeCount = 64

// sessionTrackSeed seeds the hash which picks the stripe of a connection.
var sessionTrackSeed maphash.Seed = maphash.MakeSeed()

// sessionTrackStripes holds the locks of the stripes. The refreshes of one connection take the same lock, so the session
// of each refresh is tracked in the order the refreshes run.
type sessionTrackStripes [sessionTrackStripeCount]sync.Mutex

// sessionTrack is the ext state of a connection with no record which a refresh gave a token session: its membership in
// the session index and nothing else. connStateOf returns nil for it, so the connection keeps every path of a connection
// without a record. IdentityOf reads the identity Core holds, which Core replaces at each refresh.
type sessionTrack struct {
	track accessrevoke.Tracked
}

// sessionTrackOf returns the session track of the connection, nil when the connection has none.
func sessionTrackOf(c rebindableClient) *sessionTrack {
	st, _ := c.ExtState().(*sessionTrack)
	return st
}

// trackRefreshedSession moves a connection with no record into session in the session index. The first session of the
// connection sets its track and its disconnect handler, which releases the track when the connection closes. The track
// is not set for a connection which is closed or not connected: OnDisconnectIfConnected reports that, and nothing
// releases a track which is set without a handler. A zero session takes the connection out of the index and keeps the
// track, so a later session of the connection reuses it.
func (h *Handler) trackRefreshedSession(c rebindableClient, session [16]byte) {
	mu := &h.sessionTracks[sessionTrackStripeOf(c.ID())]
	mu.Lock()
	defer mu.Unlock()
	st := sessionTrackOf(c)
	if st == nil {
		if session == ([16]byte{}) {
			return
		}
		st = &sessionTrack{}
		connected := c.OnDisconnectIfConnected(func(centrifuge.DisconnectEvent) {
			h.accessStore.SessionConnections().Close(c, &st.track)
		})
		if !connected {
			return
		}
		c.SetExtState(st)
	}
	h.accessStore.SessionConnections().Track(c, &st.track, session)
}

// sessionTrackStripeOf returns the stripe of the connection with ID id. maphash.String does not allocate, so picking a
// stripe allocates nothing.
func sessionTrackStripeOf(id string) int {
	return int(maphash.String(sessionTrackSeed, id) % sessionTrackStripeCount)
}
