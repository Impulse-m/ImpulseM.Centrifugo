package client

import (
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
)

// connState is the state the handler keeps for a connection in its ext state. It holds the rate limit state of the
// connection, and whether the connection is closed. The session index tracks a connection only while it is open, so a
// refresh which overlaps a close does not leave an index entry behind.
type connState struct {
	mu     sync.Mutex
	closed bool
	limits *ratelimit.ConnState
}

// connStateOf returns the state of the connection, nil when the connection has none.
func connStateOf(c Client) *connState {
	st, _ := c.ExtState().(*connState)
	return st
}

// rateLimitStateOf returns the rate limit state of the connection, nil when the connection has none.
func rateLimitStateOf(c Client) *ratelimit.ConnState {
	st := connStateOf(c)
	if st == nil {
		return nil
	}
	return st.limits
}
