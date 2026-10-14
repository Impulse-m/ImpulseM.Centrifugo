package accessrevoke

import (
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, 2, index.Len())
	require.Len(t, index.Connections(session), 2)

	index.Untrack("first")
	require.Equal(t, 1, index.Len())
	require.Equal(t, []Connection{second}, index.Connections(session))

	// Tracking again moves the connection to the new session.
	index.Track("second", second, otherSession)
	require.Empty(t, index.Connections(session))
	require.Equal(t, []Connection{second}, index.Connections(otherSession))

	// A zero session removes the connection, and untracking an unknown connection does nothing.
	index.Track("second", second, [16]byte{})
	index.Untrack("unknown")
	require.Equal(t, 0, index.Len())
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
