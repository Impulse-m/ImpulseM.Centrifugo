package accessrevoke

import (
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// closeRecorder records the close of every client connected to the node: the code and reason of the disconnect.
type closeRecorder struct {
	mu     sync.Mutex
	closes map[string]centrifuge.Disconnect
}

// newCloseRecorder records the closes of the clients of node. It must be created before the clients connect.
func newCloseRecorder(node *centrifuge.Node) *closeRecorder {
	recorder := &closeRecorder{closes: make(map[string]centrifuge.Disconnect)}
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnDisconnect(func(event centrifuge.DisconnectEvent) {
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			recorder.closes[client.ID()] = event.Disconnect
		})
	})
	return recorder
}

// closeOf returns the recorded close of the client with clientID.
func (r *closeRecorder) closeOf(clientID string) (centrifuge.Disconnect, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	disconnect, ok := r.closes[clientID]
	return disconnect, ok
}

// waitClose waits until the client with clientID is closed and returns its close.
func (r *closeRecorder) waitClose(t *testing.T, clientID string) centrifuge.Disconnect {
	t.Helper()
	var disconnect centrifuge.Disconnect
	require.Eventually(t, func() bool {
		var ok bool
		disconnect, ok = r.closeOf(clientID)
		return ok
	}, 5*time.Second, 5*time.Millisecond)
	return disconnect
}

// clientsOf returns the connected clients of the node with the session as their token session.
func clientsOf(t *testing.T, node *centrifuge.Node, userID string, session string) []*centrifuge.Client {
	t.Helper()
	sessionKey, _ := Key(session)
	var clients []*centrifuge.Client
	for _, client := range node.Hub().UserConnections(userID) {
		if client.TokenIdentity().Session == sessionKey {
			clients = append(clients, client)
		}
	}
	return clients
}

func TestStoreRevokeSessionKeepsDisconnect(t *testing.T) {
	store := NewStore(time.Minute)
	disconnect := &Disconnect{Code: 4001, Reason: "session revoked"}
	identity := Identity(testUUID, 0, 100)

	require.NoError(t, store.RevokeSession(testUUID, 1000, 0, disconnect))
	result := store.Check("", identity, allAccessChecks)
	require.Equal(t, Revoked, result.Verdict)
	require.Equal(t, disconnect, result.Disconnect)

	// The stored disconnect is a copy, so changing the request afterwards does not change it.
	disconnect.Code = 4002
	result = store.Check("", identity, allAccessChecks)
	require.Equal(t, &Disconnect{Code: 4001, Reason: "session revoked"}, result.Disconnect)

	// A repeated revocation without a disconnect keeps the stored one.
	require.NoError(t, store.RevokeSession(testUUID, 1000, 0, nil))
	require.Equal(t, &Disconnect{Code: 4001, Reason: "session revoked"}, store.sessionDisconnect(mustKey(t, testUUID)))

	// A repeated revocation with a disconnect replaces the stored one.
	require.NoError(t, store.RevokeSession(testUUID, 1000, 0, &Disconnect{Code: 4500, Reason: "other"}))
	require.Equal(t, &Disconnect{Code: 4500, Reason: "other"}, store.sessionDisconnect(mustKey(t, testUUID)))
}

// mustKey returns the key of the identifier.
func mustKey(t *testing.T, id string) [16]byte {
	t.Helper()
	key, ok := Key(id)
	require.True(t, ok)
	return key
}

func TestStoreRevokeSessionWithoutDisconnectHasNone(t *testing.T) {
	store := NewStore(time.Minute)
	require.NoError(t, store.RevokeSession(testUUID, 1000, 0, nil))

	result := store.Check("", Identity(testUUID, 0, 100), allAccessChecks)
	require.Equal(t, Revoked, result.Verdict)
	require.Nil(t, result.Disconnect)
	require.Nil(t, store.sessionDisconnect(mustKey(t, testOtherUUID)))
}

func TestStoreCheckLoopGuardHasNoDisconnect(t *testing.T) {
	store := NewStore(time.Minute)
	require.NoError(t, store.RevokeSession(testUUID, 1000, 0, &Disconnect{Code: 4001, Reason: "r"}))

	result := store.Check("", Identity(testUUID, 0, 2000), allAccessChecks)
	require.Equal(t, LoopGuard, result.Verdict)
	require.Nil(t, result.Disconnect)
}

func TestRevokerRevokeSessionClosesMatchingWithDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	recorder := newCloseRecorder(node)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	connectTestClient(t, node, "user-1|"+testOtherUUID+"|0|100")
	waitConnections(t, node, "user-1", 2)

	matching := clientsOf(t, node, "user-1", testUUID)
	require.Len(t, matching, 1)
	disconnect := &Disconnect{Code: 4001, Reason: "session revoked"}
	require.NoError(t, revoker.RevokeSession("user-1", testUUID, 0, 0, disconnect))

	require.Equal(t, centrifuge.Disconnect{Code: 4001, Reason: "session revoked"}, recorder.waitClose(t, matching[0].ID()))
}

func TestRevokerRevokeSessionClosesWithDefaultWithoutDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	recorder := newCloseRecorder(node)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)

	matching := clientsOf(t, node, "user-1", testUUID)
	require.Len(t, matching, 1)
	require.NoError(t, revoker.RevokeSession("user-1", testUUID, 0, 0, nil))

	require.Equal(t, centrifuge.DisconnectExpired, recorder.waitClose(t, matching[0].ID()))
}

func TestRevokerRevokeSessionIndexClosesWithDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	recorder := newCloseRecorder(node)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	// The connection has no user, so only the session index reaches it.
	connectTestClient(t, node, "|"+testUUID+"|0|100")
	require.Eventually(t, func() bool {
		return len(node.Hub().Connections()) == 1
	}, 5*time.Second, 5*time.Millisecond)
	var client *centrifuge.Client
	for _, c := range node.Hub().Connections() {
		client = c
	}
	tracked := &Tracked{}
	sessionKey, _ := Key(testUUID)
	store.SessionConnections().Track(client, tracked, sessionKey)

	require.NoError(t, revoker.RevokeSession("", testUUID, 0, 0, &Disconnect{Code: 4002, Reason: "gone"}))

	require.Equal(t, centrifuge.Disconnect{Code: 4002, Reason: "gone"}, recorder.waitClose(t, client.ID()))
}

func TestSweepConnectionsClosesWithStoredDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	recorder := newCloseRecorder(node)
	store := NewStore(time.Minute)
	queue := NewDisconnectQueue(10000, 100)
	cancel, errCh := runQueue(t, queue)
	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)

	matching := clientsOf(t, node, "user-1", testUUID)
	require.Len(t, matching, 1)
	require.NoError(t, store.RevokeSession(testUUID, 0, 0, &Disconnect{Code: 4003, Reason: "swept"}))
	rejected := SweepConnections(node.Hub(), store, queue, allAccessChecks)
	require.Equal(t, 1, rejected)

	require.Equal(t, centrifuge.Disconnect{Code: 4003, Reason: "swept"}, recorder.waitClose(t, matching[0].ID()))
}

func TestCheckConnectionUsesStoredDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	store := NewStore(time.Minute)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)
	require.NoError(t, store.RevokeSession(testUUID, 0, 0, &Disconnect{Code: 4004, Reason: "checked"}))

	var client *centrifuge.Client
	for _, c := range node.Hub().UserConnections("user-1") {
		client = c
	}
	disconnect, reason, revoked := CheckConnection(store, client, allAccessChecks, stageSweep)
	require.True(t, revoked)
	require.Equal(t, ReasonSession, reason)
	require.Equal(t, centrifuge.Disconnect{Code: 4004, Reason: "checked"}, disconnect)
}

func TestRevokerApplyKeepsNotificationDisconnect(t *testing.T) {
	node := newRevokeTestNode(t)
	recorder := newCloseRecorder(node)
	store := NewStore(time.Minute)
	revoker := newTestRevoker(t, node, store)

	connectTestClient(t, node, "user-1|"+testUUID+"|0|100")
	waitConnections(t, node, "user-1", 1)

	matching := clientsOf(t, node, "user-1", testUUID)
	require.Len(t, matching, 1)
	require.NoError(t, revoker.Apply(Notification{
		Kind:       KindRevokeToken,
		User:       "user-1",
		Session:    testUUID,
		RevokedAt:  1000,
		ExpireAt:   time.Now().Add(time.Minute).Unix(),
		Disconnect: &Disconnect{Code: 4005, Reason: "applied"},
	}))

	require.Equal(t, centrifuge.Disconnect{Code: 4005, Reason: "applied"}, recorder.waitClose(t, matching[0].ID()))
}
