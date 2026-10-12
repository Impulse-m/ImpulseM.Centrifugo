package accessrevoke

import (
	"github.com/centrifugal/centrifuge"
)

// DisconnectUserBlocked is sent to connections of a blocked user. Code is in the terminal range,
// so the client must not reconnect.
var DisconnectUserBlocked = centrifuge.Disconnect{
	Code:   4500,
	Reason: "user blocked",
}

// Hub finds local connections of a user. Satisfied by *centrifuge.Hub.
type Hub interface {
	UserConnections(userID string) map[string]*centrifuge.Client
}

// Revoker applies revocation events: it updates the Store and then disconnects matching local
// connections through the DisconnectQueue. Only local connections are affected, cluster
// propagation is done by the caller.
type Revoker struct {
	store *Store
	hub   Hub
	queue *DisconnectQueue
}

// NewRevoker creates a Revoker.
func NewRevoker(store *Store, hub Hub, queue *DisconnectQueue) *Revoker {
	return &Revoker{
		store: store,
		hub:   hub,
		queue: queue,
	}
}

// Store returns the Store the Revoker applies events to.
func (r *Revoker) Store() *Store {
	return r.store
}

// BlockUser blocks the user and disconnects all local connections of the user with DisconnectUserBlocked.
func (r *Revoker) BlockUser(userID string, expireAt int64) error {
	err := r.store.BlockUser(userID, expireAt)
	if err != nil {
		return err
	}
	r.disconnectMatching(userID, func(_ centrifuge.TokenIdentity) Reason {
		return ReasonBlocked
	}, DisconnectUserBlocked)
	return nil
}

// UnblockUser removes the block of the user. No connections are disconnected.
func (r *Revoker) UnblockUser(userID string) error {
	return r.store.UnblockUser(userID)
}

// RevokeSession revokes the session and disconnects local connections of userID which have this
// session with centrifuge.DisconnectExpired. Empty userID gives no live disconnects: only connect
// and refresh checks apply then.
func (r *Revoker) RevokeSession(userID string, sessionID string, revokedAt int64, expireAt int64) error {
	err := r.store.RevokeSession(sessionID, revokedAt, expireAt)
	if err != nil {
		return err
	}
	if userID == "" {
		return nil
	}
	sessionKey, _ := Key(sessionID)
	r.disconnectMatching(userID, func(identity centrifuge.TokenIdentity) Reason {
		if identity.Session == sessionKey {
			return ReasonSession
		}
		return ""
	}, centrifuge.DisconnectExpired)
	return nil
}

// InvalidateUser invalidates tokens of the user and disconnects local connections whose tokens are
// rejected by the stored user entry, with centrifuge.DisconnectExpired.
func (r *Revoker) InvalidateUser(userID string, minGeneration uint64, issuedBefore int64, revokedAt int64, expireAt int64) error {
	err := r.store.InvalidateUser(userID, minGeneration, issuedBefore, revokedAt, expireAt)
	if err != nil {
		return err
	}
	userKey, _ := Key(userID)
	entry, ok := r.store.user(userKey, r.store.nowUnix())
	if !ok {
		return nil
	}
	r.disconnectMatching(userID, func(identity centrifuge.TokenIdentity) Reason {
		return userTokensReason(identity, entry)
	}, centrifuge.DisconnectExpired)
	return nil
}

// disconnectMatching enqueues disconnect of every local connection of the user for which reasonOf
// returns a non-empty reason.
func (r *Revoker) disconnectMatching(
	userID string,
	reasonOf func(identity centrifuge.TokenIdentity) Reason,
	disconnect centrifuge.Disconnect,
) {
	for _, client := range r.hub.UserConnections(userID) {
		reason := reasonOf(client.TokenIdentity())
		if reason == "" {
			continue
		}
		r.queue.Enqueue(client, disconnect, reason)
	}
}
