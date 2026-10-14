package accessrevoke

import (
	"fmt"
	"sync"

	"github.com/centrifugal/centrifuge"
)

// DisconnectUserBlocked is sent to connections of a blocked user. Code is in the terminal range,
// so the client must not reconnect.
var DisconnectUserBlocked = centrifuge.Disconnect{
	Code:   4500,
	Reason: "user blocked",
}

// Hub finds local connections. Satisfied by *centrifuge.Hub.
type Hub interface {
	UserConnections(userID string) map[string]*centrifuge.Client
	Connections() map[string]*centrifuge.Client
}

// Revoker applies revocation events: it updates the Store and then disconnects matching local
// connections through the DisconnectQueue. Only local connections are affected, cluster
// propagation is done by the caller.
type Revoker struct {
	store *Store
	hub   Hub
	queue *DisconnectQueue
	// sweep checks all local connections. It is a field so tests can observe and block sweeps.
	sweep func()

	sweepMu      sync.Mutex
	sweeping     bool
	sweepPending bool
}

// NewRevoker creates a Revoker.
func NewRevoker(store *Store, hub Hub, queue *DisconnectQueue) *Revoker {
	revoker := &Revoker{
		store: store,
		hub:   hub,
		queue: queue,
	}
	revoker.sweep = func() {
		// Session is the only revocation kind changed by revoke_token, so only it is checked.
		SweepConnections(hub, store, queue, CheckOptions{Session: true})
	}
	return revoker
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
// session with centrifuge.DisconnectExpired. Connections without a user which have this session are
// disconnected through the session index, with or without userID. Empty userID runs a sweep of all
// local connections in the background, which disconnects every connection with this session. The sweep
// is O(local connections), so passing userID is strongly preferred at scale.
func (r *Revoker) RevokeSession(userID string, sessionID string, revokedAt int64, expireAt int64) error {
	err := r.store.RevokeSession(sessionID, revokedAt, expireAt)
	if err != nil {
		return err
	}
	sessionKey, _ := Key(sessionID)
	r.disconnectSessionConnections(sessionKey)
	if userID == "" {
		r.requestSweep()
		return nil
	}
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

// Apply applies the notification to local state. It is used both for operations received through
// the API and for notifications from other nodes. Only local connections are disconnected.
func (r *Revoker) Apply(notification Notification) error {
	switch notification.Kind {
	case KindBlockUser:
		return r.BlockUser(notification.User, notification.ExpireAt)
	case KindUnblockUser:
		return r.UnblockUser(notification.User)
	case KindRevokeToken:
		return r.RevokeSession(notification.User, notification.Session, notification.RevokedAt, notification.ExpireAt)
	case KindInvalidateUserTokens:
		return r.InvalidateUser(
			notification.User,
			notification.Generation,
			notification.IssuedBefore,
			notification.RevokedAt,
			notification.ExpireAt,
		)
	default:
		return fmt.Errorf("%w: %q", ErrUnknownKind, notification.Kind)
	}
}

// disconnectMatching enqueues disconnect of every local connection of the user for which reasonOf
// returns a non-empty reason.
func (r *Revoker) disconnectMatching(
	userID string,
	reasonOf func(identity centrifuge.TokenIdentity) Reason,
	disconnect centrifuge.Disconnect,
) {
	for _, client := range r.hub.UserConnections(userID) {
		reason := reasonOf(IdentityOf(client))
		if reason == "" {
			continue
		}
		r.queue.Enqueue(client, disconnect, reason)
	}
}

// disconnectSessionConnections enqueues disconnect of the indexed connections of session. Such connections
// have no user, so disconnectMatching does not find them.
func (r *Revoker) disconnectSessionConnections(session [16]byte) {
	for _, conn := range r.store.SessionConnections().Connections(session) {
		r.queue.Enqueue(conn, centrifuge.DisconnectExpired, ReasonSession)
	}
}

// requestSweep starts a sweep in the background. If a sweep is already running, it marks one more
// sweep to run after the current one, so revocations applied during the run are not missed, and
// no more than one sweep is pending at a time.
func (r *Revoker) requestSweep() {
	r.sweepMu.Lock()
	if r.sweeping {
		r.sweepPending = true
		r.sweepMu.Unlock()
		return
	}
	r.sweeping = true
	r.sweepMu.Unlock()
	go r.runSweeps()
}

// runSweeps runs sweeps until no sweep is pending.
func (r *Revoker) runSweeps() {
	for {
		r.sweep()
		r.sweepMu.Lock()
		if !r.sweepPending {
			r.sweeping = false
			r.sweepMu.Unlock()
			return
		}
		r.sweepPending = false
		r.sweepMu.Unlock()
	}
}
