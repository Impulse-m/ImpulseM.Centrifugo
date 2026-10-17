package accessrevoke

import (
	"fmt"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/metrics"

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
	queue.setRetrier(revoker.retry)
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
// session. Connections without a user which have this session are disconnected through the session
// index, with or without userID. Empty userID also runs a sweep of all local connections in the
// background, which disconnects the connections with this session that the index does not hold. The
// sweep is O(local connections), so passing userID is strongly preferred at scale. A disconnect the
// queue drops is retried by its user or session once the queue has room. Every disconnect closes with
// the disconnect the session is revoked with, see Store.RevokeSession, or with centrifuge.DisconnectExpired
// when it has none.
func (r *Revoker) RevokeSession(
	userID string,
	sessionID string,
	revokedAt int64,
	expireAt int64,
	disconnect *Disconnect,
) error {
	err := r.store.RevokeSession(sessionID, revokedAt, expireAt, disconnect)
	if err != nil {
		return err
	}
	sessionKey, _ := Key(sessionID)
	closeConn := closeWith(r.store.sessionDisconnect(sessionKey), centrifuge.DisconnectExpired)
	r.disconnectSessionConnections(sessionKey, closeConn)
	if userID == "" {
		// The sweep also finds the indexed connections, and a disconnect is idempotent, so a connection hit by both is
		// disconnected once in effect.
		r.requestSweep()
		return nil
	}
	r.disconnectMatching(userID, func(identity centrifuge.TokenIdentity) Reason {
		if identity.Session == sessionKey {
			return ReasonSession
		}
		return ""
	}, closeConn)
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
		return r.RevokeSession(
			notification.User,
			notification.Session,
			notification.RevokedAt,
			notification.ExpireAt,
			notification.Disconnect,
		)
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
// returns a non-empty reason. A dropped disconnect is kept under the user for a retry.
func (r *Revoker) disconnectMatching(
	userID string,
	reasonOf func(identity centrifuge.TokenIdentity) Reason,
	disconnect centrifuge.Disconnect,
) {
	key := retryKey{user: userID}
	for _, client := range r.hub.UserConnections(userID) {
		_, identity := IdentityOf(client)
		reason := reasonOf(identity)
		if reason == "" {
			continue
		}
		r.queue.enqueueKeyed(key, client, disconnect, reason)
	}
}

// disconnectSessionConnections enqueues disconnect of the indexed connections of session. Such connections
// may have no user, so disconnectMatching does not find them all. A dropped disconnect is kept under the
// session for a retry.
func (r *Revoker) disconnectSessionConnections(session [16]byte, disconnect centrifuge.Disconnect) {
	key := retryKey{session: session}
	for _, conn := range r.store.SessionConnections().Connections(session) {
		r.queue.enqueueKeyed(key, conn, disconnect, ReasonSession)
	}
}

// retry re-evaluates a key of dropped disconnects on the queue goroutine. The store is read again, so a connection is
// disconnected only when the store still rejects it, and an entry which expired is not applied.
func (r *Revoker) retry(key retryKey) {
	if key.user != "" {
		metrics.AccessRevokeDisconnectRetriesTotal.WithLabelValues(retryKindUser).Inc()
		// Every kind is checked, so a retry applies whatever the store still holds for the user.
		opts := CheckOptions{Block: true, Session: true, UserTokens: true}
		for _, client := range r.hub.UserConnections(key.user) {
			// A queued or closing connection needs no check: the queue holds it already, and a check would count it
			// as a reject again on every pass.
			if r.queue.pending(client) {
				continue
			}
			disconnect, reason, revoked := CheckConnection(r.store, client, opts, stageRetry)
			if revoked {
				r.queue.enqueueKeyed(key, client, disconnect, reason)
			}
		}
		return
	}
	metrics.AccessRevokeDisconnectRetriesTotal.WithLabelValues(retryKindSession).Inc()
	_, ok := r.store.session(key.session, r.store.nowUnix())
	if !ok {
		return
	}
	closeConn := closeWith(r.store.sessionDisconnect(key.session), centrifuge.DisconnectExpired)
	r.disconnectSessionConnections(key.session, closeConn)
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
