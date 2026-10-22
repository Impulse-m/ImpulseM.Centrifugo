package accessrevoke

import (
	"fmt"
	"time"

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
	store  *Store
	hub    Hub
	queue  *DisconnectQueue
	maxTTL func() time.Duration
}

// NewRevokerWithMaxTTL creates a Revoker which clamps the expire_at of every notification it applies to the limit
// maxTTL returns, read at each apply, so a config reload applies. A nil maxTTL means no limit.
func NewRevokerWithMaxTTL(store *Store, hub Hub, queue *DisconnectQueue, maxTTL func() time.Duration) *Revoker {
	revoker := &Revoker{
		store:  store,
		hub:    hub,
		queue:  queue,
		maxTTL: maxTTL,
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

// RevokeSession revokes the session and disconnects local connections which have this session. Connections
// with no user are disconnected through the session index, with or without userID. Connections of userID are
// disconnected through the user index of the hub, so only a call which names the user reaches them at once; a
// connection of a user which is passed no user is refused at its next refresh. Nothing scans the local
// connections. A disconnect the queue drops is retried by its user or session once the queue has room. Every
// disconnect closes with the disconnect the session is revoked with, see Store.RevokeSession, or with
// centrifuge.DisconnectExpired when it has none.
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
	r.disconnectSessionConnections(sessionKey, closeConn, false)
	if userID == "" {
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
// the API and for notifications from other nodes. Only local connections are disconnected. The expire_at is clamped
// to the max TTL of the revoker first, so a notification from a node which has no limit cannot exceed this node's.
func (r *Revoker) Apply(notification Notification) error {
	if r.maxTTL != nil {
		notification = ClampExpireAt(notification, time.Now(), r.maxTTL(), ClampSourceNotify)
	}
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
// session for a retry. A retry pass passes retrying, so its drops are not counted again.
func (r *Revoker) disconnectSessionConnections(session [16]byte, disconnect centrifuge.Disconnect, retrying bool) {
	key := retryKey{session: session}
	for _, conn := range r.store.SessionConnections().Connections(session) {
		if retrying {
			r.queue.requeueRetry(key, conn, disconnect, ReasonSession)
			continue
		}
		r.queue.enqueueKeyed(key, conn, disconnect, ReasonSession)
	}
}

// retry re-evaluates a key of dropped disconnects on the queue goroutine. The store is read again, so a connection is
// disconnected only when the store still rejects it, and an entry which expired is not applied.
func (r *Revoker) retry(key retryKey) {
	// Every kind is checked, so a retry applies whatever the store still holds.
	opts := CheckOptions{Block: true, Session: true, UserTokens: true}
	if key.user != "" {
		metrics.AccessRevokeDisconnectRetriesTotal.WithLabelValues(retryKindUser).Inc()
		for _, client := range r.hub.UserConnections(key.user) {
			r.retryConnection(client, opts, key)
		}
		return
	}
	metrics.AccessRevokeDisconnectRetriesTotal.WithLabelValues(retryKindSession).Inc()
	_, ok := r.store.session(key.session, r.store.nowUnix())
	if !ok {
		return
	}
	closeConn := closeWith(r.store.sessionDisconnect(key.session), centrifuge.DisconnectExpired)
	r.disconnectSessionConnections(key.session, closeConn, true)
}

// retryConnection disconnects a local connection which the store still rejects, and keeps a dropped disconnect under key.
// A queued or closing connection needs no check: the queue holds it already, and a check would count it as a reject
// again on every pass.
func (r *Revoker) retryConnection(client *centrifuge.Client, opts CheckOptions, key retryKey) {
	if r.queue.pending(client) {
		return
	}
	disconnect, reason, revoked := CheckConnection(r.store, client, opts, stageRetry)
	if revoked {
		r.queue.requeueRetry(key, client, disconnect, reason)
	}
}
