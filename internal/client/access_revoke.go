package client

import (
	"sync/atomic"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
)

// testHookConnectTracked, if set, runs in trackConnect after the session of a connection is tracked and before the
// connection is rechecked. Tests use it to apply a revocation at that point.
var testHookConnectTracked atomic.Pointer[func(c rebindableClient)]

// testHookRefreshTracked, if set, runs in refreshSameIdentity after the session of a refreshed connection is tracked and
// before the check. Tests use it to apply a revocation at that point.
var testHookRefreshTracked atomic.Pointer[func(c rebindableClient)]

// testHookRebound, if set, runs in onRefreshRebind after the connection is rebound and before its session is tracked.
// Tests use it to close the connection at that point.
var testHookRebound atomic.Pointer[func(c rebindableClient)]

// testHookRPCTracked, if set, runs in onRPCRegistry after the dedup state of a call is made, from the identity the call
// reads, and before the call runs. Tests use it to change the identity of the connection at that point.
var testHookRPCTracked atomic.Pointer[func(c rebindableClient)]

// testHookProxyMoved, if set, runs in moveProxyIdentity after the identity of a refresh proxy move is stored and before
// its session is synced in the index, with the move lock held. Tests use it to start another move at that point.
var testHookProxyMoved atomic.Pointer[func(c rebindableClient)]

// runTestHook runs the test hook set in p, if any, with c.
func runTestHook(p *atomic.Pointer[func(c rebindableClient)], c rebindableClient) {
	if f := p.Load(); f != nil {
		(*f)(c)
	}
}

// sourceGate returns accessrevoke.ErrorNotReady while the access revocation source is not ready, or
// lags when reject_connect_on_lag is set. Without a source it passes.
//
// The error is temporary, so it is never turned into a terminal close. Bidirectional clients get it as a
// connect error and retry. Unidirectional transports (WebSocket, gRPC, SSE, HTTP stream) call Core's
// ProtocolConnect, which turns a non-Disconnect connect error into a close with Disconnect from
// Core's unidirectional mapping: code 1503 is not in the default mapping, so it becomes DisconnectServerError
// (3004, reconnect range), unless the operator maps it with connect_code_to_unidirectional_disconnect.
// SSE and HTTP stream with connect_code_to_http_response enabled answer the connect with the HTTP status
// from that config instead. So no Disconnect is returned here: a fixed terminal code such as 4503 would
// stop clients from reconnecting, as codes 4500-4999 are terminal.
func (h *Handler) sourceGate() error {
	if h.accessSource == nil {
		return nil
	}
	if !h.accessSource.Ready() {
		metrics.AccessRevokeRejectsTotal.WithLabelValues(accessrevoke.RejectNotReady, "connect").Inc()
		return accessrevoke.ErrorNotReady
	}
	if h.cfgContainer.ConfigRef().AccessRevoke.Source.RejectConnectOnLag && h.accessSource.Lagging() {
		metrics.AccessRevokeRejectsTotal.WithLabelValues(accessrevoke.RejectLagging, "connect").Inc()
		return accessrevoke.ErrorNotReady
	}
	return nil
}

// checkAccess checks token identity against access revocation store and counts rejections by stage.
// Without a store nothing is revoked.
func (h *Handler) checkAccess(
	userID string,
	identity centrifuge.TokenIdentity,
	opts accessrevoke.CheckOptions,
	stage string,
) accessrevoke.Result {
	if h.accessStore == nil {
		return accessrevoke.Result{Verdict: accessrevoke.Allowed}
	}
	result := h.accessStore.Check(userID, identity, opts)
	accessrevoke.CountReject(result, stage)
	return result
}

// isUserBlocked checks only user block for paths which have no token identity (connect and
// refresh proxies).
func (h *Handler) isUserBlocked(userID string, stage string) bool {
	if !h.cfgContainer.ConfigRef().UserBlock.Enabled {
		return false
	}
	result := h.checkAccess(userID, centrifuge.TokenIdentity{}, accessrevoke.CheckOptions{Block: true}, stage)
	return result.Verdict == accessrevoke.Blocked
}

// recheckConnection checks a connection right after it was registered in the hub. Events applied
// between the connect check and the registration are caught here. A connection with zero token
// identity came from the connect proxy (no token), so only user block applies to it. The same is
// true for a token which has no session, generation and iat claims, which is a deliberate
// trade-off: the full check at connect time already ran for such a token.
func (h *Handler) recheckConnection(client *centrifuge.Client) (centrifuge.Disconnect, bool) {
	if h.accessStore == nil {
		return centrifuge.Disconnect{}, false
	}
	disconnect, _, revoked := accessrevoke.CheckConnection(
		h.accessStore,
		client,
		accessrevoke.OptionsFromConfig(h.cfgContainer.ConfigRef()),
		"recheck",
	)
	return disconnect, revoked
}

// trackConnect sets up a connection which has just connected, and returns the disconnect of a connection which must
// be closed. A connection with a record gets its disconnect handler first, when the access store or the user limiter
// needs to release it, so a close from here on releases what the connection holds. A connection without a user is
// indexed by the session of its token before the recheck, so a revocation applied from then on reaches it through the
// index. Core runs the disconnect handler of a connection closed while it connects after the connect completes, so the
// connection closed here is released then. A connection which clientNeedsConnState does not ask for has no record: nothing
// is adopted, its storage is not read, and the connect path allocates nothing.
func (h *Handler) trackConnect(client *centrifuge.Client) (centrifuge.Disconnect, bool) {
	var rec *connState
	if h.clientNeedsConnState(h.cfgContainer.ConfigRef(), client) {
		rec = adoptConnState(client)
	}
	if rec != nil && (h.accessStore != nil || h.userLimiter != nil) {
		h.installDisconnect(client, rec)
	}
	if rec != nil && h.accessStore != nil && client.UserID() == "" {
		h.trackSession(client, rec, rec.identity.Load().identity.Session)
	}
	runTestHook(&testHookConnectTracked, client)
	return h.recheckConnection(client)
}

// recordAtRefresh returns the record of a connection which has none, and attaches a new one to it. It is called for a
// connection with no user which refreshes to a token with a session, and the index needs the record to hold the session,
// so that a revocation of it reaches the connection at once. The record is attached under the storage lock of the
// connection, so two refreshes which overlap attach one record between them. The disconnect handler is installed after
// the record is attached, so a close from here on releases it. A close which lands before the install is not seen by the
// handler, see the known gaps of the README.
//
// The origin of such a connection is not known: it has no record, so it may be a connect proxy connection, made before a
// reload enabled the record for every connection. It is taken as a proxy one, which never rebinds, so a reload can not
// let it move to a user it did not connect as.
func (h *Handler) recordAtRefresh(c rebindableClient) *connState {
	storage, release := c.AcquireStorage()
	if rec := connStateOf(c); rec != nil {
		release(storage)
		return rec
	}
	rec := &connState{}
	rec.identity.Store(nextIdentity(nil, originProxy, 0, "", "", jwtverify.ConnectToken{}))
	c.SetExtState(rec)
	release(storage)
	h.installDisconnect(c, rec)
	return rec
}

// syncSessionConnection keeps the session index of access revocation up to date for a refreshed or rebound
// connection. A connection with a user is not indexed, a connection without a user is indexed by the session of its
// token.
func (h *Handler) syncSessionConnection(
	c rebindableClient,
	rec *connState,
	userID string,
	identity centrifuge.TokenIdentity,
) {
	if h.accessStore == nil {
		return
	}
	if userID != "" {
		h.accessStore.SessionConnections().Untrack(c, &rec.track)
		return
	}
	h.trackSession(c, rec, identity.Session)
}

// trackSession moves the connection into session in the index. The index does nothing for a connection which is
// closed: its disconnect released it, and a session set after that would stay in the index for good.
func (h *Handler) trackSession(c rebindableClient, rec *connState, session [16]byte) {
	h.accessStore.SessionConnections().Track(c, &rec.track, session)
}

// onClientDisconnect releases what the handler holds for the connection: its user rate limit state and its
// session index entry. The connection is marked closed first, so a rebind which fails after this point knows it, and no
// session is tracked for it after the release.
func (h *Handler) onClientDisconnect(c rebindableClient, rec *connState) {
	rec.track.MarkClosed()
	h.detachRateLimit(c)
	if h.accessStore != nil {
		h.accessStore.SessionConnections().Close(c, &rec.track)
	}
}

// installDisconnect installs the disconnect handler which releases the record of a connection when it closes. It is
// installed by trackConnect and recordAtRefresh, the two paths which attach a record.
func (h *Handler) installDisconnect(c rebindableClient, rec *connState) {
	c.OnDisconnect(func(centrifuge.DisconnectEvent) {
		h.onClientDisconnect(c, rec)
	})
}

// connectTokenIdentity returns the identity of a connection token, checked against the access revocation
// store. The error is the connect error of a token which is blocked, revoked or rejected by the loop guard.
func (h *Handler) connectTokenIdentity(
	clientID string,
	token jwtverify.ConnectToken,
	cfg *config.Config,
) (centrifuge.TokenIdentity, error) {
	identity := tokenIdentityOf(token)
	result := h.checkAccess(token.UserID, identity, accessrevoke.OptionsFromConfig(cfg), "connect")
	if result.Verdict != accessrevoke.Allowed {
		return centrifuge.TokenIdentity{}, revokeError(clientID, token.UserID, result, centrifuge.ErrorTokenExpired)
	}
	return identity, nil
}

// revokeError maps a failed access check made by a connect or a refresh to the error returned from it. A revoked
// connection gets the error revoked, which the caller chooses: a connect and a rebind differ in it.
func revokeError(clientID string, userID string, result accessrevoke.Result, revoked error) error {
	if result.Verdict == accessrevoke.Revoked {
		return revoked
	}
	if result.Verdict == accessrevoke.LoopGuard {
		accessrevoke.LogLoopGuard(clientID, userID, result)
	}
	disconnect, rejected := accessrevoke.RejectDisconnect(result, centrifuge.Disconnect{})
	if !rejected {
		return nil
	}
	return disconnect
}

// refreshRevokeReply maps a failed access check made at refresh to the refresh reply and error. A revoked
// connection gets an expired reply, a blocked or looping one a disconnect.
func refreshRevokeReply(clientID string, userID string, result accessrevoke.Result) (centrifuge.RefreshReply, RefreshExtra, error) {
	if result.Verdict == accessrevoke.Revoked {
		return centrifuge.RefreshReply{Expired: true}, RefreshExtra{}, nil
	}
	return centrifuge.RefreshReply{}, RefreshExtra{}, revokeError(clientID, userID, result, nil)
}
