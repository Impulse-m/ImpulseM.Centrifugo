package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
)

// testHookConnectTracked, if set, runs in OnConnect after the session of a connection is tracked and before the
// connection is rechecked. Tests use it to apply a revocation at that point.
var testHookConnectTracked func(c *centrifuge.Client)

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
	if result.Verdict != accessrevoke.Allowed {
		metrics.AccessRevokeRejectsTotal.WithLabelValues(string(result.Reason), stage).Inc()
	}
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

// syncSessionConnection keeps the session index of access revocation up to date for the connection. A
// connection without a user is indexed by the session of its token, a connection with a user is not.
func (h *Handler) syncSessionConnection(c Client, userID string, identity centrifuge.TokenIdentity) {
	if h.accessStore == nil {
		return
	}
	sessions := h.accessStore.SessionConnections()
	if userID != "" {
		sessions.Untrack(c.ID())
		return
	}
	sessions.Track(c.ID(), c, identity.Session)
}

// onClientDisconnect releases what the handler holds for the connection: its user rate limit state and its
// session index entry. Core keeps one disconnect handler per client, so this is the only one set in OnConnect.
func (h *Handler) onClientDisconnect(c Client) {
	h.detachRateLimit(c)
	if h.accessStore != nil {
		h.accessStore.SessionConnections().Untrack(c.ID())
	}
}

// connectTokenIdentity returns the identity of a connection token, checked against the access revocation
// store. The error is the connect error of a token which is blocked, revoked or rejected by the loop guard.
func (h *Handler) connectTokenIdentity(
	clientID string,
	token jwtverify.ConnectToken,
	cfg *config.Config,
) (centrifuge.TokenIdentity, error) {
	identity := accessrevoke.Identity(token.Session, token.Generation, token.IssuedAt)
	result := h.checkAccess(token.UserID, identity, accessrevoke.OptionsFromConfig(cfg), "connect")
	if result.Verdict != accessrevoke.Allowed {
		return centrifuge.TokenIdentity{}, connectRevokeError(clientID, token.UserID, result)
	}
	return identity, nil
}

// connectRevokeError maps a failed access check made at connect to the error returned from connecting.
func connectRevokeError(clientID string, userID string, result accessrevoke.Result) error {
	switch result.Verdict {
	case accessrevoke.Blocked:
		return accessrevoke.DisconnectUserBlocked
	case accessrevoke.Revoked:
		return centrifuge.ErrorTokenExpired
	case accessrevoke.LoopGuard:
		accessrevoke.LogLoopGuard(clientID, userID, result)
		return centrifuge.DisconnectInvalidToken
	}
	return nil
}

// refreshRevokeReply maps a failed access check made at refresh to the refresh reply and error. A revoked
// connection gets an expired reply, a blocked or looping one a disconnect.
func refreshRevokeReply(clientID string, userID string, result accessrevoke.Result) (centrifuge.RefreshReply, RefreshExtra, error) {
	switch result.Verdict {
	case accessrevoke.Blocked:
		return centrifuge.RefreshReply{}, RefreshExtra{}, accessrevoke.DisconnectUserBlocked
	case accessrevoke.Revoked:
		return centrifuge.RefreshReply{Expired: true}, RefreshExtra{}, nil
	case accessrevoke.LoopGuard:
		accessrevoke.LogLoopGuard(clientID, userID, result)
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	return centrifuge.RefreshReply{}, RefreshExtra{}, nil
}
