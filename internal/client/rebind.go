package client

import (
	"errors"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// onRefreshToken answers a refresh which the refresh proxy does not answer. A connection without a record refreshes as
// upstream does. A token which keeps the identity of the connection refreshes it in place, and a token which changes it
// rebinds the connection.
func (h *Handler) onRefreshToken(c rebindableClient, e centrifuge.RefreshEvent) (centrifuge.RefreshReply, RefreshExtra, error) {
	rec := connStateOf(c)
	if rec == nil {
		return h.refreshWithoutRecord(c, e)
	}
	// A refresh which finds another one in progress is rejected, not queued, so a refresh never waits for another one.
	if !rec.refreshing.CompareAndSwap(false, true) {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorTooManyRequests
	}
	defer rec.refreshing.Store(false)
	token, err := h.tokenVerifier.VerifyConnectToken(e.Token, h.cfgContainer.ConfigRef().Client.InsecureSkipTokenSignatureVerify)
	if err != nil {
		return refreshVerifyError(c, err)
	}
	class, ok := rpcregistry.ConnectionAuthClass(token.AuthClass, token.UserID)
	if !ok {
		log.Info().Str("client", c.ID()).Str("user", c.UserID()).Msg("invalid refresh token auth class")
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	if changesIdentity(rec, c, token.UserID, class) {
		return h.onRefreshRebind(c, rec, e.Token, token, class)
	}
	return h.refreshSameIdentity(c, rec, e.Token, token, class)
}

// refreshWithoutRecord answers a refresh of a connection which has no record. Such a connection has no rebind: a token of
// its user refreshes it, and a token of another user disconnects it. The token is checked as upstream checks it.
func (h *Handler) refreshWithoutRecord(c rebindableClient, e centrifuge.RefreshEvent) (centrifuge.RefreshReply, RefreshExtra, error) {
	token, err := h.tokenVerifier.VerifyConnectToken(e.Token, h.cfgContainer.ConfigRef().Client.InsecureSkipTokenSignatureVerify)
	if err != nil {
		return refreshVerifyError(c, err)
	}
	if token.UserID != c.UserID() {
		log.Info().Str("token_user", token.UserID).Str("user", c.UserID()).Str("client", c.ID()).Msg("refresh token user mismatch")
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	identity := accessrevoke.Identity(token.Session, token.Generation, token.IssuedAt)
	result := h.checkAccess(token.UserID, identity, accessrevoke.OptionsFromConfig(h.cfgContainer.ConfigRef()), "refresh")
	if result.Verdict != accessrevoke.Allowed {
		return refreshRevokeReply(c.ID(), c.UserID(), result)
	}
	if token.Meta != nil {
		setStorageMeta(c, token.Meta)
	}
	class, _ := rpcregistry.ConnectionAuthClass(token.AuthClass, token.UserID)
	return centrifuge.RefreshReply{
		ExpireAt:      token.ExpireAt,
		Info:          token.Info,
		TokenIdentity: &identity,
		User:          token.UserID,
		AuthClass:     h.authClassName(class),
	}, RefreshExtra{Subs: token.Subs, CheckSubs: true}, nil
}

// refreshVerifyError maps the error of verifying a refresh token to the refresh reply and error.
func refreshVerifyError(c rebindableClient, err error) (centrifuge.RefreshReply, RefreshExtra, error) {
	if errors.Is(err, jwtverify.ErrTokenExpired) {
		return centrifuge.RefreshReply{Expired: true}, RefreshExtra{}, nil
	}
	if errors.Is(err, jwtverify.ErrInvalidToken) {
		log.Info().Err(err).Str("client", c.ID()).Str("user", c.UserID()).Msg("invalid refresh token")
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	log.Error().Err(err).Str("client", c.ID()).Str("user", c.UserID()).Msg("error verifying refresh token")
	return centrifuge.RefreshReply{}, RefreshExtra{}, err
}

// changesIdentity reports whether a refresh with a token of tokenUser and class changes the identity of the connection:
// the user of the token, or the user Core sees. Core sees no user for a pending connection, so the token user is
// compared as well.
func changesIdentity(rec *connState, c Client, tokenUser string, class rpcregistry.AuthClass) bool {
	return rec.identity.Load().tokenUser != tokenUser || coreUserID(class, tokenUser) != c.UserID()
}

// onRefreshRebind answers a refresh whose token changes the identity of the connection. With rebind disabled the
// connection is disconnected, as before. Otherwise the connection moves to the identity of the token in place: its
// user, token, expiration and channels follow the transition between auth classes.
func (h *Handler) onRefreshRebind(
	c rebindableClient,
	rec *connState,
	token string,
	verified jwtverify.ConnectToken,
	nextClass rpcregistry.AuthClass,
) (centrifuge.RefreshReply, RefreshExtra, error) {
	cfg := h.cfgContainer.ConfigRef()
	if !cfg.Client.Rebind.Enabled {
		log.Info().Str("token_user", verified.UserID).Str("user", c.UserID()).Str("client", c.ID()).Msg("refresh token user mismatch")
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	prev := rec.identity.Load()
	// A connection made through a connect proxy never changes its identity: the channels the proxy granted were granted
	// under the proxy's terms, and no token proves a transition from them.
	if prev.origin == originProxy {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorPermissionDenied
	}
	// A logout of a connection with a user disconnects it unless allow_downgrade is set. The check comes before the
	// class check, so the logout of a connection is disconnected the same way whatever its class.
	if verified.UserID == "" && c.UserID() != "" && !cfg.Client.Rebind.AllowDowngrade {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	claim := h.registryConfig().AuthClassClaim
	// A connection with a user whose class was derived under another claim is not proven by the claim configured now.
	if c.UserID() != "" && prev.claim != claim {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorPermissionDenied
	}
	// A connection keeps the account it has held, which is the user of its last token with a user, so a logout does not
	// free it to take another user. An anonymous token has no user, so it is only checked by the transition. A connection
	// with no account may take any user.
	if held := prev.account; held != "" && verified.UserID != "" && held != verified.UserID {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorPermissionDenied
	}
	if err := rebindTransitionError(effectiveClass(prev, claim, c.UserID()), nextClass, cfg.Client.Rebind.AllowDowngrade); err != nil {
		return centrifuge.RefreshReply{}, RefreshExtra{}, err
	}

	// The current identity of the connection is still valid, so a rejected token is an error of the
	// refresh and the connection keeps its identity.
	identity := accessrevoke.Identity(verified.Session, verified.Generation, verified.IssuedAt)
	result := h.checkAccess(verified.UserID, identity, accessrevoke.OptionsFromConfig(cfg), "refresh")
	if result.Verdict != accessrevoke.Allowed {
		return centrifuge.RefreshReply{}, RefreshExtra{}, revokeError(c.ID(), verified.UserID, result, centrifuge.ErrorTokenExpired)
	}

	newUser := coreUserID(nextClass, verified.UserID)
	oldUser := c.UserID()
	// The rate limit state of the new user is attached before the rebind, and detached again when it fails.
	// Attaching releases the state of the previous user, so only a connection without a user is detached.
	if newUser == "" {
		h.detachRateLimit(c)
	} else {
		h.attachRateLimit(c, newUser)
	}
	// The new identity is stored before the Core user changes, so a call which overlaps the rebind sees the new class and
	// token. A failed rebind stores the previous identity back.
	rec.identity.Store(nextIdentity(prev, originToken, nextClass, claim, h.keptToken(token), verified))
	// The channels of the user go before the identity moves, so the anonymous identity never receives them. A rebind
	// which is refused after this leaves the connection without them, until its next refresh subscribes them again.
	if newUser == "" {
		h.unsubscribeNotAnonymous(c)
	}
	err := c.Rebind(centrifuge.RebindOptions{
		UserID:        newUser,
		ExpireAt:      verified.ExpireAt,
		TokenIdentity: identity,
	})
	if err != nil {
		rec.identity.Store(prev)
		h.detachRateLimit(c)
		// A connection closed meanwhile is released by its disconnect, so the old user is not attached again.
		if !errors.Is(err, centrifuge.ErrRebindClosed) {
			h.attachRateLimit(c, oldUser)
		}
		return centrifuge.RefreshReply{}, RefreshExtra{}, rebindError(c.ID(), err)
	}
	runTestHook(&testHookRebound, c)
	h.syncSessionConnection(c, rec, newUser, identity)
	// Channels which committed while the rebind ran are unsubscribed here, see unsubscribeNotAnonymous.
	if newUser == "" {
		h.unsubscribeNotAnonymous(c)
	}

	// A revocation applied between the check above and the rebind is caught here, as a connection is
	// checked after it is registered.
	result = h.checkAccess(verified.UserID, identity, accessrevoke.OptionsFromConfig(cfg), "rebind")
	if result.Verdict != accessrevoke.Allowed {
		return centrifuge.RefreshReply{}, RefreshExtra{}, revokeError(c.ID(), verified.UserID, result, centrifuge.DisconnectExpired)
	}

	if err := h.subscribeRebound(c, newUser, verified.Subs); err != nil {
		return centrifuge.RefreshReply{}, RefreshExtra{}, err
	}
	if verified.Meta != nil {
		setStorageMeta(c, verified.Meta)
	}
	return centrifuge.RefreshReply{
		ExpireAt:      verified.ExpireAt,
		Info:          verified.Info,
		TokenIdentity: &identity,
		User:          verified.UserID,
		AuthClass:     h.authClassName(nextClass),
	}, RefreshExtra{}, nil
}

// refreshSameIdentity answers a refresh whose token keeps the identity of the connection. The session is tracked before
// the check. A revocation applied after the check then finds the connection in the index, and one applied before it is
// seen by the check. The identity is stored before the check too, so a connection with a user, which is not in the
// index, is found by a revocation of the new session. A rejected refresh stores the previous identity back.
func (h *Handler) refreshSameIdentity(
	c rebindableClient,
	rec *connState,
	refreshToken string,
	verified jwtverify.ConnectToken,
	class rpcregistry.AuthClass,
) (centrifuge.RefreshReply, RefreshExtra, error) {
	prev := rec.identity.Load()
	// The class of a token changes only through the transition table, and a same-user change between authenticated and
	// bot is none of its transitions. A connection which has no token-derived class yet, as a connect proxy one, takes
	// the class of its first token.
	if prev.class != 0 && prev.class != class {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorPermissionDenied
	}
	identity := accessrevoke.Identity(verified.Session, verified.Generation, verified.IssuedAt)
	rec.identity.Store(nextIdentity(prev, originToken, class, h.registryConfig().AuthClassClaim, h.keptToken(refreshToken), verified))
	h.syncSessionConnection(c, rec, c.UserID(), identity)
	runTestHook(&testHookRefreshTracked, c)
	result := h.checkAccess(verified.UserID, identity, accessrevoke.OptionsFromConfig(h.cfgContainer.ConfigRef()), "refresh")
	if result.Verdict != accessrevoke.Allowed {
		rec.identity.Store(prev)
		h.syncSessionConnection(c, rec, c.UserID(), prev.identity)
		return refreshRevokeReply(c.ID(), c.UserID(), result)
	}
	if verified.Meta != nil {
		setStorageMeta(c, verified.Meta)
	}
	return centrifuge.RefreshReply{
		ExpireAt:      verified.ExpireAt,
		Info:          verified.Info,
		TokenIdentity: &identity,
		User:          verified.UserID,
		AuthClass:     h.authClassName(class),
	}, RefreshExtra{Subs: verified.Subs, CheckSubs: true}, nil
}

// rebindAllowed reports whether a connection of class from may move to class to. Downgrades of an
// authenticated connection are checked by rebindTransitionError.
func rebindAllowed(from rpcregistry.AuthClass, to rpcregistry.AuthClass) bool {
	switch from {
	case rpcregistry.AuthClassAnonymous:
		return to == rpcregistry.AuthClassPending2FA || to == rpcregistry.AuthClassAuthenticated || to == rpcregistry.AuthClassBot
	case rpcregistry.AuthClassPending2FA:
		return to == rpcregistry.AuthClassAuthenticated || to == rpcregistry.AuthClassAnonymous
	}
	return false
}

// rebindTransitionError returns the error of a refresh which moves a connection of class from to class to, or
// nil when the transition is allowed. A refresh of an authenticated connection to an anonymous token is a
// logout: it keeps the connection only with allowDowngrade, and disconnects it otherwise, as upstream does.
// Any other transition which is not allowed is a permission error, and the connection keeps its class.
func rebindTransitionError(from rpcregistry.AuthClass, to rpcregistry.AuthClass, allowDowngrade bool) error {
	isLogout := from == rpcregistry.AuthClassAuthenticated || from == rpcregistry.AuthClassBot
	if isLogout && to == rpcregistry.AuthClassAnonymous {
		if allowDowngrade {
			return nil
		}
		return centrifuge.DisconnectInvalidToken
	}
	if rebindAllowed(from, to) {
		return nil
	}
	return centrifuge.ErrorPermissionDenied
}

// rebindError maps an error of Client.Rebind to the error of the refresh. A new user over its connection limit is an
// error of the refresh, not a disconnect: the connection keeps its identity, and the client may refresh again later.
func rebindError(clientID string, err error) error {
	switch {
	case errors.Is(err, centrifuge.ErrRebindUserLimit):
		return centrifuge.ErrorTooManyRequests
	case errors.Is(err, centrifuge.ErrRebindExpired):
		return centrifuge.ErrorTokenExpired
	case errors.Is(err, centrifuge.ErrRebindClosed):
		return centrifuge.DisconnectConnectionClosed
	}
	log.Error().Err(err).Str("client", clientID).Msg("error rebinding connection")
	return centrifuge.DisconnectServerError
}

// unsubscribeNotAnonymous unsubscribes the channels which a connection without a user may not keep: the
// channels of a user, the personal channel, and the channels of token and proxy sources. A client channel stays
// when its namespace allows anonymous subscriptions.
func (h *Handler) unsubscribeNotAnonymous(c rebindableClient) {
	for channel, chCtx := range c.ChannelsWithContext() {
		if h.keepsAnonymous(channel, chCtx.Source) {
			continue
		}
		c.Unsubscribe(channel, centrifuge.Unsubscribe{Code: centrifuge.UnsubscribeCodeServer, Reason: "server unsubscribed"})
	}
}

// keepsAnonymous reports whether a channel of source may stay on a connection without a user.
func (h *Handler) keepsAnonymous(channel string, source uint8) bool {
	if source != subsource.ClientAllowed && source != subsource.ClientInsecure {
		return false
	}
	_, _, chOpts, found, err := h.cfgContainer.ChannelOptions(channel)
	if err != nil || !found {
		return false
	}
	if h.cfgContainer.IsUserLimited(channel) && chOpts.UserLimitedChannels {
		return false
	}
	return chOpts.SubscribeForAnonymous
}

// subscribeRebound subscribes a rebound connection to the channels of its token. A connection with a user also gets
// its personal channel, and the single connection of the user is enforced first. The channels the connection has
// already are kept. The subscriptions are not checked against the token channels of the connection, so a refresh
// does not disconnect it. A personal channel which cannot be resolved disconnects the connection after the rebind,
// which leaves no half-moved connection: a disconnect ends the connection and releases what it holds.
func (h *Handler) subscribeRebound(c rebindableClient, userID string, subs map[string]centrifuge.SubscribeOptions) error {
	cfg := h.cfgContainer.ConfigRef()
	toSubscribe := make(map[string]centrifuge.SubscribeOptions, len(subs)+1)
	if userID != "" && cfg.Client.SubscribeToUserPersonalChannel.Enabled {
		if cfg.Client.SubscribeToUserPersonalChannel.SingleConnection {
			if err := h.enforceSingleUserConnection(c.ID(), userID); err != nil {
				return err
			}
		}
		personalChannel, personalOptions, err := h.personalChannelSubscription(userID)
		if err != nil {
			return centrifuge.DisconnectServerError
		}
		toSubscribe[personalChannel] = personalOptions
	}
	for channel, opts := range subs {
		if _, ok := toSubscribe[channel]; !ok {
			toSubscribe[channel] = opts
		}
	}
	for channel, opts := range toSubscribe {
		if c.IsSubscribed(channel) {
			continue
		}
		err := c.Subscribe(channel, subscribeOptionsOf(opts))
		if err != nil && !errors.Is(err, centrifuge.ErrorAlreadySubscribed) {
			log.Error().Err(err).Str("channel", channel).Str("client", c.ID()).Msg("error subscribing connection on rebind")
			return centrifuge.DisconnectServerError
		}
	}
	return nil
}

// subscribeOptionsOf returns a subscribe option which sets all the options of opts at once, as the connect
// path does with the subscriptions of its reply.
func subscribeOptionsOf(opts centrifuge.SubscribeOptions) centrifuge.SubscribeOption {
	return func(target *centrifuge.SubscribeOptions) {
		*target = opts
	}
}
