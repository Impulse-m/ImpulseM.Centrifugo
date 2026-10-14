package client

import (
	"errors"
	"sync/atomic"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// testHookRebound, if set, runs in onRefreshRebind after the connection is rebound and before its session is
// tracked. Tests use it to close the connection at that point.
var testHookRebound atomic.Pointer[func(c Client)]

// onRefreshRebind answers a refresh whose token changes the identity of the connection. With rebind disabled the
// connection is disconnected, as before. Otherwise the connection moves to the identity of the token in place: its
// user, token, expiration and channels follow the transition between auth classes.
func (h *Handler) onRefreshRebind(
	c Client,
	token string,
	verified jwtverify.ConnectToken,
	nextClass rpcregistry.AuthClass,
) (centrifuge.RefreshReply, RefreshExtra, error) {
	cfg := h.cfgContainer.ConfigRef()
	if !cfg.Client.Rebind.Enabled {
		log.Info().Str("token_user", verified.UserID).Str("user", c.UserID()).Str("client", c.ID()).Msg("refresh token user mismatch")
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	// A logout of a connection with a user disconnects it unless allow_downgrade is set. The check comes before the
	// class check, so a connection made through a connect proxy, which has no class, is disconnected the same way.
	if verified.UserID == "" && c.UserID() != "" && !cfg.Client.Rebind.AllowDowngrade {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.DisconnectInvalidToken
	}
	claim := h.registryConfig().AuthClassClaim
	// A connection with a user and no class under the claim came through a connect proxy: its user is not
	// proven by a token, so it may not move to another identity.
	if c.UserID() != "" && !hasStoredAuthClass(c, claim) {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorPermissionDenied
	}
	// A connection which holds the user of a token keeps that user: it may not move to another user. An anonymous
	// token has no user, so it is only checked by the transition. A connection with no user may take any user.
	if held := storedTokenUser(c); held != "" && verified.UserID != "" && held != verified.UserID {
		return centrifuge.RefreshReply{}, RefreshExtra{}, centrifuge.ErrorPermissionDenied
	}
	_, currentClass := registryConnection(c, claim)
	if err := rebindTransitionError(currentClass, nextClass, cfg.Client.Rebind.AllowDowngrade); err != nil {
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
	oldToken, oldClass := registryConnection(c, claim)
	oldTokenUser := storedTokenUser(c)
	// The registry reads the class and the token of a connection, and the rate limit state reads its user. The new
	// class, token and user are stored before the Core user changes, so a call which overlaps the rebind sees the new
	// class. The rate limit state of the new user is attached before the rebind, and detached again when it fails.
	h.detachRateLimit(c)
	h.attachRateLimit(c, newUser)
	h.replaceRegistryToken(c, token, nextClass)
	storeTokenUser(c, verified.UserID)
	err := c.Rebind(centrifuge.RebindOptions{
		UserID:        newUser,
		ExpireAt:      verified.ExpireAt,
		TokenIdentity: identity,
	})
	if err != nil {
		h.detachRateLimit(c)
		// A connection closed meanwhile is released by its disconnect, so the old user is not attached again.
		if !errors.Is(err, centrifuge.ErrRebindClosed) {
			h.attachRateLimit(c, oldUser)
		}
		h.replaceRegistryToken(c, oldToken, oldClass)
		storeTokenUser(c, oldTokenUser)
		return centrifuge.RefreshReply{}, RefreshExtra{}, rebindError(c.ID(), err)
	}
	runTestHook(&testHookRebound, c)
	h.syncSessionConnection(c, newUser, identity)

	// A revocation applied between the check above and the rebind is caught here, as a connection is
	// checked after it is registered.
	result = h.checkAccess(verified.UserID, identity, accessrevoke.OptionsFromConfig(cfg), "rebind")
	if result.Verdict != accessrevoke.Allowed {
		return centrifuge.RefreshReply{}, RefreshExtra{}, revokeError(c.ID(), verified.UserID, result, centrifuge.DisconnectExpired)
	}

	if newUser == "" {
		h.unsubscribeNotAnonymous(c)
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
// seen by the check.
func (h *Handler) refreshSameIdentity(
	c Client,
	refreshToken string,
	verified jwtverify.ConnectToken,
	class rpcregistry.AuthClass,
) (centrifuge.RefreshReply, RefreshExtra, error) {
	identity := accessrevoke.Identity(verified.Session, verified.Generation, verified.IssuedAt)
	h.syncSessionConnection(c, c.UserID(), identity)
	runTestHook(&testHookRefreshTracked, c)
	result := h.checkAccess(verified.UserID, identity, accessrevoke.OptionsFromConfig(h.cfgContainer.ConfigRef()), "refresh")
	if result.Verdict != accessrevoke.Allowed {
		return refreshRevokeReply(c.ID(), c.UserID(), result)
	}
	h.replaceRegistryToken(c, refreshToken, class)
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

// rebindError maps an error of Client.Rebind to the error of the refresh.
func rebindError(clientID string, err error) error {
	switch {
	case errors.Is(err, centrifuge.ErrRebindUserLimit):
		return centrifuge.DisconnectConnectionLimit
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
func (h *Handler) unsubscribeNotAnonymous(c Client) {
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
// does not disconnect it.
func (h *Handler) subscribeRebound(c Client, userID string, subs map[string]centrifuge.SubscribeOptions) error {
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
