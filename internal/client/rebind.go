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

// onRefreshRebind answers a refresh whose token user is not the user Core has for the connection. With
// rebind disabled the connection is disconnected, as before. Otherwise the connection moves to the identity of
// the token in place: its user, token, expiration and channels follow the transition between auth classes.
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
	claim := h.registryConfig().AuthClassClaim
	// A connection with a user and no class under the claim came through a connect proxy: its user is not
	// proven by a token, so it may not move to another identity.
	if c.UserID() != "" && !hasStoredAuthClass(c, claim) {
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
		return centrifuge.RefreshReply{}, RefreshExtra{}, connectRevokeError(c.ID(), verified.UserID, result)
	}

	newUser := coreUserID(nextClass, verified.UserID)
	oldUser := c.UserID()
	h.detachRateLimit(c)
	err := c.Rebind(centrifuge.RebindOptions{
		UserID:        newUser,
		ExpireAt:      verified.ExpireAt,
		TokenIdentity: identity,
	})
	if err != nil {
		h.attachRateLimit(c, oldUser)
		return centrifuge.RefreshReply{}, RefreshExtra{}, rebindError(c.ID(), err)
	}
	h.attachRateLimit(c, newUser)
	h.replaceRegistryToken(c, token, nextClass)
	storeTokenUser(c, verified.UserID)
	h.syncSessionConnection(c, newUser, identity)

	// A revocation applied between the check above and the rebind is caught here, as a connection is
	// checked after it is registered.
	result = h.checkAccess(verified.UserID, identity, accessrevoke.OptionsFromConfig(cfg), "rebind")
	if result.Verdict != accessrevoke.Allowed {
		return centrifuge.RefreshReply{}, RefreshExtra{}, rebindRecheckError(c.ID(), verified.UserID, result)
	}

	if newUser == "" {
		h.unsubscribeNotAnonymous(c)
	} else if err := h.subscribeUpgraded(c, newUser, verified.Subs); err != nil {
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

// rebindRecheckError maps a failed access check made after the rebind to the disconnect of the connection.
func rebindRecheckError(clientID string, userID string, result accessrevoke.Result) error {
	switch result.Verdict {
	case accessrevoke.Blocked:
		return accessrevoke.DisconnectUserBlocked
	case accessrevoke.Revoked:
		return centrifuge.DisconnectExpired
	case accessrevoke.LoopGuard:
		accessrevoke.LogLoopGuard(clientID, userID, result)
		return centrifuge.DisconnectInvalidToken
	}
	return nil
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

// subscribeUpgraded subscribes a connection which got a user to its personal channel and to the channels of its
// token. The single connection of the user is enforced first. Channels the connection has already are kept. The
// subscriptions are not checked against the token channels of the connection, so a refresh does not disconnect it.
func (h *Handler) subscribeUpgraded(c Client, userID string, subs map[string]centrifuge.SubscribeOptions) error {
	cfg := h.cfgContainer.ConfigRef()
	toSubscribe := make(map[string]centrifuge.SubscribeOptions, len(subs)+1)
	if cfg.Client.SubscribeToUserPersonalChannel.Enabled {
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
