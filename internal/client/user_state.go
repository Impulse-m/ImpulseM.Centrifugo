package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/clientstorage"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// coreUserID returns the user Core sees for a connection of class with userID. A pending_2fa connection keeps
// its user for the registry, but is anonymous to Core: it gets no personal channel, no user channels and no
// entry in the user index of the hub. Any other class gives its user.
func coreUserID(class rpcregistry.AuthClass, userID string) string {
	if class == rpcregistry.AuthClassPending2FA {
		return ""
	}
	return userID
}

// storedTokenUser returns the user of the token the connection holds. A connection without a stored token user,
// such as one made through a connect proxy, is identified by its Core user.
func storedTokenUser(c Client) string {
	storage, release := c.AcquireStorage()
	defer release(storage)
	if user, ok := storage[clientstorage.KeyTokenUser].(string); ok {
		return user
	}
	return c.UserID()
}

// storeTokenUser stores the user of the token the connection holds now.
func storeTokenUser(c Client, userID string) {
	storage, release := c.AcquireStorage()
	defer release(storage)
	storage[clientstorage.KeyTokenUser] = userID
}

// personalChannelSubscription returns the personal channel of userID with the options it is subscribed with.
func (h *Handler) personalChannelSubscription(userID string) (string, centrifuge.SubscribeOptions, error) {
	personalChannel := h.cfgContainer.PersonalChannel(userID)
	_, _, chOpts, found, err := h.cfgContainer.ChannelOptions(personalChannel)
	if err != nil {
		log.Error().Err(err).Str("channel", personalChannel).Msg("error getting personal channel options")
		return "", centrifuge.SubscribeOptions{}, err
	}
	if !found {
		log.Info().Str("channel", personalChannel).Msg("subscribe unknown personal channel")
		return "", centrifuge.SubscribeOptions{}, centrifuge.ErrorUnknownChannel
	}
	return personalChannel, centrifuge.SubscribeOptions{
		EmitPresence:      chOpts.Presence,
		EmitJoinLeave:     chOpts.JoinLeave,
		PushJoinLeave:     chOpts.ForcePushJoinLeave,
		EnableRecovery:    chOpts.ForceRecovery,
		EnablePositioning: chOpts.ForcePositioning,
		RecoveryMode:      chOpts.GetRecoveryMode(),
		Source:            subsource.UserPersonal,
		HistoryMetaTTL:    chOpts.HistoryMetaTTL.ToDuration(),
		AllowedDeltaTypes: chOpts.AllowedDeltaTypes,
		AllowTagsFilter:   chOpts.AllowTagsFilter,
	}, nil
}

// enforceSingleUserConnection disconnects the other connections of userID, so that the connection clientID
// is the only one of the user on the personal channel.
func (h *Handler) enforceSingleUserConnection(clientID string, userID string) error {
	personalChannel := h.cfgContainer.PersonalChannel(userID)
	presenceStats, err := h.node.PresenceStats(personalChannel)
	if err != nil {
		log.Error().Err(err).Str("channel", personalChannel).Str("user", userID).Str("client", clientID).Msg("error calling presence stats in connecting")
		return centrifuge.DisconnectServerError
	}
	if presenceStats.NumClients >= 1 {
		err = h.node.Disconnect(
			userID,
			centrifuge.WithCustomDisconnect(centrifuge.DisconnectConnectionLimit),
			centrifuge.WithDisconnectClientWhitelist([]string{clientID}),
		)
		if err != nil {
			log.Error().Err(err).Str("user", userID).Str("client", clientID).Msg("error disconnecting user in connecting")
			return centrifuge.DisconnectServerError
		}
	}
	return nil
}

// attachRateLimit attaches the user state of the rate limits to the connection. Connections without a user
// are not attached.
func (h *Handler) attachRateLimit(c Client, userID string) {
	if h.userLimiter == nil || userID == "" {
		return
	}
	st, ok := c.ExtState().(*ratelimit.ConnState)
	if !ok {
		return
	}
	h.userLimiter.Attach(st, userID)
}

// detachRateLimit releases the user state of the rate limits attached to the connection. It is safe to call
// for a connection which is not attached.
func (h *Handler) detachRateLimit(c Client) {
	if h.userLimiter == nil {
		return
	}
	st, ok := c.ExtState().(*ratelimit.ConnState)
	if !ok {
		return
	}
	h.userLimiter.Detach(st)
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
