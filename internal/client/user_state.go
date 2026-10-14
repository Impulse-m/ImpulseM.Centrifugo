package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/rpcregistry"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// storageKeyTokenUser is the storage key of the user of the connection token. A pending connection keeps it, as Core
// sees no user for it. It is stored for a pending connection at connect, and by a rebind.
const storageKeyTokenUser = "token_user"

// storageKeyAccountUser is the storage key of the account of the connection, see storedAccountUser.
const storageKeyAccountUser = "account_user"

// coreUserID returns the user Core sees for a connection of class with userID. A pending_2fa connection keeps
// its user for the registry, but is anonymous to Core: it gets no personal channel, no user channels and no
// entry in the user index of the hub. Any other class gives its user.
func coreUserID(class rpcregistry.AuthClass, userID string) string {
	if class == rpcregistry.AuthClassPending2FA {
		return ""
	}
	return userID
}

// changesIdentity reports whether a refresh with a token of tokenUser and class changes the identity of the
// connection: the user of the token, or the user Core sees. Core sees no user for a pending connection, so the token
// user is compared as well.
func changesIdentity(c Client, tokenUser string, class rpcregistry.AuthClass) bool {
	return storedTokenUser(c) != tokenUser || coreUserID(class, tokenUser) != c.UserID()
}

// storedTokenUser returns the user of the token the connection holds. A connection without a stored token user,
// such as one made through a connect proxy, is identified by its Core user.
func storedTokenUser(c Client) string {
	storage, release := c.AcquireStorage()
	defer release(storage)
	if user, ok := storage[storageKeyTokenUser].(string); ok {
		return user
	}
	return c.UserID()
}

// storeTokenUser stores the user of the token the connection holds now.
func storeTokenUser(c Client, userID string) {
	storage, release := c.AcquireStorage()
	defer release(storage)
	storage[storageKeyTokenUser] = userID
}

// storedAccountUser returns the account of the connection: the user of its last token with a user. A downgrade to an
// anonymous token keeps it, so a connection which logged out may not take another account. Without a stored account
// it is the user of the token, or the Core user of a connection made through a connect proxy.
func storedAccountUser(c Client) string {
	storage, release := c.AcquireStorage()
	defer release(storage)
	if user, ok := storage[storageKeyAccountUser].(string); ok {
		return user
	}
	if user, ok := storage[storageKeyTokenUser].(string); ok {
		return user
	}
	return c.UserID()
}

// storeAccountUser stores the account of the connection.
func storeAccountUser(c Client, userID string) {
	storage, release := c.AcquireStorage()
	defer release(storage)
	storage[storageKeyAccountUser] = userID
}

// storedIdentity returns the token identity the server keeps for the connection. It is the zero identity when none is
// kept, as for a connection made through a connect proxy.
func storedIdentity(c Client) centrifuge.TokenIdentity {
	storage, release := c.AcquireStorage()
	defer release(storage)
	identity, _ := storage[accessrevoke.StorageKeyIdentity].(centrifuge.TokenIdentity)
	return identity
}

// setStoredIdentity stores the token identity the server keeps for the connection. Access revocation reads it, see
// accessrevoke.IdentityOf.
func setStoredIdentity(c Client, identity centrifuge.TokenIdentity) {
	storage, release := c.AcquireStorage()
	defer release(storage)
	storage[accessrevoke.StorageKeyIdentity] = identity
}

// setReplyIdentity sets the user and the auth class which a connect reply reports. A pending connection reports the
// user of its token, which Core does not see.
func (h *Handler) setReplyIdentity(reply *centrifuge.ConnectReply, userID string, class rpcregistry.AuthClass) {
	reply.User = userID
	reply.AuthClass = h.authClassName(class)
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
