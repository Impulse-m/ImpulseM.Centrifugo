package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

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
