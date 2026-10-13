package config

import (
	"errors"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// validateSharedPositionSync checks the channel options of shared_position_sync: it needs positioning or
// recovery, and the channel position check delay of the client must be set.
func validateSharedPositionSync(c configtypes.ChannelOptions, cfg Config) error {
	if c.SharedPositionSync && !c.ForcePositioning && !c.AllowPositioning && !c.ForceRecovery && !c.AllowRecovery {
		return errors.New("shared_position_sync requires positioning or recovery")
	}
	if c.SharedPositionSync && cfg.Client.ChannelPositionCheckDelay == 0 {
		return errors.New("shared_position_sync requires client.channel_position_check_delay to be greater than zero")
	}
	return nil
}
