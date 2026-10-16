package config

import (
	"github.com/spf13/viper"
)

// setChannelStateDefaults sets the defaults of the channel_state settings in both placements. They are set
// on viper rather than with default tags: envconfig applies a default tag to any zero field, so an explicit
// zero from the config file would be replaced by the default. An absent key gets the default, an explicit
// zero keeps its meaning (no grace period for vacated_event_delay, and a validation error for num_partitions).
func setChannelStateDefaults(v *viper.Viper) {
	for _, prefix := range []string{"engine.redis.channel_state.", "presence_manager.redis.channel_state."} {
		v.SetDefault(prefix+"vacated_event_delay", "5s")
		v.SetDefault(prefix+"num_partitions", 128)
	}
}
