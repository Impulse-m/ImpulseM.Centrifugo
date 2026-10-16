package configtypes

// RedisChannelState configures channel state events (occupied and vacated) of the Redis presence manager.
// Channel state events are sent to the channel state proxy for namespaces with state_proxy_enabled.
type RedisChannelState struct {
	// VacatedEventDelay is a delay before the vacated event is sent after the last subscriber leaves
	// a channel. A subscribe within the delay cancels the event. Zero means no grace period.
	// The default is set in the config package, see setChannelStateDefaults.
	VacatedEventDelay Duration `mapstructure:"vacated_event_delay" json:"vacated_event_delay" envconfig:"vacated_event_delay" yaml:"vacated_event_delay" toml:"vacated_event_delay" doc:"Delay before the vacated event is sent after the last subscriber leaves a channel. A subscribe within the delay cancels it. Zero means no grace period. Default <<5s>>."`
	// NumPartitions is a number of partitions which serialize channel state events in Redis.
	// It must not change while channels are occupied. The default is set in the config package.
	NumPartitions int `mapstructure:"num_partitions" json:"num_partitions" envconfig:"num_partitions" yaml:"num_partitions" toml:"num_partitions" doc:"Number of partitions which serialize channel state events in Redis. Must be between 1 and 4096. Do not change while channels are occupied, the state of occupied channels is orphaned otherwise. Default <<128>>."`
}
