package configtypes

// NamedBroker is an extra broker which channels select with the broker_name channel option.
type NamedBroker struct {
	// Name is a unique name of the broker, referenced by the broker_name channel option.
	Name string `mapstructure:"name" json:"name" envconfig:"name" yaml:"name" toml:"name" expose:"full" doc:"Unique name of the broker. Referenced by the <<broker_name>> channel option. Required."`

	// Broker is the configuration of the broker, the same as the standalone broker section.
	Broker `mapstructure:",squash" yaml:",inline"`
}

// NamedBrokers is a list of extra brokers.
type NamedBrokers []NamedBroker

// Decode to implement the envconfig.Decoder interface
func (d *NamedBrokers) Decode(value string) error {
	return decodeToNamedSlice(value, d)
}

// NamedPresenceManager is an extra presence manager which channels select with the
// presence_manager_name channel option.
type NamedPresenceManager struct {
	// Name is a unique name of the presence manager, referenced by the presence_manager_name channel option.
	Name string `mapstructure:"name" json:"name" envconfig:"name" yaml:"name" toml:"name" expose:"full" doc:"Unique name of the presence manager. Referenced by the <<presence_manager_name>> channel option. Required."`

	// PresenceManager is the configuration of the presence manager, the same as the standalone section.
	PresenceManager `mapstructure:",squash" yaml:",inline"`
}

// NamedPresenceManagers is a list of extra presence managers.
type NamedPresenceManagers []NamedPresenceManager

// Decode to implement the envconfig.Decoder interface
func (d *NamedPresenceManagers) Decode(value string) error {
	return decodeToNamedSlice(value, d)
}

// RedisController is a configuration for the "redis" controller.
type RedisController struct {
	// RedisPrefixed holds the prefix and the Redis connection options of the controller.
	RedisPrefixed `mapstructure:",squash" yaml:",inline"`
}

// NatsController is a configuration for the "nats" controller.
type NatsController struct {
	// NatsPrefixed holds the prefix and the Nats connection options of the controller.
	NatsPrefixed `mapstructure:",squash" yaml:",inline"`
}
