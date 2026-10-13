package configtypes

type Singleflight struct {
	// Enabled merges concurrent identical History, Presence and PresenceStats calls on the node into
	// one broker or presence manager request. Gives benefit only with a remote engine (Redis);
	// with the memory engine there is nothing to save.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enabled merges concurrent identical History, Presence and PresenceStats calls on the node into one broker or presence manager request. Gives benefit only with a remote engine (Redis); with the memory engine there is nothing to save."`
}
