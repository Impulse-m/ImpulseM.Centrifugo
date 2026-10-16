package configtypes

// Rebind is a configuration for moving a live connection to another identity on refresh, without a
// reconnect. Without it, a refresh with a token of another user disconnects the connection.
type Rebind struct {
	// Enabled turns on rebinding a live connection on refresh. A connection can move from anonymous to a
	// user, from a pending_2fa connection to a user, and back to pending_2fa or anonymous.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables rebinding a live connection to another identity on refresh. Off by default: a refresh with a token of another user disconnects the connection."`
	// AllowDowngrade allows a refresh of an authenticated connection to an anonymous token, which keeps
	// the connection and drops its user channels. Only read when Enabled is set.
	AllowDowngrade bool `mapstructure:"allow_downgrade" json:"allow_downgrade" envconfig:"allow_downgrade" yaml:"allow_downgrade" toml:"allow_downgrade" doc:"Allows a refresh of an authenticated connection to an anonymous token, which keeps the connection and drops its user channels. Only read when rebind is enabled. Cannot be set together with client.concurrency above 1. Off by default: such a refresh disconnects the connection."`
}
