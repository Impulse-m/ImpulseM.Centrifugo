package configtypes

// UserBlock is a configuration for user blocking. Blocked user connections are disconnected
// with 4500 code and can't connect or refresh while the block is active.
type UserBlock struct {
	// Enabled turns on user block checks. Enabled by default.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enabled turns on user block checks. Enabled by default."`
	// StorageType is a storage for block entries. Only memory is supported at the moment.
	StorageType string `mapstructure:"storage_type" default:"memory" json:"storage_type" envconfig:"storage_type" yaml:"storage_type" toml:"storage_type" doc:"A storage for block entries. Only memory is supported at the moment."`
}

// TokenRevoke is a configuration for revoking connection tokens by session ID.
type TokenRevoke struct {
	// Enabled turns on session revocation checks. Enabled by default.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enabled turns on session revocation checks. Enabled by default."`
	// StorageType is a storage for revoked sessions. Only memory is supported at the moment.
	StorageType string `mapstructure:"storage_type" default:"memory" json:"storage_type" envconfig:"storage_type" yaml:"storage_type" toml:"storage_type" doc:"A storage for revoked sessions. Only memory is supported at the moment."`
	// UIDClaim is a name of connection token claim with session ID. Default is jti.
	UIDClaim string `mapstructure:"uid_claim" default:"jti" json:"uid_claim" envconfig:"uid_claim" yaml:"uid_claim" toml:"uid_claim" doc:"A name of connection token claim with session ID. Default is jti."`
}

// UserTokensInvalidate is a configuration for invalidating all tokens of a user.
type UserTokensInvalidate struct {
	// Enabled turns on user token invalidation checks. Enabled by default.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enabled turns on user token invalidation checks. Enabled by default."`
	// StorageType is a storage for invalidation entries. Only memory is supported at the moment.
	StorageType string `mapstructure:"storage_type" default:"memory" json:"storage_type" envconfig:"storage_type" yaml:"storage_type" toml:"storage_type" doc:"A storage for invalidation entries. Only memory is supported at the moment."`
	// GenerationClaim is a name of connection token claim with revocation generation. Empty means
	// generation checks are off.
	GenerationClaim string `mapstructure:"generation_claim" json:"generation_claim" envconfig:"generation_claim" yaml:"generation_claim" toml:"generation_claim" doc:"A name of connection token claim with revocation generation. Empty means generation checks are off."`
}

// AccessRevoke is a configuration for access revocation internals: live disconnects and cluster
// notifications.
type AccessRevoke struct {
	// DisconnectRate is a maximum number of live connections disconnected per second on this node
	// because of access revocation.
	DisconnectRate int `mapstructure:"disconnect_rate" default:"1000" json:"disconnect_rate" envconfig:"disconnect_rate" yaml:"disconnect_rate" toml:"disconnect_rate" doc:"A maximum number of live connections disconnected per second on this node because of access revocation."`
	// DisconnectQueueSize is a size of the queue of pending live disconnects. Disconnects beyond
	// the queue size are dropped and retried by user or session once the queue has room.
	DisconnectQueueSize int `mapstructure:"disconnect_queue_size" default:"100000" json:"disconnect_queue_size" envconfig:"disconnect_queue_size" yaml:"disconnect_queue_size" toml:"disconnect_queue_size" doc:"A size of the queue of pending live disconnects. Disconnects beyond the queue size are dropped and retried by user or session once the queue has room."`
	// NotifyRate is a maximum number of access revocation notifications sent to other nodes per second.
	NotifyRate int `mapstructure:"notify_rate" default:"100" json:"notify_rate" envconfig:"notify_rate" yaml:"notify_rate" toml:"notify_rate" doc:"A maximum number of access revocation notifications sent to other nodes per second."`
	// DefaultTTL is how long a revocation entry is kept when the request does not set expiration time.
	DefaultTTL Duration `mapstructure:"default_ttl" default:"20m" json:"default_ttl" envconfig:"default_ttl" yaml:"default_ttl" toml:"default_ttl" doc:"How long a revocation entry is kept when the request does not set expiration time."`
	// MaxTTL is the longest time a revocation entry may be kept, counted from the time it is applied. The API refuses
	// a larger expiration time, and the Kafka and notify paths clamp it. Zero means no limit.
	MaxTTL Duration `mapstructure:"max_ttl" json:"max_ttl" envconfig:"max_ttl" yaml:"max_ttl" toml:"max_ttl" doc:"The longest time a revocation entry may be kept, counted from the time it is applied. The API refuses a larger expiration time, and the Kafka and notify paths clamp it. Zero means no limit."`
	// SweepInterval is the shortest time between two removals of expired revocation entries from memory.
	SweepInterval Duration `mapstructure:"sweep_interval" default:"30s" json:"sweep_interval" envconfig:"sweep_interval" yaml:"sweep_interval" toml:"sweep_interval" doc:"The shortest time between two removals of expired revocation entries from memory. An expired entry is removed at most this long after it expires, and an entry written already expired at most this long after the write; nothing runs while no entry is due."`
	// Source is a source of access revocation events for this node. Empty type means no source.
	Source AccessRevokeSource `mapstructure:"source" json:"source" envconfig:"source" yaml:"source" toml:"source" doc:"A source of access revocation events for this node. Empty type means no source."`
}

// AccessRevokeSource is a configuration of a replayable log of access revocation events. Every node
// reads all events from it, rebuilds its revocation state after restart and gates connects until it caught up.
type AccessRevokeSource struct {
	// Type is a source type. Empty means no source. Known types: kafka.
	Type string `mapstructure:"type" json:"type" envconfig:"type" yaml:"type" toml:"type" doc:"A source type. Empty means no source. Known types: kafka."`
	// Window is how far back in time the source is read at start. Events are enforced only within
	// this window of their record timestamp. Must be greater or equal to access_revoke.default_ttl.
	Window Duration `mapstructure:"window" default:"30m" json:"window" envconfig:"window" yaml:"window" toml:"window" doc:"How far back in time the source is read at start. Events are enforced only within this window of their record timestamp. Must be greater or equal to access_revoke.default_ttl."`
	// StartupTimeout is how long the node waits to catch up with the source before becoming ready anyway.
	StartupTimeout Duration `mapstructure:"startup_timeout" default:"60s" json:"startup_timeout" envconfig:"startup_timeout" yaml:"startup_timeout" toml:"startup_timeout" doc:"How long the node waits to catch up with the source before becoming ready anyway."`
	// LagThreshold is how long the node may lag behind the source before it is considered lagging.
	LagThreshold Duration `mapstructure:"lag_threshold" default:"30s" json:"lag_threshold" envconfig:"lag_threshold" yaml:"lag_threshold" toml:"lag_threshold" doc:"How long the node may lag behind the source before it is considered lagging."`
	// RejectConnectOnLag rejects new connections while lagging. It also disables startup timeout:
	// the node waits for catch up indefinitely. Note that when all nodes lag this is a cluster-wide outage.
	RejectConnectOnLag bool `mapstructure:"reject_connect_on_lag" json:"reject_connect_on_lag" envconfig:"reject_connect_on_lag" yaml:"reject_connect_on_lag" toml:"reject_connect_on_lag" doc:"RejectConnectOnLag rejects new connections while lagging. It also disables startup timeout: the node waits for catch up indefinitely. Note that when all nodes lag this is a cluster-wide outage."`
	// Kafka is a configuration for the kafka source type.
	Kafka AccessRevokeKafkaSource `mapstructure:"kafka" json:"kafka" envconfig:"kafka" yaml:"kafka" toml:"kafka" doc:"A configuration for the kafka source type."`
}

// AccessRevokeKafkaSource is a configuration for the kafka access revocation source.
type AccessRevokeKafkaSource struct {
	// Brokers is a list of Kafka brokers.
	Brokers []string `mapstructure:"brokers" json:"brokers" envconfig:"brokers" yaml:"brokers" toml:"brokers" doc:"A list of Kafka brokers."`
	// Topic is a Kafka topic with access revocation events. Use cleanup.policy=delete, not compact.
	Topic string `mapstructure:"topic" json:"topic" envconfig:"topic" yaml:"topic" toml:"topic" doc:"A Kafka topic with access revocation events. Use cleanup.policy=delete, not compact."`
	// MethodHeader is a header name with the access revocation method name, such as block_user.
	MethodHeader string `mapstructure:"method_header" default:"centrifugo-method" json:"method_header" envconfig:"method_header" yaml:"method_header" toml:"method_header" doc:"A header name with the access revocation method name, such as block_user."`
	// FetchMaxWait is the maximum time to wait for records when polling.
	FetchMaxWait Duration `mapstructure:"fetch_max_wait" default:"2s" json:"fetch_max_wait" envconfig:"fetch_max_wait" yaml:"fetch_max_wait" toml:"fetch_max_wait" doc:"The maximum time to wait for records when polling."`
	// TLS for the connection to Kafka.
	TLS TLSConfig `mapstructure:"tls" json:"tls" envconfig:"tls" yaml:"tls" toml:"tls" doc:"TLS for the connection to Kafka."`
	// SASLMechanism when not empty enables SASL auth.
	SASLMechanism string `mapstructure:"sasl_mechanism" json:"sasl_mechanism" envconfig:"sasl_mechanism" yaml:"sasl_mechanism" toml:"sasl_mechanism" doc:"SASLMechanism when not empty enables SASL auth."`
	SASLUser      string `mapstructure:"sasl_user" json:"sasl_user" envconfig:"sasl_user" yaml:"sasl_user" toml:"sasl_user" doc:"SASL username for the access revocation Kafka source."`
	SASLPassword  string `mapstructure:"sasl_password" json:"sasl_password" envconfig:"sasl_password" yaml:"sasl_password" toml:"sasl_password" doc:"SASL password for the access revocation Kafka source."`
	// AssumeRoleARN, when set with sasl_mechanism aws-msk-iam, obtains MSK IAM credentials by STS AssumeRole.
	AssumeRoleARN string `mapstructure:"assume_role_arn" json:"assume_role_arn" envconfig:"assume_role_arn" yaml:"assume_role_arn" toml:"assume_role_arn" doc:"AssumeRoleARN, when set with sasl_mechanism aws-msk-iam, obtains MSK IAM credentials by STS AssumeRole."`
}
