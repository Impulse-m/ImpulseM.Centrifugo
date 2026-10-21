package configtypes

import (
	"encoding/json"
)

// RateLimit is a configuration of operation rate limits. See PRO docs for details.
type RateLimit struct {
	// ClientCommand is a per-connection rate limit of client commands.
	ClientCommand RateLimitClientCommand `mapstructure:"client_command" json:"client_command" envconfig:"client_command" yaml:"client_command" toml:"client_command" doc:"A per-connection rate limit of client commands."`
	// UserCommand is a per-user in-memory rate limit of client commands.
	UserCommand RateLimitUserCommand `mapstructure:"user_command" json:"user_command" envconfig:"user_command" yaml:"user_command" toml:"user_command" doc:"A per-user in-memory rate limit of client commands."`
	// RedisUserCommand is a per-user rate limit of client commands with Redis as a state storage.
	RedisUserCommand RateLimitRedisUserCommand `mapstructure:"redis_user_command" json:"redis_user_command" envconfig:"redis_user_command" yaml:"redis_user_command" toml:"redis_user_command" doc:"A per-user rate limit of client commands with Redis as a state storage."`
	// ClientError is a per-connection limit of protocol errors. Connection is disconnected when exceeded.
	ClientError RateLimitClientError `mapstructure:"client_error" json:"client_error" envconfig:"client_error" yaml:"client_error" toml:"client_error" doc:"A per-connection limit of protocol errors. Connection is disconnected when exceeded."`
}

// RateLimitBucket is a single token bucket: Rate operations are allowed per Interval.
type RateLimitBucket struct {
	// Interval is a period of time for Rate.
	Interval Duration `mapstructure:"interval" json:"interval" envconfig:"interval" yaml:"interval" toml:"interval" doc:"A period of time for Rate."`
	// Rate is a number of operations allowed per Interval. It is also a burst size.
	Rate int `mapstructure:"rate" json:"rate" envconfig:"rate" yaml:"rate" toml:"rate" doc:"A number of operations allowed per Interval. It is also a burst size."`
}

type RateLimitBucketList []RateLimitBucket

// Decode to implement the envconfig.Decoder interface
func (d *RateLimitBucketList) Decode(value string) error {
	var items RateLimitBucketList
	err := json.Unmarshal([]byte(value), &items)
	if err != nil {
		return err
	}
	*d = items
	return nil
}

// RateLimitBuckets is a set of buckets applied to an operation or to a special container.
type RateLimitBuckets struct {
	// Enabled enables the buckets.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the buckets."`
	// Algorithm of the buckets, see the doc of the field in RateLimitChannelBuckets.
	Algorithm string `mapstructure:"algorithm" json:"algorithm" envconfig:"algorithm" yaml:"algorithm" toml:"algorithm" doc:"Algorithm of the buckets: token_bucket (the default, GCRA), sliding_window (sliding window counter) or fixed_window. Empty means token_bucket. Each algorithm keeps its own state, so changing it starts the counters again. Window algorithms need an interval from 1s to 1h, in whole milliseconds, and a rate of at most 2097152. Read at start."`
	// Buckets is a list of token buckets. All of them must allow the operation.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets" doc:"A list of token buckets. All of them must allow the operation."`
}

// RateLimitNamespaceOverride replaces base channel operation buckets for channels in a namespace.
type RateLimitNamespaceOverride struct {
	// NamespaceName is a name of channel namespace to apply override to.
	NamespaceName string `mapstructure:"namespace_name" json:"namespace_name" envconfig:"namespace_name" yaml:"namespace_name" toml:"namespace_name" doc:"A name of channel namespace to apply override to."`
	// Enabled enables the override.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the override."`
	// Algorithm of the buckets of the override, see RateLimitBuckets.
	Algorithm string `mapstructure:"algorithm" json:"algorithm" envconfig:"algorithm" yaml:"algorithm" toml:"algorithm" doc:"Algorithm of the buckets: token_bucket (the default, GCRA), sliding_window (sliding window counter) or fixed_window. Empty means token_bucket. Each algorithm keeps its own state, so changing it starts the counters again. Window algorithms need an interval from 1s to 1h, in whole milliseconds, and a rate of at most 2097152. Read at start."`
	// Buckets is a list of token buckets used instead of base buckets for the namespace.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets" doc:"A list of token buckets used instead of base buckets for the namespace."`
}

type RateLimitNamespaceOverrides []RateLimitNamespaceOverride

// Decode to implement the envconfig.Decoder interface
func (d *RateLimitNamespaceOverrides) Decode(value string) error {
	var items RateLimitNamespaceOverrides
	err := json.Unmarshal([]byte(value), &items)
	if err != nil {
		return err
	}
	*d = items
	return nil
}

// RateLimitChannelBuckets are buckets of a channel operation. Namespace overrides are optional.
type RateLimitChannelBuckets struct {
	// Enabled enables the buckets.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the buckets."`
	// Algorithm of the buckets, see RateLimitBuckets.
	Algorithm string `mapstructure:"algorithm" json:"algorithm" envconfig:"algorithm" yaml:"algorithm" toml:"algorithm" doc:"Algorithm of the buckets: token_bucket (the default, GCRA), sliding_window (sliding window counter) or fixed_window. Empty means token_bucket. Each algorithm keeps its own state, so changing it starts the counters again. Window algorithms need an interval from 1s to 1h, in whole milliseconds, and a rate of at most 2097152. Read at start."`
	// Buckets is a list of token buckets. All of them must allow the operation.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets" doc:"A list of token buckets. All of them must allow the operation."`
	// NamespaceOverrides allows replacing buckets for channels of specific namespaces.
	NamespaceOverrides RateLimitNamespaceOverrides `mapstructure:"namespace_overrides" json:"namespace_overrides" envconfig:"namespace_overrides" yaml:"namespace_overrides" toml:"namespace_overrides" doc:"NamespaceOverrides allows replacing buckets for channels of specific namespaces."`
}

// RateLimitMethodOverride replaces RPC buckets for a specific RPC method.
type RateLimitMethodOverride struct {
	// Method is an RPC method name to apply override to.
	Method string `mapstructure:"method" json:"method" envconfig:"method" yaml:"method" toml:"method" doc:"An RPC method name to apply override to."`
	// Enabled enables the override.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the override."`
	// Algorithm of the buckets of the override, see RateLimitBuckets.
	Algorithm string `mapstructure:"algorithm" json:"algorithm" envconfig:"algorithm" yaml:"algorithm" toml:"algorithm" doc:"Algorithm of the buckets: token_bucket (the default, GCRA), sliding_window (sliding window counter) or fixed_window. Empty means token_bucket. Each algorithm keeps its own state, so changing it starts the counters again. Window algorithms need an interval from 1s to 1h, in whole milliseconds, and a rate of at most 2097152. Read at start."`
	// Buckets is a list of token buckets used instead of base RPC buckets for the method.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets" doc:"A list of token buckets used instead of base RPC buckets for the method."`
}

type RateLimitMethodOverrides []RateLimitMethodOverride

// Decode to implement the envconfig.Decoder interface
func (d *RateLimitMethodOverrides) Decode(value string) error {
	var items RateLimitMethodOverrides
	err := json.Unmarshal([]byte(value), &items)
	if err != nil {
		return err
	}
	*d = items
	return nil
}

// RateLimitMethodOverrideMap is a deprecated map form of RPC method overrides, keyed by method name.
type RateLimitMethodOverrideMap map[string]RateLimitBuckets

// Decode to implement the envconfig.Decoder interface
func (d *RateLimitMethodOverrideMap) Decode(value string) error {
	var items RateLimitMethodOverrideMap
	err := json.Unmarshal([]byte(value), &items)
	if err != nil {
		return err
	}
	*d = items
	return nil
}

// RateLimitRPCBuckets are buckets of RPC calls. Method overrides are optional.
type RateLimitRPCBuckets struct {
	// Enabled enables the buckets.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the buckets."`
	// Algorithm of the buckets, see RateLimitBuckets.
	Algorithm string `mapstructure:"algorithm" json:"algorithm" envconfig:"algorithm" yaml:"algorithm" toml:"algorithm" doc:"Algorithm of the buckets: token_bucket (the default, GCRA), sliding_window (sliding window counter) or fixed_window. Empty means token_bucket. Each algorithm keeps its own state, so changing it starts the counters again. Window algorithms need an interval from 1s to 1h, in whole milliseconds, and a rate of at most 2097152. Read at start."`
	// Buckets is a list of token buckets. All of them must allow the operation.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets" doc:"A list of token buckets. All of them must allow the operation."`
	// MethodOverrides allows replacing buckets for specific RPC methods.
	MethodOverrides RateLimitMethodOverrides `mapstructure:"method_overrides" json:"method_overrides" envconfig:"method_overrides" yaml:"method_overrides" toml:"method_overrides" doc:"MethodOverrides allows replacing buckets for specific RPC methods."`
	// MethodOverride is a deprecated map form of MethodOverrides. Use MethodOverrides instead. Can't be used together with it.
	MethodOverride RateLimitMethodOverrideMap `mapstructure:"method_override" json:"method_override" envconfig:"method_override" yaml:"method_override" toml:"method_override" doc:"A deprecated map form of MethodOverrides. Use MethodOverrides instead. Can't be used together with it."`
}

// RateLimitOperations are the operation buckets shared by client command, user command and Redis user
// command limits. Each limit embeds it, so the keys are the same in every section.
type RateLimitOperations struct {
	// Default buckets are used for operations which have no explicit buckets.
	Default RateLimitBuckets `mapstructure:"default" json:"default" envconfig:"default" yaml:"default" toml:"default" doc:"Default buckets are used for operations which have no explicit buckets."`
	// Subscribe operation buckets.
	Subscribe RateLimitChannelBuckets `mapstructure:"subscribe" json:"subscribe" envconfig:"subscribe" yaml:"subscribe" toml:"subscribe" doc:"Subscribe operation buckets."`
	// Unsubscribe operation buckets.
	Unsubscribe RateLimitChannelBuckets `mapstructure:"unsubscribe" json:"unsubscribe" envconfig:"unsubscribe" yaml:"unsubscribe" toml:"unsubscribe" doc:"Unsubscribe operation buckets."`
	// Publish operation buckets.
	Publish RateLimitChannelBuckets `mapstructure:"publish" json:"publish" envconfig:"publish" yaml:"publish" toml:"publish" doc:"Publish operation buckets."`
	// History operation buckets.
	History RateLimitChannelBuckets `mapstructure:"history" json:"history" envconfig:"history" yaml:"history" toml:"history" doc:"History operation buckets."`
	// Presence operation buckets.
	Presence RateLimitChannelBuckets `mapstructure:"presence" json:"presence" envconfig:"presence" yaml:"presence" toml:"presence" doc:"Presence operation buckets."`
	// PresenceStats operation buckets.
	PresenceStats RateLimitChannelBuckets `mapstructure:"presence_stats" json:"presence_stats" envconfig:"presence_stats" yaml:"presence_stats" toml:"presence_stats" doc:"PresenceStats operation buckets."`
	// Refresh operation buckets.
	Refresh RateLimitBuckets `mapstructure:"refresh" json:"refresh" envconfig:"refresh" yaml:"refresh" toml:"refresh" doc:"Refresh operation buckets."`
	// SubRefresh operation buckets.
	SubRefresh RateLimitChannelBuckets `mapstructure:"sub_refresh" json:"sub_refresh" envconfig:"sub_refresh" yaml:"sub_refresh" toml:"sub_refresh" doc:"SubRefresh operation buckets."`
	// RPC operation buckets.
	RPC RateLimitRPCBuckets `mapstructure:"rpc" json:"rpc" envconfig:"rpc" yaml:"rpc" toml:"rpc" doc:"RPC operation buckets."`
	// MapPublish operation buckets.
	MapPublish RateLimitChannelBuckets `mapstructure:"map_publish" json:"map_publish" envconfig:"map_publish" yaml:"map_publish" toml:"map_publish" doc:"MapPublish operation buckets."`
	// MapRemove operation buckets.
	MapRemove RateLimitChannelBuckets `mapstructure:"map_remove" json:"map_remove" envconfig:"map_remove" yaml:"map_remove" toml:"map_remove" doc:"MapRemove operation buckets."`
	// Track operation buckets.
	Track RateLimitChannelBuckets `mapstructure:"track" json:"track" envconfig:"track" yaml:"track" toml:"track" doc:"Track operation buckets."`
	// Untrack operation buckets.
	Untrack RateLimitChannelBuckets `mapstructure:"untrack" json:"untrack" envconfig:"untrack" yaml:"untrack" toml:"untrack" doc:"Untrack operation buckets."`
}

// RateLimitClientCommand configures per-connection rate limits of client commands.
type RateLimitClientCommand struct {
	// Enabled enables per-connection command rate limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables per-connection command rate limits."`
	// Total caps the combined rate of all limited commands from a connection.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total" doc:"Total caps the combined rate of all limited commands from a connection."`
	// RateLimitOperations are the operation buckets of client commands.
	RateLimitOperations `mapstructure:",squash" yaml:",inline"`
}

// RateLimitUserCommand configures per-user in-memory rate limits of client commands.
type RateLimitUserCommand struct {
	// Enabled enables per-user command rate limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables per-user command rate limits."`
	// Total caps the combined rate of all limited commands from a user.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total" doc:"Total caps the combined rate of all limited commands from a user."`
	// Connect operation buckets.
	Connect RateLimitBuckets `mapstructure:"connect" json:"connect" envconfig:"connect" yaml:"connect" toml:"connect" doc:"Connect operation buckets."`
	// RateLimitOperations are the operation buckets of client commands of a user.
	RateLimitOperations `mapstructure:",squash" yaml:",inline"`
}

// RateLimitRedis is a Redis configuration of per-user Redis rate limits.
type RateLimitRedis struct {
	// ReuseFromEngine makes rate limits use Redis instances configured for the Redis engine.
	ReuseFromEngine bool `mapstructure:"reuse_from_engine" json:"reuse_from_engine" envconfig:"reuse_from_engine" yaml:"reuse_from_engine" toml:"reuse_from_engine" doc:"ReuseFromEngine makes rate limits use Redis instances configured for the Redis engine."`
	// RedisPrefixed is a Redis configuration. Ignored when ReuseFromEngine is set.
	RedisPrefixed `mapstructure:",squash" yaml:",inline"`
}

// RateLimitRedisUserCommand configures per-user rate limits of client commands with Redis as a state storage.
type RateLimitRedisUserCommand struct {
	// Enabled enables per-user Redis command rate limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables per-user Redis command rate limits."`
	// Redis is a Redis configuration of rate limits.
	Redis RateLimitRedis `mapstructure:"redis" json:"redis" envconfig:"redis" yaml:"redis" toml:"redis" doc:"A Redis configuration of rate limits."`
	// Total is not supported for Redis rate limits. Setting it is a configuration error.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total" doc:"Not supported for Redis rate limits. Setting it is a configuration error."`
	// Connect operation buckets.
	Connect RateLimitBuckets `mapstructure:"connect" json:"connect" envconfig:"connect" yaml:"connect" toml:"connect" doc:"Connect operation buckets."`
	// RateLimitOperations are the operation buckets of client commands of a user, kept in Redis.
	RateLimitOperations `mapstructure:",squash" yaml:",inline"`
}

// RateLimitClientError configures per-connection limits of protocol errors.
type RateLimitClientError struct {
	// Enabled enables per-connection error limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables per-connection error limits."`
	// Total is a bucket of errors. Connection is disconnected when it is exhausted.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total" doc:"A bucket of errors. Connection is disconnected when it is exhausted."`
}
