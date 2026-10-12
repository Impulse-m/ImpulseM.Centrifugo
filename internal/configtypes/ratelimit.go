package configtypes

import (
	"encoding/json"
)

// RateLimit is a configuration of operation rate limits. See PRO docs for details.
type RateLimit struct {
	// ClientCommand is a per-connection rate limit of client commands.
	ClientCommand RateLimitClientCommand `mapstructure:"client_command" json:"client_command" envconfig:"client_command" yaml:"client_command" toml:"client_command"`
	// UserCommand is a per-user in-memory rate limit of client commands.
	UserCommand RateLimitUserCommand `mapstructure:"user_command" json:"user_command" envconfig:"user_command" yaml:"user_command" toml:"user_command"`
	// RedisUserCommand is a per-user rate limit of client commands with Redis as a state storage.
	RedisUserCommand RateLimitRedisUserCommand `mapstructure:"redis_user_command" json:"redis_user_command" envconfig:"redis_user_command" yaml:"redis_user_command" toml:"redis_user_command"`
	// ClientError is a per-connection limit of protocol errors. Connection is disconnected when exceeded.
	ClientError RateLimitClientError `mapstructure:"client_error" json:"client_error" envconfig:"client_error" yaml:"client_error" toml:"client_error"`
}

// RateLimitBucket is a single token bucket: Rate operations are allowed per Interval.
type RateLimitBucket struct {
	// Interval is a period of time for Rate.
	Interval Duration `mapstructure:"interval" json:"interval" envconfig:"interval" yaml:"interval" toml:"interval"`
	// Rate is a number of operations allowed per Interval. It is also a burst size.
	Rate int `mapstructure:"rate" json:"rate" envconfig:"rate" yaml:"rate" toml:"rate"`
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
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Buckets is a list of token buckets. All of them must allow the operation.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets"`
}

// RateLimitNamespaceOverride replaces base channel operation buckets for channels in a namespace.
type RateLimitNamespaceOverride struct {
	// NamespaceName is a name of channel namespace to apply override to.
	NamespaceName string `mapstructure:"namespace_name" json:"namespace_name" envconfig:"namespace_name" yaml:"namespace_name" toml:"namespace_name"`
	// Enabled enables the override.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Buckets is a list of token buckets used instead of base buckets for the namespace.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets"`
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
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Buckets is a list of token buckets. All of them must allow the operation.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets"`
	// NamespaceOverrides allows replacing buckets for channels of specific namespaces.
	NamespaceOverrides RateLimitNamespaceOverrides `mapstructure:"namespace_overrides" json:"namespace_overrides" envconfig:"namespace_overrides" yaml:"namespace_overrides" toml:"namespace_overrides"`
}

// RateLimitMethodOverride replaces RPC buckets for a specific RPC method.
type RateLimitMethodOverride struct {
	// Method is an RPC method name to apply override to.
	Method string `mapstructure:"method" json:"method" envconfig:"method" yaml:"method" toml:"method"`
	// Enabled enables the override.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Buckets is a list of token buckets used instead of base RPC buckets for the method.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets"`
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
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Buckets is a list of token buckets. All of them must allow the operation.
	Buckets RateLimitBucketList `mapstructure:"buckets" json:"buckets" envconfig:"buckets" yaml:"buckets" toml:"buckets"`
	// MethodOverrides allows replacing buckets for specific RPC methods.
	MethodOverrides RateLimitMethodOverrides `mapstructure:"method_overrides" json:"method_overrides" envconfig:"method_overrides" yaml:"method_overrides" toml:"method_overrides"`
	// MethodOverride is a deprecated map form of MethodOverrides. Use MethodOverrides instead. Can't be used together with it.
	MethodOverride RateLimitMethodOverrideMap `mapstructure:"method_override" json:"method_override" envconfig:"method_override" yaml:"method_override" toml:"method_override"`
}

// RateLimitClientCommand configures per-connection rate limits of client commands.
type RateLimitClientCommand struct {
	// Enabled enables per-connection command rate limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Total caps the combined rate of all limited commands from a connection.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total"`
	// Default buckets are used for operations which have no explicit buckets.
	Default RateLimitBuckets `mapstructure:"default" json:"default" envconfig:"default" yaml:"default" toml:"default"`
	// Subscribe operation buckets.
	Subscribe RateLimitChannelBuckets `mapstructure:"subscribe" json:"subscribe" envconfig:"subscribe" yaml:"subscribe" toml:"subscribe"`
	// Unsubscribe operation buckets.
	Unsubscribe RateLimitChannelBuckets `mapstructure:"unsubscribe" json:"unsubscribe" envconfig:"unsubscribe" yaml:"unsubscribe" toml:"unsubscribe"`
	// Publish operation buckets.
	Publish RateLimitChannelBuckets `mapstructure:"publish" json:"publish" envconfig:"publish" yaml:"publish" toml:"publish"`
	// History operation buckets.
	History RateLimitChannelBuckets `mapstructure:"history" json:"history" envconfig:"history" yaml:"history" toml:"history"`
	// Presence operation buckets.
	Presence RateLimitChannelBuckets `mapstructure:"presence" json:"presence" envconfig:"presence" yaml:"presence" toml:"presence"`
	// PresenceStats operation buckets.
	PresenceStats RateLimitChannelBuckets `mapstructure:"presence_stats" json:"presence_stats" envconfig:"presence_stats" yaml:"presence_stats" toml:"presence_stats"`
	// Refresh operation buckets.
	Refresh RateLimitBuckets `mapstructure:"refresh" json:"refresh" envconfig:"refresh" yaml:"refresh" toml:"refresh"`
	// SubRefresh operation buckets.
	SubRefresh RateLimitChannelBuckets `mapstructure:"sub_refresh" json:"sub_refresh" envconfig:"sub_refresh" yaml:"sub_refresh" toml:"sub_refresh"`
	// RPC operation buckets.
	RPC RateLimitRPCBuckets `mapstructure:"rpc" json:"rpc" envconfig:"rpc" yaml:"rpc" toml:"rpc"`
	// MapPublish operation buckets. Not supported in this build.
	MapPublish RateLimitChannelBuckets `mapstructure:"map_publish" json:"map_publish" envconfig:"map_publish" yaml:"map_publish" toml:"map_publish"`
	// MapRemove operation buckets. Not supported in this build.
	MapRemove RateLimitChannelBuckets `mapstructure:"map_remove" json:"map_remove" envconfig:"map_remove" yaml:"map_remove" toml:"map_remove"`
	// Track operation buckets. Not supported in this build.
	Track RateLimitChannelBuckets `mapstructure:"track" json:"track" envconfig:"track" yaml:"track" toml:"track"`
	// Untrack operation buckets. Not supported in this build.
	Untrack RateLimitChannelBuckets `mapstructure:"untrack" json:"untrack" envconfig:"untrack" yaml:"untrack" toml:"untrack"`
}

// RateLimitUserCommand configures per-user in-memory rate limits of client commands.
type RateLimitUserCommand struct {
	// Enabled enables per-user command rate limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Total caps the combined rate of all limited commands from a user.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total"`
	// Default buckets are used for operations which have no explicit buckets.
	Default RateLimitBuckets `mapstructure:"default" json:"default" envconfig:"default" yaml:"default" toml:"default"`
	// Connect operation buckets.
	Connect RateLimitBuckets `mapstructure:"connect" json:"connect" envconfig:"connect" yaml:"connect" toml:"connect"`
	// Subscribe operation buckets.
	Subscribe RateLimitChannelBuckets `mapstructure:"subscribe" json:"subscribe" envconfig:"subscribe" yaml:"subscribe" toml:"subscribe"`
	// Unsubscribe operation buckets.
	Unsubscribe RateLimitChannelBuckets `mapstructure:"unsubscribe" json:"unsubscribe" envconfig:"unsubscribe" yaml:"unsubscribe" toml:"unsubscribe"`
	// Publish operation buckets.
	Publish RateLimitChannelBuckets `mapstructure:"publish" json:"publish" envconfig:"publish" yaml:"publish" toml:"publish"`
	// History operation buckets.
	History RateLimitChannelBuckets `mapstructure:"history" json:"history" envconfig:"history" yaml:"history" toml:"history"`
	// Presence operation buckets.
	Presence RateLimitChannelBuckets `mapstructure:"presence" json:"presence" envconfig:"presence" yaml:"presence" toml:"presence"`
	// PresenceStats operation buckets.
	PresenceStats RateLimitChannelBuckets `mapstructure:"presence_stats" json:"presence_stats" envconfig:"presence_stats" yaml:"presence_stats" toml:"presence_stats"`
	// Refresh operation buckets.
	Refresh RateLimitBuckets `mapstructure:"refresh" json:"refresh" envconfig:"refresh" yaml:"refresh" toml:"refresh"`
	// SubRefresh operation buckets.
	SubRefresh RateLimitChannelBuckets `mapstructure:"sub_refresh" json:"sub_refresh" envconfig:"sub_refresh" yaml:"sub_refresh" toml:"sub_refresh"`
	// RPC operation buckets.
	RPC RateLimitRPCBuckets `mapstructure:"rpc" json:"rpc" envconfig:"rpc" yaml:"rpc" toml:"rpc"`
	// MapPublish operation buckets. Not supported in this build.
	MapPublish RateLimitChannelBuckets `mapstructure:"map_publish" json:"map_publish" envconfig:"map_publish" yaml:"map_publish" toml:"map_publish"`
	// MapRemove operation buckets. Not supported in this build.
	MapRemove RateLimitChannelBuckets `mapstructure:"map_remove" json:"map_remove" envconfig:"map_remove" yaml:"map_remove" toml:"map_remove"`
	// Track operation buckets. Not supported in this build.
	Track RateLimitChannelBuckets `mapstructure:"track" json:"track" envconfig:"track" yaml:"track" toml:"track"`
	// Untrack operation buckets. Not supported in this build.
	Untrack RateLimitChannelBuckets `mapstructure:"untrack" json:"untrack" envconfig:"untrack" yaml:"untrack" toml:"untrack"`
}

// RateLimitRedis is a Redis configuration of per-user Redis rate limits.
type RateLimitRedis struct {
	// ReuseFromEngine makes rate limits use Redis instances configured for the Redis engine.
	ReuseFromEngine bool `mapstructure:"reuse_from_engine" json:"reuse_from_engine" envconfig:"reuse_from_engine" yaml:"reuse_from_engine" toml:"reuse_from_engine"`
	// RedisPrefixed is a Redis configuration. Ignored when ReuseFromEngine is set.
	RedisPrefixed `mapstructure:",squash" yaml:",inline"`
}

// RateLimitRedisUserCommand configures per-user rate limits of client commands with Redis as a state storage.
type RateLimitRedisUserCommand struct {
	// Enabled enables per-user Redis command rate limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Redis is a Redis configuration of rate limits.
	Redis RateLimitRedis `mapstructure:"redis" json:"redis" envconfig:"redis" yaml:"redis" toml:"redis"`
	// Total is not supported for Redis rate limits. Setting it is a configuration error.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total"`
	// Default buckets are used for operations which have no explicit buckets.
	Default RateLimitBuckets `mapstructure:"default" json:"default" envconfig:"default" yaml:"default" toml:"default"`
	// Connect operation buckets.
	Connect RateLimitBuckets `mapstructure:"connect" json:"connect" envconfig:"connect" yaml:"connect" toml:"connect"`
	// Subscribe operation buckets.
	Subscribe RateLimitChannelBuckets `mapstructure:"subscribe" json:"subscribe" envconfig:"subscribe" yaml:"subscribe" toml:"subscribe"`
	// Unsubscribe operation buckets.
	Unsubscribe RateLimitChannelBuckets `mapstructure:"unsubscribe" json:"unsubscribe" envconfig:"unsubscribe" yaml:"unsubscribe" toml:"unsubscribe"`
	// Publish operation buckets.
	Publish RateLimitChannelBuckets `mapstructure:"publish" json:"publish" envconfig:"publish" yaml:"publish" toml:"publish"`
	// History operation buckets.
	History RateLimitChannelBuckets `mapstructure:"history" json:"history" envconfig:"history" yaml:"history" toml:"history"`
	// Presence operation buckets.
	Presence RateLimitChannelBuckets `mapstructure:"presence" json:"presence" envconfig:"presence" yaml:"presence" toml:"presence"`
	// PresenceStats operation buckets.
	PresenceStats RateLimitChannelBuckets `mapstructure:"presence_stats" json:"presence_stats" envconfig:"presence_stats" yaml:"presence_stats" toml:"presence_stats"`
	// Refresh operation buckets.
	Refresh RateLimitBuckets `mapstructure:"refresh" json:"refresh" envconfig:"refresh" yaml:"refresh" toml:"refresh"`
	// SubRefresh operation buckets.
	SubRefresh RateLimitChannelBuckets `mapstructure:"sub_refresh" json:"sub_refresh" envconfig:"sub_refresh" yaml:"sub_refresh" toml:"sub_refresh"`
	// RPC operation buckets.
	RPC RateLimitRPCBuckets `mapstructure:"rpc" json:"rpc" envconfig:"rpc" yaml:"rpc" toml:"rpc"`
	// MapPublish operation buckets. Not supported in this build.
	MapPublish RateLimitChannelBuckets `mapstructure:"map_publish" json:"map_publish" envconfig:"map_publish" yaml:"map_publish" toml:"map_publish"`
	// MapRemove operation buckets. Not supported in this build.
	MapRemove RateLimitChannelBuckets `mapstructure:"map_remove" json:"map_remove" envconfig:"map_remove" yaml:"map_remove" toml:"map_remove"`
	// Track operation buckets. Not supported in this build.
	Track RateLimitChannelBuckets `mapstructure:"track" json:"track" envconfig:"track" yaml:"track" toml:"track"`
	// Untrack operation buckets. Not supported in this build.
	Untrack RateLimitChannelBuckets `mapstructure:"untrack" json:"untrack" envconfig:"untrack" yaml:"untrack" toml:"untrack"`
}

// RateLimitClientError configures per-connection limits of protocol errors.
type RateLimitClientError struct {
	// Enabled enables per-connection error limits.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`
	// Total is a bucket of errors. Connection is disconnected when it is exhausted.
	Total RateLimitBuckets `mapstructure:"total" json:"total" envconfig:"total" yaml:"total" toml:"total"`
}
