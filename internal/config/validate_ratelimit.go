package config

import (
	"fmt"
	"slices"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// rateLimitBucketsEntry is a named non-channel operation buckets container.
type rateLimitBucketsEntry struct {
	name    string
	buckets *configtypes.RateLimitBuckets
}

// rateLimitChannelEntry is a named channel operation buckets container.
type rateLimitChannelEntry struct {
	name    string
	buckets *configtypes.RateLimitChannelBuckets
}

// rateLimitSection is a view over a rate limit section used for validation. Fields which
// are not supported by a section are left nil or empty.
type rateLimitSection struct {
	name        string
	total       *configtypes.RateLimitBuckets
	defaults    *configtypes.RateLimitBuckets
	plain       []rateLimitBucketsEntry
	channels    []rateLimitChannelEntry
	unsupported []rateLimitChannelEntry
	rpc         *configtypes.RateLimitRPCBuckets
}

// rateLimitOperationsSection returns the validation view of a section with the shared operation buckets.
// connect is nil for sections without connect buckets. It is validated before refresh, as before.
func rateLimitOperationsSection(
	name string,
	total *configtypes.RateLimitBuckets,
	ops *configtypes.RateLimitOperations,
	connect *configtypes.RateLimitBuckets,
) rateLimitSection {
	section := rateLimitSection{
		name:     name,
		total:    total,
		defaults: &ops.Default,
		plain:    []rateLimitBucketsEntry{{name: "refresh", buckets: &ops.Refresh}},
		channels: []rateLimitChannelEntry{
			{name: "subscribe", buckets: &ops.Subscribe},
			{name: "unsubscribe", buckets: &ops.Unsubscribe},
			{name: "publish", buckets: &ops.Publish},
			{name: "history", buckets: &ops.History},
			{name: "presence", buckets: &ops.Presence},
			{name: "presence_stats", buckets: &ops.PresenceStats},
			{name: "sub_refresh", buckets: &ops.SubRefresh},
		},
		unsupported: []rateLimitChannelEntry{
			{name: "map_publish", buckets: &ops.MapPublish},
			{name: "map_remove", buckets: &ops.MapRemove},
			{name: "track", buckets: &ops.Track},
			{name: "untrack", buckets: &ops.Untrack},
		},
		rpc: &ops.RPC,
	}
	if connect != nil {
		section.plain = append([]rateLimitBucketsEntry{{name: "connect", buckets: connect}}, section.plain...)
	}
	return section
}

func validateRateLimit(c Config) error {
	nsNames := make([]string, 0, len(c.Channel.Namespaces))
	for _, ns := range c.Channel.Namespaces {
		nsNames = append(nsNames, ns.Name)
	}

	rl := &c.Client.RateLimit
	ccSection := rateLimitOperationsSection("client_command", &rl.ClientCommand.Total, &rl.ClientCommand.RateLimitOperations, nil)
	if err := validateRateLimitSection(ccSection, nsNames); err != nil {
		return err
	}

	ucSection := rateLimitOperationsSection("user_command", &rl.UserCommand.Total, &rl.UserCommand.RateLimitOperations, &rl.UserCommand.Connect)
	if err := validateRateLimitSection(ucSection, nsNames); err != nil {
		return err
	}

	rc := &rl.RedisUserCommand
	rcSection := rateLimitOperationsSection("redis_user_command", &rc.Total, &rc.RateLimitOperations, &rc.Connect)
	if err := validateRateLimitSection(rcSection, nsNames); err != nil {
		return err
	}
	if rc.Total.Enabled || len(rc.Total.Buckets) > 0 {
		return fmt.Errorf("redis_user_command.total is not supported, use per-operation buckets instead")
	}
	if rc.Redis.ReuseFromEngine && rc.Enabled && c.Engine.Type != "redis" {
		return fmt.Errorf("redis_user_command.redis.reuse_from_engine requires engine.type to be redis")
	}

	if err := validateRateLimitSection(rateLimitSection{
		name:  "client_error",
		total: &rl.ClientError.Total,
	}, nsNames); err != nil {
		return err
	}
	if rl.ClientError.Enabled && !rl.ClientError.Total.Enabled {
		return fmt.Errorf("client_error: enabled without total bucket has no effect, enable client_error.total")
	}
	return nil
}

func validateRateLimitSection(s rateLimitSection, nsNames []string) error {
	if s.total != nil {
		if err := validateRateLimitBuckets(s.total.Enabled, s.total.Buckets, true); err != nil {
			return fmt.Errorf("%s.total: %v", s.name, err)
		}
	}
	if s.defaults != nil {
		if err := validateRateLimitBuckets(s.defaults.Enabled, s.defaults.Buckets, true); err != nil {
			return fmt.Errorf("%s.default: %v", s.name, err)
		}
	}
	for _, entry := range s.plain {
		if err := validateRateLimitBuckets(entry.buckets.Enabled, entry.buckets.Buckets, true); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
	}
	for _, entry := range s.channels {
		if err := validateRateLimitBuckets(entry.buckets.Enabled, entry.buckets.Buckets, true); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
		if err := validateRateLimitNamespaceOverrides(entry.buckets.NamespaceOverrides, nsNames); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
	}
	for _, entry := range s.unsupported {
		if entry.buckets.Enabled || len(entry.buckets.Buckets) > 0 || len(entry.buckets.NamespaceOverrides) > 0 {
			return fmt.Errorf("%s.%s: not supported in this build", s.name, entry.name)
		}
	}
	if s.rpc != nil {
		if err := validateRateLimitBuckets(s.rpc.Enabled, s.rpc.Buckets, true); err != nil {
			return fmt.Errorf("%s.rpc: %v", s.name, err)
		}
		if len(s.rpc.MethodOverride) > 0 && len(s.rpc.MethodOverrides) > 0 {
			return fmt.Errorf("%s.rpc: method_override and method_overrides cannot be used together", s.name)
		}
		if err := validateRateLimitMethodOverrides(s.rpc.MethodOverrides); err != nil {
			return fmt.Errorf("%s.rpc: %v", s.name, err)
		}
		for method, override := range s.rpc.MethodOverride {
			if err := validateRateLimitBuckets(override.Enabled, override.Buckets, false); err != nil {
				return fmt.Errorf("%s.rpc.method_override.%s: %v", s.name, method, err)
			}
		}
	}
	return nil
}

// validateRateLimitBuckets validates bucket values of an enabled container. An enabled container
// without buckets is an error when requireBuckets is set.
func validateRateLimitBuckets(enabled bool, buckets configtypes.RateLimitBucketList, requireBuckets bool) error {
	if !enabled {
		return nil
	}
	if requireBuckets && len(buckets) == 0 {
		return fmt.Errorf("enabled buckets must not be empty")
	}
	for _, bucket := range buckets {
		if bucket.Interval.ToDuration() <= 0 {
			return fmt.Errorf("bucket interval must be greater than 0")
		}
		if bucket.Rate <= 0 {
			return fmt.Errorf("bucket rate must be greater than 0")
		}
		if bucket.Interval.ToDuration() < time.Duration(bucket.Rate) {
			return fmt.Errorf("bucket interval %s is too small for rate %d", bucket.Interval.ToDuration(), bucket.Rate)
		}
	}
	return nil
}

func validateRateLimitNamespaceOverrides(overrides configtypes.RateLimitNamespaceOverrides, nsNames []string) error {
	seen := make([]string, 0, len(overrides))
	for _, override := range overrides {
		if slices.Contains(seen, override.NamespaceName) {
			return fmt.Errorf("duplicate namespace_name in namespace_overrides: %s", override.NamespaceName)
		}
		if !slices.Contains(nsNames, override.NamespaceName) {
			return fmt.Errorf("namespace_name %s in namespace_overrides not found in channel.namespaces", override.NamespaceName)
		}
		if err := validateRateLimitBuckets(override.Enabled, override.Buckets, false); err != nil {
			return fmt.Errorf("namespace override %s: %v", override.NamespaceName, err)
		}
		seen = append(seen, override.NamespaceName)
	}
	return nil
}

func validateRateLimitMethodOverrides(overrides configtypes.RateLimitMethodOverrides) error {
	seen := make([]string, 0, len(overrides))
	for _, override := range overrides {
		if slices.Contains(seen, override.Method) {
			return fmt.Errorf("duplicate method in method_overrides: %s", override.Method)
		}
		if err := validateRateLimitBuckets(override.Enabled, override.Buckets, false); err != nil {
			return fmt.Errorf("method override %s: %v", override.Method, err)
		}
		seen = append(seen, override.Method)
	}
	return nil
}
