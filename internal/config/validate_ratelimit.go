package config

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// Window algorithms (sliding_window and fixed_window) take intervals from windowMinInterval to windowMaxInterval, in
// whole milliseconds, and rates up to windowMaxRate. The caps keep every product of a window algorithm exact.
const (
	windowMinInterval = time.Second
	windowMaxInterval = time.Hour
	windowMaxRate     = 1 << 21
)

// The caps bound the state of a connection, which holds one slot per bucket of every scope of the plan, see
// ratelimit.CommandPlan. Each list of buckets is one scope, and every method override is one scope too.
const (
	rateLimitMaxBuckets         = 16
	rateLimitMaxMethodOverrides = 4096
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
	name     string
	total    *configtypes.RateLimitBuckets
	defaults *configtypes.RateLimitBuckets
	plain    []rateLimitBucketsEntry
	channels []rateLimitChannelEntry
	rpc      *configtypes.RateLimitRPCBuckets
	// tokenBucketOnly is set for sections which support the token bucket algorithm only.
	tokenBucketOnly bool
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
	if rc.OnError != "" && rc.OnError != configtypes.RateLimitOnErrorAllow && rc.OnError != configtypes.RateLimitOnErrorDeny {
		return fmt.Errorf("redis_user_command.on_error must be %s or %s", configtypes.RateLimitOnErrorAllow, configtypes.RateLimitOnErrorDeny)
	}
	if rc.Redis.ReuseFromEngine && rc.Enabled && c.Engine.Type != "redis" {
		return fmt.Errorf("redis_user_command.redis.reuse_from_engine requires engine.type to be redis")
	}

	if err := validateRateLimitSection(rateLimitSection{
		name:            "client_error",
		total:           &rl.ClientError.Total,
		tokenBucketOnly: true,
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
		if s.tokenBucketOnly && s.total.Enabled && s.total.Algorithm != "" && s.total.Algorithm != configtypes.RateLimitAlgorithmTokenBucket {
			return fmt.Errorf("%s.total: algorithm %q is not supported, client_error uses token_bucket only", s.name, s.total.Algorithm)
		}
		if err := validateRateLimitBuckets(s.total.Enabled, s.total.Algorithm, s.total.Buckets, true); err != nil {
			return fmt.Errorf("%s.total: %v", s.name, err)
		}
	}
	if s.defaults != nil {
		if err := validateRateLimitBuckets(s.defaults.Enabled, s.defaults.Algorithm, s.defaults.Buckets, true); err != nil {
			return fmt.Errorf("%s.default: %v", s.name, err)
		}
	}
	for _, entry := range s.plain {
		if err := validateRateLimitBuckets(entry.buckets.Enabled, entry.buckets.Algorithm, entry.buckets.Buckets, true); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
	}
	for _, entry := range s.channels {
		if err := validateRateLimitBuckets(entry.buckets.Enabled, entry.buckets.Algorithm, entry.buckets.Buckets, true); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
		if err := validateRateLimitNamespaceOverrides(entry.buckets.NamespaceOverrides, nsNames); err != nil {
			return fmt.Errorf("%s.%s: %v", s.name, entry.name, err)
		}
	}
	if s.rpc != nil {
		if err := validateRateLimitBuckets(s.rpc.Enabled, s.rpc.Algorithm, s.rpc.Buckets, true); err != nil {
			return fmt.Errorf("%s.rpc: %v", s.name, err)
		}
		if len(s.rpc.MethodOverride) > 0 && len(s.rpc.MethodOverrides) > 0 {
			return fmt.Errorf("%s.rpc: method_override and method_overrides cannot be used together", s.name)
		}
		if len(s.rpc.MethodOverrides) > rateLimitMaxMethodOverrides {
			return fmt.Errorf("%s.rpc: method_overrides must not have more than %d entries", s.name, rateLimitMaxMethodOverrides)
		}
		if err := validateRateLimitMethodNames(s.rpc.MethodOverrides); err != nil {
			return err
		}
		if err := validateRateLimitMethodOverrides(s.rpc.MethodOverrides); err != nil {
			return fmt.Errorf("%s.rpc: %v", s.name, err)
		}
		for method, override := range s.rpc.MethodOverride {
			if err := validateRateLimitBuckets(override.Enabled, override.Algorithm, override.Buckets, false); err != nil {
				return fmt.Errorf("%s.rpc.method_override.%s: %v", s.name, method, err)
			}
		}
	}
	return nil
}

// validateRateLimitBuckets validates bucket values and the algorithm of an enabled container. An enabled container
// without buckets is an error when requireBuckets is set. Window algorithms have their own limits on buckets.
func validateRateLimitBuckets(enabled bool, algorithm string, buckets configtypes.RateLimitBucketList, requireBuckets bool) error {
	if !enabled {
		return nil
	}
	if err := validateRateLimitAlgorithm(algorithm); err != nil {
		return err
	}
	if requireBuckets && len(buckets) == 0 {
		return fmt.Errorf("enabled buckets must not be empty")
	}
	if len(buckets) > rateLimitMaxBuckets {
		return fmt.Errorf("buckets must not have more than %d entries", rateLimitMaxBuckets)
	}
	window := configtypes.RateLimitAlgorithmWindows[algorithm]
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
		if window {
			if err := validateWindowBucket(bucket, algorithm); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateRateLimitAlgorithm reports an unknown algorithm. The empty name is the token bucket.
func validateRateLimitAlgorithm(algorithm string) error {
	if _, ok := configtypes.RateLimitAlgorithmWindows[algorithm]; ok || algorithm == "" {
		return nil
	}
	return fmt.Errorf("algorithm %q is unknown, use %s", algorithm, rateLimitAlgorithmList())
}

// rateLimitAlgorithmList returns the algorithm names of configtypes as a list for a message, such as "a, b or c".
func rateLimitAlgorithmList() string {
	names := configtypes.RateLimitAlgorithmNames
	last := len(names) - 1
	return strings.Join(names[:last], ", ") + " or " + names[last]
}

// validateWindowBucket validates the interval and rate of a bucket of a window algorithm.
func validateWindowBucket(bucket configtypes.RateLimitBucket, algorithm string) error {
	interval := bucket.Interval.ToDuration()
	if interval < windowMinInterval {
		return fmt.Errorf("bucket interval must be at least 1s for algorithm %s", algorithm)
	}
	if interval > windowMaxInterval {
		return fmt.Errorf("bucket interval must not be greater than 1h for algorithm %s", algorithm)
	}
	if interval%time.Millisecond != 0 {
		return fmt.Errorf("bucket interval must be a whole number of milliseconds for algorithm %s", algorithm)
	}
	if bucket.Rate > windowMaxRate {
		return fmt.Errorf("bucket rate must not be greater than %d for algorithm %s", windowMaxRate, algorithm)
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
		if err := validateRateLimitBuckets(override.Enabled, override.Algorithm, override.Buckets, false); err != nil {
			return fmt.Errorf("namespace override %s: %v", override.NamespaceName, err)
		}
		seen = append(seen, override.NamespaceName)
	}
	return nil
}

// validateRateLimitMethodNames refuses method names with a tilde. The tilde separates the scope name from the
// algorithm suffix in Redis keys, so a method name with a tilde could collide with another scope.
func validateRateLimitMethodNames(overrides configtypes.RateLimitMethodOverrides) error {
	for _, override := range overrides {
		if strings.Contains(override.Method, "~") {
			return fmt.Errorf("client.rate_limit method_overrides: method name %q must not contain \"~\"", override.Method)
		}
	}
	return nil
}

func validateRateLimitMethodOverrides(overrides configtypes.RateLimitMethodOverrides) error {
	seen := make([]string, 0, len(overrides))
	for _, override := range overrides {
		if slices.Contains(seen, override.Method) {
			return fmt.Errorf("duplicate method in method_overrides: %s", override.Method)
		}
		if err := validateRateLimitBuckets(override.Enabled, override.Algorithm, override.Buckets, false); err != nil {
			return fmt.Errorf("method override %s: %v", override.Method, err)
		}
		seen = append(seen, override.Method)
	}
	return nil
}
