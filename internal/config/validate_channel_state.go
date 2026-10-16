package config

import (
	"errors"
	"fmt"
	"slices"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// maxChannelStatePartitions is the upper bound of channel_state.num_partitions.
const maxChannelStatePartitions = 4096

// validateChannelState checks the channel_state settings of the Redis presence manager. Both placements
// are checked: engine.redis.channel_state and presence_manager.redis.channel_state.
func validateChannelState(cfg Config) error {
	if err := validateChannelStateSettings(cfg.Engine.Redis.ChannelState); err != nil {
		return fmt.Errorf("in engine.redis.channel_state: %v", err)
	}
	if err := validateChannelStateSettings(cfg.PresenceManager.Redis.ChannelState); err != nil {
		return fmt.Errorf("in presence_manager.redis.channel_state: %v", err)
	}
	return nil
}

func validateChannelStateSettings(s configtypes.RedisChannelState) error {
	if s.NumPartitions < 1 || s.NumPartitions > maxChannelStatePartitions {
		return fmt.Errorf("num_partitions must be between 1 and %d, got %d", maxChannelStatePartitions, s.NumPartitions)
	}
	if s.VacatedEventDelay < 0 {
		return fmt.Errorf("vacated_event_delay must not be negative, got %s", s.VacatedEventDelay)
	}
	return nil
}

// validateChannelStateOptions checks the state_proxy_enabled and cache_empty_proxy_enabled options of
// a namespace (or without_namespace).
func validateChannelStateOptions(c configtypes.ChannelOptions, proxyNames []string, cfg Config) error {
	if c.StateProxyEnabled {
		if !c.Presence {
			return errors.New("state_proxy_enabled requires presence")
		}
		if err := validateProxy("default", cfg.Channel.Proxy.State); err != nil {
			return fmt.Errorf("in channel.proxy.state: %v", err)
		}
		if !usesRedisPresenceManager(cfg) {
			return errors.New("state_proxy_enabled requires the redis presence manager (engine.type redis or presence_manager.type redis)")
		}
	}

	if (c.CacheEmptyProxyEnabled || c.CacheEmptyProxyName != "") && !slices.Contains(proxyNames, c.CacheEmptyProxyName) {
		return fmt.Errorf("cache empty proxy with name \"%s\" not found", c.CacheEmptyProxyName)
	}
	if c.CacheEmptyProxyEnabled && c.CacheEmptyProxyName == DefaultProxyName {
		if err := validateProxy("default", cfg.Channel.Proxy.CacheEmpty); err != nil {
			return fmt.Errorf("in channel.proxy.cache_empty: %v", err)
		}
	}
	if c.CacheEmptyProxyEnabled && c.ForceRecoveryMode != "cache" {
		return errors.New("cache_empty_proxy_enabled requires force_recovery_mode \"cache\"")
	}
	return nil
}

// usesRedisPresenceManager reports whether the presence manager is Redis: a separate presence manager
// of type redis, or the engine of type redis when no separate presence manager is enabled.
func usesRedisPresenceManager(cfg Config) bool {
	if cfg.PresenceManager.Enabled {
		return cfg.PresenceManager.Type == "redis"
	}
	return cfg.Engine.Type == "redis"
}
