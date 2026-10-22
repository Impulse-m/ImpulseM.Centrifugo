package redisshard

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/redis/rueidis"
)

// ParseRedisShardConfigs returns configs of all shards of redisConf. It validates them without connecting
// to Redis, so it returns configuration errors only: a Redis which is down does not make them fail.
func ParseRedisShardConfigs(redisConf configtypes.Redis) ([]RedisShardConfig, error) {
	configs, err := getRedisShardConfigs(redisConf)
	if err != nil {
		return nil, err
	}
	for i := range configs {
		if configs[i].ConnectTimeout == 0 {
			configs[i].ConnectTimeout = defaultRedisConnectTimeout
		}
		if configs[i].IOTimeout == 0 {
			configs[i].IOTimeout = defaultRedisIOTimeout
		}
		if err := validateRedisShardConfig(configs[i]); err != nil {
			return nil, err
		}
	}
	return configs, nil
}

// ConfigsOf is ParseRedisShardConfigs for the Redis configuration of a setting. Its error is prefixed with the name of
// the setting, so that the setting is named in the error.
func ConfigsOf(redisConf configtypes.Redis, setting string) ([]RedisShardConfig, error) {
	configs, err := ParseRedisShardConfigs(redisConf)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", setting, err)
	}
	return configs, nil
}

// errInvalidEscape replaces the escape error of the URL parser, whose text quotes the two bytes after a percent sign.
// Those bytes can be part of a password.
var errInvalidEscape = errors.New("invalid URL escape")

// redactURLError returns err with the URL of a url.Error replaced by a placeholder, so that a password in a Redis
// address cannot reach an error message. The wrapped error stays reachable through errors.As and errors.Unwrap, except
// that an escape error is replaced by errInvalidEscape. Other errors are returned as they are.
// tools.RedactedLogURLs is not used because a URL which does not parse becomes "<invalid_url>", which drops the error
// chain this function keeps.
func redactURLError(err error) error {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	cause := urlErr.Err
	var escapeErr url.EscapeError
	if errors.As(cause, &escapeErr) {
		cause = errInvalidEscape
	}
	return &url.Error{Op: urlErr.Op, URL: "<redacted>", Err: cause}
}

// validateRedisShardConfig checks the same configuration errors NewRedisShard reports before it connects.
// It does not connect. The replica client check is left to NewRedisShard, because cluster mode may be
// detected only after connecting.
func validateRedisShardConfig(conf RedisShardConfig) error {
	if len(conf.SentinelAddresses) > 0 {
		if conf.SentinelMasterName == "" {
			return errors.New("sentinel master name must be configured for Redis Sentinel setup")
		}
		return nil
	}
	if len(conf.ClusterAddresses) > 0 {
		return nil
	}
	addressOpts, err := optionsFromAddress(conf.Address, rueidis.ClientOption{})
	if err != nil {
		return fmt.Errorf("error processing Redis address: %v", err)
	}
	if addressOpts.IsSentinel && addressOpts.ClientOption.Sentinel.MasterSet == "" {
		return errors.New("sentinel master name must be configured for Redis Sentinel setup")
	}
	return nil
}

// NewRedisShards connects to all shards of configs. It is all-or-nothing: when any shard fails, the shards
// already connected are closed and the error is returned, so the caller never gets a partial set of shards.
func NewRedisShards(configs []RedisShardConfig) ([]*RedisShard, error) {
	shards := make([]*RedisShard, 0, len(configs))
	for _, conf := range configs {
		shard, err := NewRedisShard(conf)
		if err != nil {
			for _, connected := range shards {
				connected.Close()
			}
			return nil, err
		}
		shards = append(shards, shard)
	}
	return shards, nil
}
