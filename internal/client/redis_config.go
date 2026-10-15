package client

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// redisConfigOf returns the Redis configuration and the prefix of a Redis user of the rate limits or of the dedup: its
// own, or the engine's when reuseFromEngine is set.
func redisConfigOf(
	reuseFromEngine bool,
	own configtypes.RedisPrefixed,
	engine configtypes.RedisPrefixed,
) (configtypes.Redis, string) {
	if reuseFromEngine {
		return engine.Redis, engine.Prefix
	}
	return own.Redis, own.Prefix
}
