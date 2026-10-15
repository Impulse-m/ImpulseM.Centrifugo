package client

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/stretchr/testify/require"
)

func TestRedisRateLimitDisabledRegistersNothing(t *testing.T) {
	disabled := newUserRateLimitHandler(t, configtypes.RateLimit{
		RedisUserCommand: configtypes.RateLimitRedisUserCommand{Enabled: false},
	})
	require.Nil(t, disabled.redisLimiter)
	require.Empty(t, disabled.Services())
	require.True(t, disabled.userConnectAllowed("42"))
}

// TestRedisRateLimitAloneGivesNoConnectionRecord checks that the Redis user limit alone gives no connection a record: it
// needs only the user, and the client limiter, which gives each connection its state, is not set for it.
func TestRedisRateLimitAloneGivesNoConnectionRecord(t *testing.T) {
	h := newUserRateLimitHandler(t, configtypes.RateLimit{
		RedisUserCommand: configtypes.RateLimitRedisUserCommand{
			Enabled: true,
			Redis: configtypes.RateLimitRedis{
				RedisPrefixed: configtypes.RedisPrefixed{
					Redis: configtypes.Redis{Address: []string{"127.0.0.1:1"}},
				},
			},
		},
	})
	require.NotNil(t, h.redisLimiter)
	require.Nil(t, h.clientLimiter)
	require.False(t, h.configNeedsConnState(h.cfgContainer.ConfigRef()))
}

func TestRedisRateLimitConfigErrorIsFatal(t *testing.T) {
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	cfg.Client.RateLimit.RedisUserCommand = configtypes.RateLimitRedisUserCommand{
		Enabled: true,
		Redis: configtypes.RateLimitRedis{
			RedisPrefixed: configtypes.RedisPrefixed{
				Redis: configtypes.Redis{Address: []string{"not a valid address"}},
			},
		},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{})
	h.SetAccessRevoke(accessrevoke.NewStore(time.Minute), nil)
	require.ErrorContains(t, h.Setup(), "malformed connection address")
}
