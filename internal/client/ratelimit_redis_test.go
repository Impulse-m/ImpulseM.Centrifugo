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
