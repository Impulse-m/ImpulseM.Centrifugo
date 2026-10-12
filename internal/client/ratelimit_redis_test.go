package client

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

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
