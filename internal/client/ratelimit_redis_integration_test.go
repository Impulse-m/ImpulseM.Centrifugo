//go:build integration

package client

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// integrationRedisAddress is the Redis of integration tests, see docker-compose.yml.
const integrationRedisAddress = "localhost:6379"

// uniqueRedisPrefix returns a key prefix which no other test run uses.
func uniqueRedisPrefix() string {
	return fmt.Sprintf("clienttest%d", time.Now().UnixNano())
}

// redisRateLimitOf returns the Redis configuration of redis_user_command limits with prefix.
func redisRateLimitOf(prefix string) configtypes.RateLimitRedis {
	return configtypes.RateLimitRedis{
		RedisPrefixed: configtypes.RedisPrefixed{
			Prefix: prefix,
			Redis:  configtypes.Redis{Address: []string{integrationRedisAddress}},
		},
	}
}

// newRedisRateLimitHandler creates a handler like newUserRateLimitHandler, with the config changed by mutate.
func newRedisRateLimitHandler(t *testing.T, mutate func(cfg *config.Config)) *Handler {
	t.Helper()
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Client.Insecure = true
	mutate(&cfg)
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, hmacJWTVerifier(t, cfgContainer), nil, &ProxyMap{}, accessrevoke.NewStore(time.Minute), nil)
	require.NoError(t, h.Setup())
	return h
}

// unreachableRedisAddress returns an address which refuses connections: the port was just released.
func unreachableRedisAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func TestRedisRateLimitUnreachableRedisStartsAndAllows(t *testing.T) {
	address := unreachableRedisAddress(t)
	before := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)
	h := newRedisRateLimitHandler(t, func(cfg *config.Config) {
		cfg.Client.RateLimit.RedisUserCommand = configtypes.RateLimitRedisUserCommand{
			Enabled: true,
			Redis: configtypes.RateLimitRedis{
				RedisPrefixed: configtypes.RedisPrefixed{
					Prefix: uniqueRedisPrefix(),
					Redis:  configtypes.Redis{Address: []string{address}},
				},
			},
			Connect:     rateLimitBucketsOf(time.Minute, 1),
			Unsubscribe: rateLimitChannelOf(time.Minute, 1),
		}
	})
	require.NotNil(t, h.redisLimiter)

	// Setup succeeded without Redis. While not connected nothing is limited: the limits of 1 per minute
	// are not enforced, and the connects and commands are counted as Redis errors.
	client, transport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, transport.waitReply(t, 1).Error)
	for id := uint32(2); id <= 4; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
		require.False(t, isRateLimited(transport.waitReply(t, id)), "command %d must be allowed while Redis is unreachable", id)
	}
	_, secondTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, secondTransport.waitReply(t, 1).Error)

	after := testutil.ToFloat64(metrics.RateLimitRedisErrorsTotal)
	require.Equal(t, float64(5), after-before)
}

func TestRedisRateLimitSharedBetweenConnections(t *testing.T) {
	h := newRedisRateLimitHandler(t, func(cfg *config.Config) {
		cfg.Client.RateLimit.RedisUserCommand = configtypes.RateLimitRedisUserCommand{
			Enabled:     true,
			Redis:       redisRateLimitOf(uniqueRedisPrefix()),
			Unsubscribe: rateLimitChannelOf(time.Minute, 2),
		}
	})
	first, firstTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, firstTransport.waitReply(t, 1).Error)
	second, secondTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, secondTransport.waitReply(t, 1).Error)
	before := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("redis_user_command", "unsubscribe"))

	// The budget of 2 is shared by both connections of the user, Redis holds it.
	sendRateLimitCommand(first, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(firstTransport.waitReply(t, 2)))
	sendRateLimitCommand(second, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(secondTransport.waitReply(t, 2)))

	sendRateLimitCommand(first, &protocol.Command{Id: 3, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	reply := firstTransport.waitReply(t, 3)
	require.True(t, isRateLimited(reply))
	require.Equal(t, uint32(111), reply.Error.Code)

	after := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("redis_user_command", "unsubscribe"))
	require.Equal(t, float64(1), after-before)

	// Another user has its own budget.
	other, otherTransport := connectUserRateLimitClient(t, h.node, "43")
	require.Nil(t, otherTransport.waitReply(t, 1).Error)
	sendRateLimitCommand(other, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(otherTransport.waitReply(t, 2)))
}

func TestRedisRateLimitAnonymousBypass(t *testing.T) {
	h := newRedisRateLimitHandler(t, func(cfg *config.Config) {
		cfg.Client.RateLimit.RedisUserCommand = configtypes.RateLimitRedisUserCommand{
			Enabled:     true,
			Redis:       redisRateLimitOf(uniqueRedisPrefix()),
			Unsubscribe: rateLimitChannelOf(time.Minute, 1),
		}
	})
	client, transport := connectRateLimitClient(t, h.node)
	for id := uint32(2); id <= 6; id++ {
		sendRateLimitCommand(client, &protocol.Command{Id: id, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
		require.False(t, isRateLimited(transport.waitReply(t, id)), "anonymous command %d must not be limited", id)
	}
}

func TestRedisRateLimitConnectOverLimit(t *testing.T) {
	h := newRedisRateLimitHandler(t, func(cfg *config.Config) {
		cfg.Client.RateLimit.RedisUserCommand = configtypes.RateLimitRedisUserCommand{
			Enabled: true,
			Redis:   redisRateLimitOf(uniqueRedisPrefix()),
			Connect: rateLimitBucketsOf(time.Minute, 1),
		}
	})
	before := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("redis_user_command", "connect"))

	_, firstTransport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, firstTransport.waitReply(t, 1).Error)

	_, secondTransport := connectUserRateLimitClient(t, h.node, "42")
	reply := secondTransport.waitReply(t, 1)
	require.NotNil(t, reply.Error)
	require.Equal(t, uint32(111), reply.Error.Code)

	after := testutil.ToFloat64(metrics.RateLimitRejectsTotal.WithLabelValues("redis_user_command", "connect"))
	require.Equal(t, float64(1), after-before)

	// Anonymous connections are not limited by user connect.
	_, anonTransport := connectRateLimitClient(t, h.node)
	require.Nil(t, anonTransport.waitReply(t, 1).Error)
}

func TestRedisRateLimitReuseFromEngine(t *testing.T) {
	enginePrefix := uniqueRedisPrefix()
	h := newRedisRateLimitHandler(t, func(cfg *config.Config) {
		cfg.Engine.Type = "redis"
		cfg.Engine.Redis.Prefix = enginePrefix
		cfg.Engine.Redis.Address = []string{integrationRedisAddress}
		cfg.Client.RateLimit.RedisUserCommand = configtypes.RateLimitRedisUserCommand{
			Enabled: true,
			Redis: configtypes.RateLimitRedis{
				ReuseFromEngine: true,
				RedisPrefixed:   configtypes.RedisPrefixed{Prefix: "ignored"},
			},
			Unsubscribe: rateLimitChannelOf(time.Minute, 1),
		}
	})
	require.NotNil(t, h.redisLimiter)
	require.Len(t, h.Services(), 1)

	client, transport := connectUserRateLimitClient(t, h.node, "42")
	require.Nil(t, transport.waitReply(t, 1).Error)
	sendRateLimitCommand(client, &protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "test:1"}})
	require.False(t, isRateLimited(transport.waitReply(t, 2)))

	// The key is named by the engine prefix, not by the prefix of the rate limit configuration.
	shards, err := redisshard.BuildRedisShards(configtypes.Redis{Address: []string{integrationRedisAddress}})
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, shard := range shards {
			shard.Close()
		}
	})
	exists := func(key string) int64 {
		n, err := shards[0].RunOp(func(c rueidis.Client) rueidis.RedisResult {
			return c.Do(context.Background(), c.B().Exists().Key(key).Build())
		}).AsInt64()
		require.NoError(t, err)
		return n
	}
	require.Equal(t, int64(1), exists(enginePrefix+".rl.{42}.unsubscribe"))
	require.Equal(t, int64(0), exists("ignored.rl.{42}.unsubscribe"))
}
