package client

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// syncBuffer is a log sink safe for concurrent writes, as a Redis limiter may log from its background connect.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p to the buffer.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns the logged text.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestRedisRateLimitDefaultFallbackWarning checks that redis_user_command gets the same startup warning as the other
// limits: a publish limit without map publish, map remove and default buckets leaves map publishes unlimited.
func TestRedisRateLimitDefaultFallbackWarning(t *testing.T) {
	var logBuf syncBuffer
	savedLogger := log.Logger
	log.Logger = zerolog.New(&logBuf)
	defer func() { log.Logger = savedLogger }()

	newUserRateLimitHandler(t, configtypes.RateLimit{
		RedisUserCommand: configtypes.RateLimitRedisUserCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: rateLimitChannelOf(time.Minute, 1),
			},
			Redis: configtypes.RateLimitRedis{
				RedisPrefixed: configtypes.RedisPrefixed{
					Redis: configtypes.Redis{Address: []string{"127.0.0.1:1"}},
				},
			},
		},
	})
	require.Contains(t, logBuf.String(), `"section":"redis_user_command"`)
	require.Contains(t, logBuf.String(), `"operation":"map_publish"`)
}

// TestRateLimitNamespaceOverrideFallbackWarning checks that a publish limit given only by a namespace override is
// reported as well: map publishes of that namespace were limited before the upgrade and are not limited after it.
func TestRateLimitNamespaceOverrideFallbackWarning(t *testing.T) {
	var logBuf syncBuffer
	savedLogger := log.Logger
	log.Logger = zerolog.New(&logBuf)
	defer func() { log.Logger = savedLogger }()

	newRateLimitHandler(t, configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			RateLimitOperations: configtypes.RateLimitOperations{
				Publish: configtypes.RateLimitChannelBuckets{
					NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
						{
							NamespaceName: "chat",
							Enabled:       true,
							Buckets:       rateLimitChannelOf(time.Minute, 1).Buckets,
						},
					},
				},
			},
		},
	}, "chat")
	require.Contains(t, logBuf.String(), `"operation":"map_publish"`)
	require.Contains(t, logBuf.String(), `"section":"client_command"`)
}
