package redisshard

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/rs/zerolog/log"
)

const (
	// connectRetryMin and connectRetryMax bound the delay between connect attempts while Redis is unreachable.
	connectRetryMin = time.Second
	connectRetryMax = 30 * time.Second
)

const (
	// DefaultIOTimeout bounds each Redis call of the Redis users of the server when their configuration has no IO
	// timeout.
	DefaultIOTimeout = 4 * time.Second
	// ErrorLogInterval is the minimum time between two logged errors of one Redis user.
	ErrorLogInterval = 10 * time.Second
)

// ErrNotConnected is returned by the checks and the calls made while a connector has no shards.
var ErrNotConnected = errors.New("not connected to Redis")

// ConnectFunc makes all shards of configs, all-or-nothing: on failure no shard is kept.
type ConnectFunc func(configs []RedisShardConfig) ([]*RedisShard, error)

// ConnectLog is called after every connect attempt. err is nil on success, shards is the number of connected shards.
type ConnectLog func(err error, shards int)

// LogConnect returns the ConnectLog of a connector of subject, which names the caller in the log, for example
// "redis_user_command rate limit". A failed attempt is logged as a warning with what the caller does meanwhile, which
// completes the sentence "Redis is unreachable, ...". A connected attempt is logged as info.
func LogConnect(subject string, unreachable string) ConnectLog {
	return func(err error, shards int) {
		if err != nil {
			log.Warn().Err(err).Msg(subject + ": Redis is unreachable, " + unreachable)
			return
		}
		log.Info().Int("shards", shards).Msg(subject + ": connected to Redis")
	}
}

// Connector connects to the Redis shards of a configuration once and keeps them for the life of the process.
// Connecting is retried in Run with exponential backoff, so a Redis which is down at start does not stop the
// caller. Shards are closed when Run returns.
type Connector struct {
	configs    []RedisShardConfig
	connect    ConnectFunc
	log        ConnectLog
	backoffMin time.Duration
	backoffMax time.Duration
	// shards is set once, by a successful Connect. Its shard count never changes. Nil means not connected.
	shards atomic.Pointer[[]*RedisShard]
	// connectMu serializes Connect, so that two calls do not both dial, and the second store does not drop the first
	// set of shards without closing it.
	connectMu sync.Mutex
}

// NewConnector returns a connector of configs. It does not connect: call Connect, or run Run as a service.
// log may be nil.
func NewConnector(configs []RedisShardConfig, log ConnectLog) *Connector {
	return &Connector{
		configs:    configs,
		connect:    NewRedisShards,
		log:        log,
		backoffMin: connectRetryMin,
		backoffMax: connectRetryMax,
	}
}

// SetConnect replaces the function which connects to Redis. It is for tests, call it before Connect or Run.
func (c *Connector) SetConnect(connect ConnectFunc) {
	c.connect = connect
}

// SetBackoff replaces the bounds of the delay between connect attempts in Run. It is for tests, call it before
// Run.
func (c *Connector) SetBackoff(backoffMin time.Duration, backoffMax time.Duration) {
	c.backoffMin = backoffMin
	c.backoffMax = backoffMax
}

// Connect makes one attempt to connect to all shards. The attempt is all-or-nothing: on failure nothing is
// kept and the connector stays not connected. Once connected, Connect does nothing: the Redis client
// reconnects by itself.
func (c *Connector) Connect() error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	if c.Shards() != nil {
		return nil
	}
	shards, err := c.connect(c.configs)
	if c.log != nil {
		c.log(err, len(shards))
	}
	if err != nil {
		return err
	}
	c.shards.Store(&shards)
	return nil
}

// Run connects in the background while the connector is not connected: attempts are spaced by exponential
// backoff with jitter, from backoffMin up to backoffMax. When the context is done, Run stops retrying and
// closes the connected shards. It returns the context error.
func (c *Connector) Run(ctx context.Context) error {
	backoff := c.backoffMin
	for c.Shards() == nil {
		if !sleepContext(ctx, tools.HalfJitter(backoff)) {
			break
		}
		// The failure is reported by the connect log, and the next attempt is made after the backoff.
		_ = c.Connect()
		backoff = min(2*backoff, c.backoffMax)
	}
	<-ctx.Done()
	c.Close()
	return ctx.Err()
}

// Shards returns the connected shards, or nil when not connected.
func (c *Connector) Shards() []*RedisShard {
	shards := c.shards.Load()
	if shards == nil {
		return nil
	}
	return *shards
}

// Close closes the connected shards and forgets them, so Shards returns nil until a later Connect.
func (c *Connector) Close() {
	shards := c.shards.Swap(nil)
	if shards == nil {
		return
	}
	for _, shard := range *shards {
		if shard != nil {
			shard.Close()
		}
	}
}

// sleepContext waits for d. It returns false when the context is done first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
