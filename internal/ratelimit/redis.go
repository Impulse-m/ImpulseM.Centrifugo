package ratelimit

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/redis/rueidis"
	"github.com/rs/zerolog/log"
)

// errMalformedWait is returned for a script result which is neither allowed, refused nor a refusal with a wait.
var errMalformedWait = errors.New("rate limit script returned a malformed result")

// redisScriptRunner runs script for key on shard with argv. It returns the wait of the operation, 0 when it is allowed.
// It is a field of RedisUserLimiter so tests can replace Redis.
type redisScriptRunner func(ctx context.Context, shard *redisshard.RedisShard, script *rueidis.Lua, key string, argv []string) (time.Duration, error)

// RedisUserLimiter holds compiled redis_user_command limits. Limits of a user are kept in Redis, on the
// shard chosen by consistent hash of the user ID, so all nodes sharing Redis share the limits.
// Redis errors allow commands: a Redis outage must not take down connections, and neither must a Redis
// which is unreachable at startup. Until the limiter connects, commands are allowed too.
type RedisUserLimiter struct {
	plan    *redisPlan
	prefix  string
	timeout time.Duration
	// connector holds the shards. Their count never changes, so the user-to-shard mapping stays the same
	// for the life of the process.
	connector *redisshard.Connector
	// run executes a script on a shard. NewRedisUserLimiter sets runRedisScript; tests replace it with a fake runner.
	run redisScriptRunner
	// errorLog limits how often Redis errors are logged to once per redisshard.ErrorLogInterval.
	errorLog *tools.IntervalLimiter
	// refusals holds the refusals of this node, so that refused commands need no Redis call, see refusalCache.
	refusals *refusalCache
	// denyOnError refuses commands and connects when Redis fails, instead of allowing them, see on_error.
	denyOnError bool
}

// denyWaitOnError is the wait of a command which is refused because Redis failed. The limit has no state to tell when
// it is allowed again, so the client is asked to retry after this long.
const denyWaitOnError = time.Second

// NewRedisUserLimiter compiles redis_user_command limits. It does not connect to Redis: call Connect, then
// run Run as a service, which keeps connecting until it succeeds. shardConfigs must not be empty, shards are
// closed by Run when its context is done. prefix starts all Redis keys of the limiter. ioTimeout bounds each
// Redis call, zero means the default. A command_timeout of cfg replaces it for the command and connect checks.
func NewRedisUserLimiter(
	cfg configtypes.RateLimitRedisUserCommand,
	prefix string,
	shardConfigs []redisshard.RedisShardConfig,
	ioTimeout time.Duration,
) *RedisUserLimiter {
	if ioTimeout <= 0 {
		ioTimeout = redisshard.DefaultIOTimeout
	}
	if cfg.CommandTimeout > 0 {
		ioTimeout = cfg.CommandTimeout.ToDuration()
	}
	limiter := &RedisUserLimiter{
		plan:        newRedisUserPlan(cfg),
		prefix:      prefix,
		timeout:     ioTimeout,
		run:         runRedisScript,
		errorLog:    tools.NewIntervalLimiter(redisshard.ErrorLogInterval),
		refusals:    newRefusalCache(),
		denyOnError: cfg.OnError == configtypes.RateLimitOnErrorDeny,
	}
	limiter.connector = redisshard.NewConnector(shardConfigs, redisshard.LogConnect(
		"redis_user_command rate limit",
		"commands are allowed until it is reachable",
	))
	return limiter
}

// NeedsKey reports whether checks of op depend on the namespace or method.
func (l *RedisUserLimiter) NeedsKey(op Operation) bool {
	return l.plan.ops[op].needsKey()
}

// CommandAllowed checks and consumes one command of op of userID. key is a namespace name for channel
// operations and a method name for RPC. It returns 0 when the command is allowed, else the wait until it is.
// Anonymous users (empty user ID) are not limited. On Redis errors and while not connected the command is allowed,
// unless on_error denies it.
func (l *RedisUserLimiter) CommandAllowed(userID string, op Operation, key string) time.Duration {
	if userID == "" {
		return 0
	}
	s := l.plan.ops[op].resolve(key)
	if s == nil {
		return 0
	}
	return l.check(userID, s)
}

// ConnectAllowed checks and consumes one connect of userID. Anonymous users are not limited. On Redis
// errors and while not connected the connect is allowed, unless on_error denies it.
func (l *RedisUserLimiter) ConnectAllowed(userID string) bool {
	if userID == "" {
		return true
	}
	s := l.plan.ops[OperationConnect].base
	if s == nil {
		return true
	}
	return l.check(userID, s) == 0
}

// Connect makes one attempt to connect to all shards. The attempt is all-or-nothing: on failure nothing is
// kept and the limiter stays not connected. Failure is logged as a warning, success as info. Once connected,
// Connect does nothing: the Redis client reconnects by itself.
func (l *RedisUserLimiter) Connect() {
	if l.connected() {
		return
	}
	_ = l.connector.Connect()
}

// Run connects to Redis in the background while the limiter is not connected, see redisshard.Connector.Run.
// When the context is done, Run closes the connected shards. It implements service.Service.
func (l *RedisUserLimiter) Run(ctx context.Context) error {
	return l.connector.Run(ctx)
}

// connected reports whether Connect succeeded.
func (l *RedisUserLimiter) connected() bool {
	return l.connector.Shards() != nil
}

// check runs the script of scope s for userID and returns its wait. A refusal which this node has cached is returned
// without a Redis call. Redis errors are counted and logged, and allow the operation unless on_error denies it. While
// not connected, the operation is counted as an error and is allowed unless on_error denies it.
func (l *RedisUserLimiter) check(userID string, s *scope) time.Duration {
	list := l.connector.Shards()
	if list == nil {
		metrics.RateLimitRedisErrorsTotal.Inc()
		l.logError(redisshard.ErrNotConnected)
		return l.errorWait()
	}
	key := redisKey(l.prefix, userID, s.redisName)
	now := refusalClock()
	if wait, ok := l.refusals.refused(key, now); ok {
		return wait
	}
	shard := list[redisshard.ShardIndex(userID, len(list))]

	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	runStart := time.Now()
	wait, err := l.run(ctx, shard, s.script, key, s.argv)
	metrics.RateLimitRedisDuration.Observe(time.Since(runStart).Seconds())
	cancel()

	if err != nil {
		metrics.RateLimitRedisErrorsTotal.Inc()
		l.logError(err)
		return l.errorWait()
	}
	l.refusals.refuse(key, now, wait)
	return wait
}

// refusalClockStart is the origin of refusalClock.
var refusalClockStart = time.Now()

// refusalClock returns the nanoseconds since the process started. It reads the monotonic clock, so a step of the wall
// clock does not change how long a cached refusal lasts.
func refusalClock() int64 {
	return int64(time.Since(refusalClockStart))
}

// errorWait is the wait of a command when Redis failed: none, unless on_error denies commands.
func (l *RedisUserLimiter) errorWait() time.Duration {
	if l.denyOnError {
		return denyWaitOnError
	}
	return 0
}

// logError logs a Redis error at most once per redisshard.ErrorLogInterval.
func (l *RedisUserLimiter) logError(err error) {
	if l.errorLog.Allow() {
		log.Error().Err(err).Msg("redis_user_command rate limit: Redis error, command allowed")
	}
}

// runRedisScript runs the rate limit script on shard and returns the wait it reports.
func runRedisScript(ctx context.Context, shard *redisshard.RedisShard, script *rueidis.Lua, key string, argv []string) (time.Duration, error) {
	keys := []string{key}
	res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return script.Exec(ctx, client, keys, argv)
	})
	v, err := res.AsInt64()
	if err != nil {
		return 0, err
	}
	return waitOf(v)
}

// waitOf converts the result of a rate limit script to a wait. 1 is allowed. A negative number is a refusal, its
// absolute value is the wait in microseconds, clamped so that it fits in a duration. 0 is a refusal from a script
// which reports no wait, as older nodes do, and waits one nanosecond. Any other value is a malformed result.
func waitOf(v int64) (time.Duration, error) {
	switch {
	case v == 1:
		return 0, nil
	case v == 0:
		return time.Nanosecond, nil
	case v < 0 && v != math.MinInt64:
		us := min(-v, math.MaxInt64/int64(time.Microsecond))
		return time.Duration(us) * time.Microsecond, nil
	}
	return 0, errMalformedWait
}

// userIDEscaper escapes the characters which would make the key of a user ambiguous: a closing brace ends the user ID
// in the key, and a percent sign starts an escape. IDs without them keep their keys.
var userIDEscaper = strings.NewReplacer("%", "%25", "}", "%7D")

// redisKey returns the key of a scope of userID: {prefix}.rl.{userID}.{scope}. Braces around the user ID
// make it the Redis Cluster hash tag, so all scopes of a user live in one slot. The user ID is escaped, so the
// first closing brace ends it, and the scope name after it, which may hold braces, cannot shift the key.
func redisKey(prefix string, userID string, scopeName string) string {
	return prefix + ".rl.{" + userIDEscaper.Replace(userID) + "}." + scopeName
}
