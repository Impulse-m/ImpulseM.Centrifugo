package ratelimit

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/redis/rueidis"
	"github.com/rs/zerolog/log"
)

const (
	// redisDefaultIOTimeout bounds Redis calls when the configuration has no IO timeout.
	redisDefaultIOTimeout = 4 * time.Second
	// redisErrorLogInterval is the minimum time between two logged Redis errors of a node.
	redisErrorLogInterval = 10 * time.Second
)

// rateLimitScript is the rate limit check which runs in Redis. KEYS[1] is a hash with one theoretical
// arrival time (TAT, microseconds) per bucket of a scope, fields are bucket indexes starting from 0.
// ARGV is described in redisScriptArgs. All buckets are checked first and TATs are written only when
// every bucket allows the operation, so a rejected operation consumes nothing. TIME is the Redis clock,
// the same for all nodes. Scripts replicate effects, so TIME is allowed.
var rateLimitScript = rueidis.NewLuaScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local n = tonumber(ARGV[2])
local fields = {}
for i = 1, n do
	fields[i] = tostring(i - 1)
end
local stored = redis.call('HMGET', KEYS[1], unpack(fields))
local updates = {}
for i = 1, n do
	local emission = tonumber(ARGV[2 * i + 1])
	local tolerance = tonumber(ARGV[2 * i + 2])
	local tat = tonumber(stored[i]) or 0
	if tat < now then
		tat = now
	end
	tat = tat + emission
	if tat - now > tolerance then
		return 0
	end
	updates[2 * i - 1] = fields[i]
	updates[2 * i] = string.format('%.0f', tat)
end
redis.call('HSET', KEYS[1], unpack(updates))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
`)

// redisScriptRunner runs the rate limit script for key on shard with argv. It returns true when the
// operation is allowed. It is a field of RedisUserLimiter so tests can replace Redis.
type redisScriptRunner func(ctx context.Context, shard *redisshard.RedisShard, key string, argv []string) (bool, error)

// RedisUserLimiter holds compiled redis_user_command limits. Limits of a user are kept in Redis, on the
// shard chosen by consistent hash of the user ID, so all nodes sharing Redis share the limits.
// Redis errors allow commands: a Redis outage must not take down connections.
type RedisUserLimiter struct {
	plan    *redisPlan
	shards  []*redisshard.RedisShard
	prefix  string
	timeout time.Duration
	run     redisScriptRunner
	// lastErrorLog is the time of the last logged Redis error in Unix nanoseconds.
	lastErrorLog atomic.Int64
}

// NewRedisUserLimiter compiles redis_user_command limits. Shards must not be empty, they are closed
// by Run when its context is done. prefix starts all Redis keys of the limiter. ioTimeout bounds each
// Redis call, zero means the default.
func NewRedisUserLimiter(
	cfg configtypes.RateLimitRedisUserCommand,
	prefix string,
	shards []*redisshard.RedisShard,
	ioTimeout time.Duration,
) *RedisUserLimiter {
	if ioTimeout <= 0 {
		ioTimeout = redisDefaultIOTimeout
	}
	return &RedisUserLimiter{
		plan:    newRedisUserPlan(cfg),
		shards:  shards,
		prefix:  prefix,
		timeout: ioTimeout,
		run:     runRedisScript,
	}
}

// NeedsKey reports whether checks of op depend on the namespace or method.
func (l *RedisUserLimiter) NeedsKey(op Operation) bool {
	return l.plan.ops[op].needsKey()
}

// CommandAllowed checks and consumes one command of op of userID. key is a namespace name for channel
// operations and a method name for RPC. Anonymous users (empty user ID) are not limited. On Redis errors
// the command is allowed.
func (l *RedisUserLimiter) CommandAllowed(userID string, op Operation, key string) bool {
	if userID == "" {
		return true
	}
	s := l.plan.ops[op].resolve(key)
	if s == nil {
		return true
	}
	return l.check(userID, s)
}

// ConnectAllowed checks and consumes one connect of userID. Anonymous users are not limited. On Redis
// errors the connect is allowed.
func (l *RedisUserLimiter) ConnectAllowed(userID string) bool {
	if userID == "" {
		return true
	}
	s := l.plan.ops[OperationConnect].base
	if s == nil {
		return true
	}
	return l.check(userID, s)
}

// Run closes Redis shards of the limiter when the context is done. It is a service of the handler.
func (l *RedisUserLimiter) Run(ctx context.Context) error {
	<-ctx.Done()
	for _, shard := range l.shards {
		shard.Close()
	}
	return ctx.Err()
}

// check runs the script for scope s of userID. Redis errors are counted and logged, and allow the operation.
func (l *RedisUserLimiter) check(userID string, s *scope) bool {
	key := redisKey(l.prefix, userID, s.name)
	shard := l.shards[shardIndex(userID, len(l.shards))]

	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	start := time.Now()
	allowed, err := l.run(ctx, shard, key, s.argv)
	metrics.RateLimitRedisDuration.Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		metrics.RateLimitRedisErrorsTotal.Inc()
		l.logError(err)
		return true
	}
	return allowed
}

// logError logs a Redis error at most once per redisErrorLogInterval.
func (l *RedisUserLimiter) logError(err error) {
	now := time.Now().UnixNano()
	last := l.lastErrorLog.Load()
	if now-last < int64(redisErrorLogInterval) {
		return
	}
	if !l.lastErrorLog.CompareAndSwap(last, now) {
		return
	}
	log.Error().Err(err).Msg("redis_user_command rate limit: Redis error, command allowed")
}

// runRedisScript runs the rate limit script on shard.
func runRedisScript(ctx context.Context, shard *redisshard.RedisShard, key string, argv []string) (bool, error) {
	keys := []string{key}
	res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return rateLimitScript.Exec(ctx, client, keys, argv)
	})
	allowed, err := res.AsInt64()
	if err != nil {
		return false, err
	}
	return allowed == 1, nil
}

// redisKey returns the key of a scope of userID: {prefix}.rl.{userID}.{scope}. Braces around the user ID
// make it the Redis Cluster hash tag, so all scopes of a user live in one slot.
func redisKey(prefix string, userID string, scopeName string) string {
	return prefix + ".rl.{" + userID + "}." + scopeName
}

// shardIndex returns the index of the shard of userID among numShards shards.
func shardIndex(userID string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	return int(jumpConsistentHash(fnv1a64(userID), int32(numShards)))
}

// jumpConsistentHash maps key to one of numBuckets buckets (Lamping and Veach). Growing numBuckets by one
// moves only about 1/numBuckets of the keys, and only to the new bucket.
func jumpConsistentHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}

// fnv1a64 returns FNV-1a hash of s. It does not allocate.
func fnv1a64(s string) uint64 {
	const offset64 uint64 = 14695981039346656037
	const prime64 uint64 = 1099511628211
	hash := offset64
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}

// redisScriptArgs returns arguments of the rate limit script for a scope with buckets. The arguments
// are: key TTL in milliseconds (the longest interval of the scope, rounded up), number of buckets,
// then emission interval and burst tolerance of every bucket in microseconds, the clock of the script.
// Microsecond resolution is used because Redis TIME returns microseconds; sub-microsecond parts of
// emission intervals are truncated, but never below one microsecond.
func redisScriptArgs(buckets []slotBucket, maxInterval time.Duration) []string {
	ttlMs := int64((maxInterval + time.Millisecond - 1) / time.Millisecond)
	if ttlMs < 1 {
		ttlMs = 1
	}
	args := make([]string, 0, 2+2*len(buckets))
	args = append(args, strconv.FormatInt(ttlMs, 10), strconv.Itoa(len(buckets)))
	for i := range buckets {
		emission := int64(buckets[i].bucket.emission / int64(time.Microsecond))
		if emission < 1 {
			emission = 1
		}
		tolerance := int64(buckets[i].bucket.tolerance / int64(time.Microsecond))
		if tolerance < emission {
			tolerance = emission
		}
		args = append(args, strconv.FormatInt(emission, 10), strconv.FormatInt(tolerance, 10))
	}
	return args
}
