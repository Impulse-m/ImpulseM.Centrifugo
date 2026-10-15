package client

import (
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/service"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// limiterClientCommand is a value of the limiter metric label for per-connection command limits.
const limiterClientCommand = "client_command"

// limiterUserCommand is a value of the limiter metric label for per-user command limits.
const limiterUserCommand = "user_command"

// limiterRedisUserCommand is a value of the limiter metric label for per-user Redis command limits.
const limiterRedisUserCommand = "redis_user_command"

// setupRateLimit creates rate limiters and registers node hooks for client rate limits. Nothing is
// created or registered when client_command, client_error, user_command and redis_user_command are all disabled.
func (h *Handler) setupRateLimit(cfg *config.Config) error {
	rl := cfg.Client.RateLimit
	if !rl.ClientCommand.Enabled && !rl.ClientError.Enabled && !rl.UserCommand.Enabled && !rl.RedisUserCommand.Enabled {
		return nil
	}
	var redisLimiter *ratelimit.RedisUserLimiter
	if rl.RedisUserCommand.Enabled {
		limiter, err := newRedisUserLimiter(*cfg)
		if err != nil {
			return err
		}
		// One attempt to connect now. If Redis is unreachable the node still starts: the limiter allows
		// commands and connects in the background, see the Services of the handler.
		limiter.Connect()
		redisLimiter = limiter
		h.redisLimiter = redisLimiter
	}
	limiter := ratelimit.NewClientLimiter(rl)
	h.clientLimiter = limiter
	var userLimiter *ratelimit.UserLimiter
	if rl.UserCommand.Enabled {
		userLimiter = ratelimit.NewUserLimiter(rl.UserCommand)
		h.userLimiter = userLimiter
	}
	// Connection state is allocated on connect with the connection record, so the read hook is needed for client_error
	// and user limits too.
	h.node.OnCommandRead(h.rateLimitCommandRead(limiter, rl.ClientCommand.Enabled, userLimiter, redisLimiter))
	if rl.ClientError.Enabled {
		h.node.OnCommandProcessed(h.rateLimitCommandProcessed(limiter))
	}
	return nil
}

// newRedisUserLimiter creates the redis_user_command limiter. It uses the Redis and prefix of the rate limit
// configuration, or the engine Redis and engine prefix when reuse_from_engine is set. Engine prefix keeps
// limits of different apps sharing one Redis apart. Configuration errors are returned, connection errors
// are not: the limiter connects later.
func newRedisUserLimiter(cfg config.Config) (*ratelimit.RedisUserLimiter, error) {
	rc := cfg.Client.RateLimit.RedisUserCommand
	redisConf, prefix := redisConfigOf(rc.Redis.ReuseFromEngine, rc.Redis.RedisPrefixed, cfg.Engine.Redis.RedisPrefixed)
	configs, err := redisshard.ConfigsOf(redisConf, "redis_user_command: Redis configuration")
	if err != nil {
		return nil, err
	}
	return ratelimit.NewRedisUserLimiter(rc, prefix, configs, redisConf.IOTimeout.ToDuration()), nil
}

// rateLimitCommandRead checks commands against the rate limit state of the connection, which its record holds from
// connect. It checks client command limits when checkCommands is set, then user command limits when userLimiter is not nil,
// then Redis user command limits when redisLimiter is not nil. It stops at the first rejection.
// Anonymous connections have no user state, and anonymous users are not limited by user limits.
func (h *Handler) rateLimitCommandRead(
	limiter *ratelimit.ClientLimiter,
	checkCommands bool,
	userLimiter *ratelimit.UserLimiter,
	redisLimiter *ratelimit.RedisUserLimiter,
) centrifuge.CommandReadHandler {
	return func(c *centrifuge.Client, e centrifuge.CommandReadEvent) error {
		if !checkCommands && userLimiter == nil && redisLimiter == nil {
			return nil
		}
		st := rateLimitStateOf(c)
		if st == nil {
			return nil
		}
		op, name, ok := rateLimitOperation(e.Command)
		if !ok {
			return nil
		}
		key := ""
		needsKey := (checkCommands && limiter.NeedsKey(op)) ||
			(userLimiter != nil && userLimiter.NeedsKey(op)) ||
			(redisLimiter != nil && redisLimiter.NeedsKey(op))
		if needsKey {
			key = h.rateLimitKey(op, name)
		}
		if checkCommands && !limiter.CommandAllowed(st, e.Started, op, key) {
			metrics.RateLimitRejectsTotal.WithLabelValues(limiterClientCommand, op.String()).Inc()
			return centrifuge.ErrorTooManyRequests
		}
		if userLimiter != nil && !userLimiter.CommandAllowed(st, e.Started, op, key) {
			metrics.RateLimitRejectsTotal.WithLabelValues(limiterUserCommand, op.String()).Inc()
			return centrifuge.ErrorTooManyRequests
		}
		if redisLimiter != nil && !redisLimiter.CommandAllowed(c.UserID(), op, key) {
			metrics.RateLimitRejectsTotal.WithLabelValues(limiterRedisUserCommand, op.String()).Inc()
			return centrifuge.ErrorTooManyRequests
		}
		return nil
	}
}

// Services returns background services of rate limits: the user state sweeper when user_command is enabled,
// and closing of Redis shards when redis_user_command is enabled.
func (h *Handler) Services() []service.Service {
	var services []service.Service
	if h.userLimiter != nil {
		services = append(services, h.userLimiter)
	}
	if h.redisLimiter != nil {
		services = append(services, h.redisLimiter)
	}
	if h.rpcDedup != nil && h.rpcDedup.shared != nil {
		services = append(services, newRPCDedupRedisService(h.rpcDedup.connector, h.rpcDedup.shared))
	}
	return services
}

// attachRateLimit attaches the user state of the rate limits to the connection. Connections without a user
// are not attached. A rebind attaches the state of the new user after it detaches the old one.
func (h *Handler) attachRateLimit(c rebindableClient, userID string) {
	if h.userLimiter == nil || userID == "" {
		return
	}
	st := rateLimitStateOf(c)
	if st == nil {
		return
	}
	h.userLimiter.Attach(st, userID)
}

// detachRateLimit releases the user state of the rate limits attached to the connection. It is safe to call
// for a connection which is not attached.
func (h *Handler) detachRateLimit(c rebindableClient) {
	if h.userLimiter == nil {
		return
	}
	st := rateLimitStateOf(c)
	if st == nil {
		return
	}
	h.userLimiter.Detach(st)
}

// userConnectAllowed checks and consumes the user_command and redis_user_command connect limits of userID.
// Anonymous users are not limited. Over the limit the rejection is counted.
func (h *Handler) userConnectAllowed(userID string) bool {
	if userID == "" {
		return true
	}
	if h.userLimiter != nil && !h.userLimiter.ConnectAllowed(userID, time.Now()) {
		metrics.RateLimitRejectsTotal.WithLabelValues(limiterUserCommand, ratelimit.OperationConnect.String()).Inc()
		return false
	}
	if h.redisLimiter != nil && !h.redisLimiter.ConnectAllowed(userID) {
		metrics.RateLimitRejectsTotal.WithLabelValues(limiterRedisUserCommand, ratelimit.OperationConnect.String()).Inc()
		return false
	}
	return true
}

// rateLimitCommandProcessed counts protocol errors of a connection. Internal errors are not
// counted, disconnects are not errors for this purpose.
func (h *Handler) rateLimitCommandProcessed(limiter *ratelimit.ClientLimiter) centrifuge.CommandProcessedHandler {
	return func(c *centrifuge.Client, e centrifuge.CommandProcessedEvent) {
		if e.Error == nil {
			return
		}
		var clientErr *centrifuge.Error
		if !errors.As(e.Error, &clientErr) || clientErr.Code == centrifuge.ErrorInternal.Code {
			return
		}
		st := rateLimitStateOf(c)
		if st == nil {
			return
		}
		if limiter.ErrorAllowed(st, e.Started) {
			return
		}
		if st.MarkErrorExhausted() {
			metrics.RateLimitErrorDisconnectsTotal.Inc()
			c.Disconnect(centrifuge.DisconnectTooManyErrors)
		}
	}
}

// rateLimitKey returns the key which selects buckets of op: a method for RPC, a namespace for
// channel operations.
func (h *Handler) rateLimitKey(op ratelimit.Operation, name string) string {
	if op == ratelimit.OperationRPC {
		return name
	}
	return h.rateLimitNamespace(name)
}

// rateLimitNamespace returns the namespace name of a channel, empty when channel has no namespace.
func (h *Handler) rateLimitNamespace(channel string) string {
	nsName, _, _, _, err := h.cfgContainer.ChannelOptions(channel)
	if err != nil {
		return ""
	}
	return nsName
}

// rateLimitOperation returns the rate limited operation of a command and the name which selects
// its buckets: a channel for channel operations, a method for RPC. Commands which are not
// rate limited (connect, ping, send) return false. The order of checks follows Core dispatch.
func rateLimitOperation(cmd *protocol.Command) (ratelimit.Operation, string, bool) {
	switch {
	case cmd.Ping != nil:
		return 0, "", false
	case cmd.Subscribe != nil:
		return ratelimit.OperationSubscribe, cmd.Subscribe.Channel, true
	case cmd.Unsubscribe != nil:
		return ratelimit.OperationUnsubscribe, cmd.Unsubscribe.Channel, true
	case cmd.Publish != nil:
		return ratelimit.OperationPublish, cmd.Publish.Channel, true
	case cmd.Presence != nil:
		return ratelimit.OperationPresence, cmd.Presence.Channel, true
	case cmd.PresenceStats != nil:
		return ratelimit.OperationPresenceStats, cmd.PresenceStats.Channel, true
	case cmd.History != nil:
		return ratelimit.OperationHistory, cmd.History.Channel, true
	case cmd.Rpc != nil:
		return ratelimit.OperationRPC, cmd.Rpc.Method, true
	case cmd.Refresh != nil:
		return ratelimit.OperationRefresh, "", true
	case cmd.SubRefresh != nil:
		return ratelimit.OperationSubRefresh, cmd.SubRefresh.Channel, true
	}
	return 0, "", false
}
