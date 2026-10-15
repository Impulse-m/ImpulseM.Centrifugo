package client

import (
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/service"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog/log"
)

// limiterClientCommand is a value of the limiter metric label for per-connection command limits.
const limiterClientCommand = "client_command"

// limiterUserCommand is a value of the limiter metric label for per-user command limits.
const limiterUserCommand = "user_command"

// limiterRedisUserCommand is a value of the limiter metric label for per-user Redis command limits.
const limiterRedisUserCommand = "redis_user_command"

// publishTypeMap is the publish request type of a map publication, see PublishRequest.Type.
const publishTypeMap int32 = 1

// subscriptionTypeSharedPoll is the subscription type of shared poll namespaces, see ChannelOptions.SubscriptionType.
const subscriptionTypeSharedPoll = "shared_poll"

// subRefreshTypeTrack and subRefreshTypeUntrack are the sub refresh request types of shared poll
// track and untrack, see SubRefreshRequest.Type. Core uses the same values for typeTrack and typeUntrack
// in client_keyed.go. Core exports no symbol for them, so a change of those values in Core must be made here too.
const (
	subRefreshTypeTrack   int32 = 1
	subRefreshTypeUntrack int32 = 2
)

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
	// The client limiter gives each connection its state, which the user limiter needs too. A Redis limit needs only the
	// user, so a configuration with the Redis limit alone gives no connection a record.
	if rl.ClientCommand.Enabled || rl.ClientError.Enabled || rl.UserCommand.Enabled {
		h.clientLimiter = limiter
	}
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
	if rl.ClientCommand.Enabled {
		warnDefaultFallback(limiterClientCommand, &rl.ClientCommand.RateLimitOperations)
	}
	if rl.UserCommand.Enabled {
		warnDefaultFallback(limiterUserCommand, &rl.UserCommand.RateLimitOperations)
	}
	if rl.RedisUserCommand.Enabled {
		warnDefaultFallback(limiterRedisUserCommand, &rl.RedisUserCommand.RateLimitOperations)
	}
	return nil
}

// warnDefaultFallback logs a warning for each map or track operation which has no buckets while its
// previous operation (publish or sub_refresh) has buckets, and default has none. Such an operation is not
// limited after an upgrade: it used to count as publish or sub_refresh and now falls back to default only.
func warnDefaultFallback(section string, ops *configtypes.RateLimitOperations) {
	if isDefaultSet(ops.Default) {
		return
	}
	if isChannelBucketsSet(ops.Publish) {
		warnUnlimitedOperation(section, ratelimit.OperationMapPublish.String(), isChannelBucketsSet(ops.MapPublish))
		warnUnlimitedOperation(section, ratelimit.OperationMapRemove.String(), isChannelBucketsSet(ops.MapRemove))
	}
	if isChannelBucketsSet(ops.SubRefresh) {
		warnUnlimitedOperation(section, ratelimit.OperationTrack.String(), isChannelBucketsSet(ops.Track))
		warnUnlimitedOperation(section, ratelimit.OperationUntrack.String(), isChannelBucketsSet(ops.Untrack))
	}
}

// warnUnlimitedOperation logs a warning about operation of section when it has no buckets.
func warnUnlimitedOperation(section string, operation string, set bool) {
	if set {
		return
	}
	log.Warn().
		Str("section", section).
		Str("operation", operation).
		Msg("rate limit operation has no buckets and no default buckets, its commands are not limited; set its buckets or default buckets")
}

// isDefaultSet reports whether default buckets are enabled and have at least one bucket.
func isDefaultSet(buckets configtypes.RateLimitBuckets) bool {
	return buckets.Enabled && len(buckets.Buckets) > 0
}

// isChannelBucketsSet reports whether base buckets of a channel operation are enabled and have at least one bucket.
func isChannelBucketsSet(buckets configtypes.RateLimitChannelBuckets) bool {
	return buckets.Enabled && len(buckets.Buckets) > 0
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
		op, name, ok := h.rateLimitOperation(e.Command)
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
		// The client and user limits keep their state in the record, so a connection without one passes them. The Redis
		// limit needs only the user, so it checks a connection without a record too.
		st := rateLimitStateOf(c)
		if st != nil && checkCommands && !limiter.CommandAllowed(st, e.Started, op, key) {
			metrics.RateLimitRejectsTotal.WithLabelValues(limiterClientCommand, op.String()).Inc()
			return centrifuge.ErrorTooManyRequests
		}
		if st != nil && userLimiter != nil && !userLimiter.CommandAllowed(st, e.Started, op, key) {
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
func (h *Handler) rateLimitOperation(cmd *protocol.Command) (ratelimit.Operation, string, bool) {
	switch {
	case cmd.Ping != nil:
		return 0, "", false
	case cmd.Subscribe != nil:
		return ratelimit.OperationSubscribe, cmd.Subscribe.Channel, true
	case cmd.Unsubscribe != nil:
		return ratelimit.OperationUnsubscribe, cmd.Unsubscribe.Channel, true
	case cmd.Publish != nil:
		return publishOperation(cmd.Publish), cmd.Publish.Channel, true
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
		return h.subRefreshOperation(cmd.SubRefresh), cmd.SubRefresh.Channel, true
	}
	return 0, "", false
}

// publishOperation returns the operation of a publish command: a map publish or a map remove for
// map channels, a publish otherwise.
func publishOperation(req *protocol.PublishRequest) ratelimit.Operation {
	if req.Type != publishTypeMap {
		return ratelimit.OperationPublish
	}
	if req.Removed {
		return ratelimit.OperationMapRemove
	}
	return ratelimit.OperationMapPublish
}

// subRefreshOperation returns the operation of a sub refresh command: track or untrack keys of a
// shared poll subscription, a subscription token refresh otherwise. Core reads the type only on a
// shared poll channel. On any other channel the request refreshes the token whatever its type is.
func (h *Handler) subRefreshOperation(req *protocol.SubRefreshRequest) ratelimit.Operation {
	if req.Type == 0 || !h.isSharedPollChannel(req.Channel) {
		return ratelimit.OperationSubRefresh
	}
	switch req.Type {
	case subRefreshTypeTrack:
		return ratelimit.OperationTrack
	case subRefreshTypeUntrack:
		return ratelimit.OperationUntrack
	}
	return ratelimit.OperationSubRefresh
}

// isSharedPollChannel reports whether the channel belongs to a shared poll namespace. Core takes the
// keyed flag of a subscription from the same namespace options. The lookup does not allocate.
func (h *Handler) isSharedPollChannel(channel string) bool {
	chOpts, ok, err := h.cfgContainer.ChannelOptionsRef(channel)
	if err != nil || !ok {
		return false
	}
	return chOpts.SubscriptionType == subscriptionTypeSharedPoll
}
