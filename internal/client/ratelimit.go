package client

import (
	"errors"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"
	"github.com/centrifugal/centrifugo/v6/internal/service"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// limiterClientCommand is a value of the limiter metric label for per-connection command limits.
const limiterClientCommand = "client_command"

// limiterUserCommand is a value of the limiter metric label for per-user command limits.
const limiterUserCommand = "user_command"

// setupRateLimit registers node hooks for client rate limits. Nothing is registered when
// client_command, client_error and user_command are all disabled.
func (h *Handler) setupRateLimit(rl configtypes.RateLimit) {
	if !rl.ClientCommand.Enabled && !rl.ClientError.Enabled && !rl.UserCommand.Enabled {
		return
	}
	limiter := ratelimit.NewClientLimiter(rl)
	var userLimiter *ratelimit.UserLimiter
	if rl.UserCommand.Enabled {
		userLimiter = ratelimit.NewUserLimiter(rl.UserCommand)
		h.userLimiter = userLimiter
	}
	// Connection state is allocated on connect, so the read hook is needed for client_error and
	// user_command too.
	h.node.OnCommandRead(h.rateLimitCommandRead(limiter, rl.ClientCommand.Enabled, userLimiter))
	if rl.ClientError.Enabled {
		h.node.OnCommandProcessed(h.rateLimitCommandProcessed(limiter))
	}
}

// rateLimitCommandRead allocates connection rate limit state on connect. For other commands it
// checks client command limits when checkCommands is set, then user command limits when userLimiter
// is not nil. Anonymous connections have no user state, so user limits skip them.
func (h *Handler) rateLimitCommandRead(
	limiter *ratelimit.ClientLimiter,
	checkCommands bool,
	userLimiter *ratelimit.UserLimiter,
) centrifuge.CommandReadHandler {
	return func(c *centrifuge.Client, e centrifuge.CommandReadEvent) error {
		if e.Command.Connect != nil {
			// Keep existing state: a repeated connect must not reset the buckets of the connection.
			if c.ExtState() == nil {
				c.SetExtState(limiter.NewConnState())
			}
			return nil
		}
		st, ok := c.ExtState().(*ratelimit.ConnState)
		if !ok {
			return nil
		}
		op, name, ok := rateLimitOperation(e.Command)
		if !ok {
			return nil
		}
		key := ""
		needsKey := (checkCommands && limiter.NeedsKey(op)) || (userLimiter != nil && userLimiter.NeedsKey(op))
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
		return nil
	}
}

// Services returns background services of rate limits: the user state sweeper when user_command
// is enabled, nothing otherwise.
func (h *Handler) Services() []service.Service {
	if h.userLimiter == nil {
		return nil
	}
	return []service.Service{h.userLimiter}
}

// attachUserRateLimit attaches the user state to a connected client. The state is resolved once
// per connection and released on disconnect. Anonymous connections are not attached.
func (h *Handler) attachUserRateLimit(c *centrifuge.Client) {
	if h.userLimiter == nil || c.UserID() == "" {
		return
	}
	st, ok := c.ExtState().(*ratelimit.ConnState)
	if !ok {
		return
	}
	h.userLimiter.Attach(st, c.UserID())
	c.OnDisconnect(func(centrifuge.DisconnectEvent) {
		h.userLimiter.Detach(st)
	})
}

// userConnectAllowed checks and consumes user_command connect limit of userID. Anonymous users
// are not limited. Over the limit the rejection is counted.
func (h *Handler) userConnectAllowed(userID string) bool {
	if h.userLimiter == nil || userID == "" {
		return true
	}
	if h.userLimiter.ConnectAllowed(userID, time.Now()) {
		return true
	}
	metrics.RateLimitRejectsTotal.WithLabelValues(limiterUserCommand, ratelimit.OperationConnect.String()).Inc()
	return false
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
		st, ok := c.ExtState().(*ratelimit.ConnState)
		if !ok {
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
