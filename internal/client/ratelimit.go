package client

import (
	"errors"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/ratelimit"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// limiterClientCommand is a value of the limiter metric label for per-connection command limits.
const limiterClientCommand = "client_command"

// setupRateLimit registers node hooks for client rate limits. Nothing is registered when
// client_command and client_error are both disabled.
func (h *Handler) setupRateLimit(rl configtypes.RateLimit) {
	if !rl.ClientCommand.Enabled && !rl.ClientError.Enabled {
		return
	}
	limiter := ratelimit.NewClientLimiter(rl)
	// Connection state is allocated on connect, so the read hook is needed for client_error too.
	h.node.OnCommandRead(h.rateLimitCommandRead(limiter, rl.ClientCommand.Enabled))
	if rl.ClientError.Enabled {
		h.node.OnCommandProcessed(h.rateLimitCommandProcessed(limiter))
	}
}

// rateLimitCommandRead allocates connection rate limit state on connect and, when checkCommands
// is set, checks each command against client command limits.
func (h *Handler) rateLimitCommandRead(limiter *ratelimit.ClientLimiter, checkCommands bool) centrifuge.CommandReadHandler {
	return func(c *centrifuge.Client, e centrifuge.CommandReadEvent) error {
		if e.Command.Connect != nil {
			// Keep existing state: a repeated connect must not reset the buckets of the connection.
			if c.ExtState() == nil {
				c.SetExtState(limiter.NewConnState())
			}
			return nil
		}
		if !checkCommands {
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
		if limiter.NeedsKey(op) {
			key = name
			if op != ratelimit.OperationRPC {
				key = h.rateLimitNamespace(name)
			}
		}
		if !limiter.CommandAllowed(st, e.Started, op, key) {
			metrics.RateLimitRejectsTotal.WithLabelValues(limiterClientCommand, op.String()).Inc()
			return centrifuge.ErrorTooManyRequests
		}
		return nil
	}
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
