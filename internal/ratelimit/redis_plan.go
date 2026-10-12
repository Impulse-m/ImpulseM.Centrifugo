package ratelimit

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// redisPlan is a compiled redis_user_command configuration. Resolution is the same as for user_command,
// except that total is not supported. Every resolved scope is one Redis key, named by the scope.
type redisPlan struct {
	ops [operationCount]opLimit
}

// newRedisUserPlan compiles redis_user_command limits. Configuration must be validated before.
// Wiring mirrors newUserPlan: the same operations, overrides and fallbacks, only scopes differ.
func newRedisUserPlan(cfg configtypes.RateLimitRedisUserCommand) *redisPlan {
	alloc := &slotAllocator{}
	def := alloc.compileScope(defaultScopeName, cfg.Default.Enabled, cfg.Default.Buckets)
	p := &redisPlan{}

	p.ops[OperationConnect] = opLimit{base: alloc.baseScope(OperationConnect.String(), cfg.Connect.Enabled, cfg.Connect.Buckets, def)}
	p.ops[OperationSubscribe] = alloc.channelLimit(OperationSubscribe, cfg.Subscribe, def)
	p.ops[OperationUnsubscribe] = alloc.channelLimit(OperationUnsubscribe, cfg.Unsubscribe, def)
	p.ops[OperationPublish] = alloc.channelLimit(OperationPublish, cfg.Publish, def)
	p.ops[OperationHistory] = alloc.channelLimit(OperationHistory, cfg.History, def)
	p.ops[OperationPresence] = alloc.channelLimit(OperationPresence, cfg.Presence, def)
	p.ops[OperationPresenceStats] = alloc.channelLimit(OperationPresenceStats, cfg.PresenceStats, def)
	p.ops[OperationSubRefresh] = alloc.channelLimit(OperationSubRefresh, cfg.SubRefresh, def)
	p.ops[OperationRefresh] = opLimit{base: alloc.baseScope(OperationRefresh.String(), cfg.Refresh.Enabled, cfg.Refresh.Buckets, def)}
	p.ops[OperationRPC] = alloc.rpcLimit(cfg.RPC, def)
	return p
}
