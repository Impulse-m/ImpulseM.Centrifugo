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
	wireOperations(alloc, &p.ops, &cfg.RateLimitOperations, def)
	return p
}
