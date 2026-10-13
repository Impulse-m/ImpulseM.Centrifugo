package ratelimit

import (
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// defaultScopeName names the scope shared by all operations which fall back to default buckets.
const defaultScopeName = "default"

// Operation is a client command which can be rate limited.
type Operation uint8

const (
	OperationSubscribe Operation = iota
	OperationUnsubscribe
	OperationPublish
	OperationHistory
	OperationPresence
	OperationPresenceStats
	OperationSubRefresh
	OperationRefresh
	OperationRPC
	// OperationConnect is checked only by user_command limits, on connect, without total.
	OperationConnect
	operationCount
)

var operationNames = [operationCount]string{
	OperationSubscribe:     "subscribe",
	OperationUnsubscribe:   "unsubscribe",
	OperationPublish:       "publish",
	OperationHistory:       "history",
	OperationPresence:      "presence",
	OperationPresenceStats: "presence_stats",
	OperationSubRefresh:    "sub_refresh",
	OperationRefresh:       "refresh",
	OperationRPC:           "rpc",
	OperationConnect:       "connect",
}

// String returns the configuration name of the operation.
func (o Operation) String() string {
	return operationNames[o]
}

// slotBucket is a bucket with a static index of its state in connection slots.
type slotBucket struct {
	slot   int
	bucket Bucket
}

// scope is a list of buckets which must all allow an operation. Nil scope means no limit.
// name and argv are used by Redis limits only: name is a part of the Redis key and argv holds
// the arguments of the rate limit script for the buckets.
type scope struct {
	name    string
	buckets []slotBucket
	argv    []string
}

// allowed reports whether all buckets of the scope allow an operation at now. It does not change state.
func (s *scope) allowed(slots []int64, now int64) bool {
	for i := range s.buckets {
		sb := &s.buckets[i]
		if _, ok := sb.bucket.next(now, slots[sb.slot]); !ok {
			return false
		}
	}
	return true
}

// commit consumes one operation from all buckets of the scope. Must be called only after allowed returned true.
func (s *scope) commit(slots []int64, now int64) {
	for i := range s.buckets {
		sb := &s.buckets[i]
		newTAT, _ := sb.bucket.next(now, slots[sb.slot])
		slots[sb.slot] = newTAT
	}
}

// opLimit resolves buckets of one operation. Overrides are keyed by namespace name for channel
// operations and by method for RPC.
type opLimit struct {
	base      *scope
	overrides map[string]*scope
}

// resolve returns the scope of the operation for key: an override for key when there is one,
// otherwise the base scope. Nil means no limit.
func (l *opLimit) resolve(key string) *scope {
	if s, ok := l.overrides[key]; ok {
		return s
	}
	return l.base
}

// needsKey reports whether resolution depends on the key (namespace or method).
func (l *opLimit) needsKey() bool {
	return len(l.overrides) > 0
}

// CommandPlan is a compiled rate limit of client commands. It is built once from configuration
// and is safe for concurrent use: all mutable state is kept in ConnState.
type CommandPlan struct {
	numSlots int
	// maxInterval is the longest interval of all buckets in the plan.
	maxInterval time.Duration
	total       *scope
	ops         [operationCount]opLimit
}

// slotAllocator assigns static slot indexes to buckets while a plan is compiled.
type slotAllocator struct {
	next        int
	maxInterval time.Duration
}

// compileScope compiles enabled buckets into a scope named name. Returns nil when there is nothing to limit.
func (a *slotAllocator) compileScope(name string, enabled bool, buckets configtypes.RateLimitBucketList) *scope {
	if !enabled || len(buckets) == 0 {
		return nil
	}
	s := &scope{name: name, buckets: make([]slotBucket, 0, len(buckets))}
	var maxInterval time.Duration
	for _, item := range buckets {
		interval := item.Interval.ToDuration()
		if interval > maxInterval {
			maxInterval = interval
		}
		s.buckets = append(s.buckets, slotBucket{
			slot:   a.next,
			bucket: NewBucket(interval, item.Rate),
		})
		a.next++
	}
	if maxInterval > a.maxInterval {
		a.maxInterval = maxInterval
	}
	s.argv = redisScriptArgs(s.buckets, maxInterval)
	return s
}

// baseScope returns buckets of the operation, or the default scope when the operation has none.
func (a *slotAllocator) baseScope(name string, enabled bool, buckets configtypes.RateLimitBucketList, def *scope) *scope {
	if s := a.compileScope(name, enabled, buckets); s != nil {
		return s
	}
	return def
}

// channelLimit compiles a channel operation with its namespace overrides. An override counts only
// when it is enabled and has buckets: per the docs an enabled override without buckets is treated
// the same as no override, so the base operation buckets apply, or default when there are none.
func (a *slotAllocator) channelLimit(op Operation, cfg configtypes.RateLimitChannelBuckets, def *scope) opLimit {
	name := op.String()
	limit := opLimit{base: a.baseScope(name, cfg.Enabled, cfg.Buckets, def)}
	for _, override := range cfg.NamespaceOverrides {
		if !override.Enabled || len(override.Buckets) == 0 {
			continue
		}
		if limit.overrides == nil {
			limit.overrides = make(map[string]*scope)
		}
		limit.overrides[override.NamespaceName] = a.compileScope(name+".ns."+override.NamespaceName, true, override.Buckets)
	}
	return limit
}

// rpcLimit compiles RPC buckets with method overrides, same resolution as channelLimit.
func (a *slotAllocator) rpcLimit(cfg configtypes.RateLimitRPCBuckets, def *scope) opLimit {
	limit := opLimit{base: a.baseScope(OperationRPC.String(), cfg.Enabled, cfg.Buckets, def)}
	for _, override := range cfg.MethodOverrides {
		if !override.Enabled || len(override.Buckets) == 0 {
			continue
		}
		if limit.overrides == nil {
			limit.overrides = make(map[string]*scope)
		}
		limit.overrides[override.Method] = a.compileScope(OperationRPC.String()+".m."+override.Method, true, override.Buckets)
	}
	return limit
}

// newCommandPlan compiles client command rate limits. Configuration must be validated before.
// Buckets are compiled once here, every bucket in every scope gets a static slot index.
// The default scope is shared by all operations which fall back to it: it is one bucket
// for all such commands, as documented.
func newCommandPlan(cfg configtypes.RateLimitClientCommand) *CommandPlan {
	alloc := &slotAllocator{}
	p := &CommandPlan{}
	p.total = alloc.compileScope("total", cfg.Total.Enabled, cfg.Total.Buckets)
	def := alloc.compileScope(defaultScopeName, cfg.Default.Enabled, cfg.Default.Buckets)
	wireOperations(alloc, &p.ops, &cfg.RateLimitOperations, def)

	p.numSlots = alloc.next
	p.maxInterval = alloc.maxInterval
	return p
}

// wireOperations compiles the operations shared by client command and user command limits into ops.
// Slots are assigned in the order of the calls, so it must be the same for all limits.
func wireOperations(
	alloc *slotAllocator,
	ops *[operationCount]opLimit,
	cfg *configtypes.RateLimitOperations,
	def *scope,
) {
	ops[OperationSubscribe] = alloc.channelLimit(OperationSubscribe, cfg.Subscribe, def)
	ops[OperationUnsubscribe] = alloc.channelLimit(OperationUnsubscribe, cfg.Unsubscribe, def)
	ops[OperationPublish] = alloc.channelLimit(OperationPublish, cfg.Publish, def)
	ops[OperationHistory] = alloc.channelLimit(OperationHistory, cfg.History, def)
	ops[OperationPresence] = alloc.channelLimit(OperationPresence, cfg.Presence, def)
	ops[OperationPresenceStats] = alloc.channelLimit(OperationPresenceStats, cfg.PresenceStats, def)
	ops[OperationSubRefresh] = alloc.channelLimit(OperationSubRefresh, cfg.SubRefresh, def)
	ops[OperationRefresh] = opLimit{base: alloc.baseScope(OperationRefresh.String(), cfg.Refresh.Enabled, cfg.Refresh.Buckets, def)}
	ops[OperationRPC] = alloc.rpcLimit(cfg.RPC, def)
}

// newUserPlan compiles user command rate limits. Resolution is the same as for client commands,
// connect is added. Connect resolves its own buckets, then default, and is not subject to total.
func newUserPlan(cfg configtypes.RateLimitUserCommand) *CommandPlan {
	alloc := &slotAllocator{}
	p := &CommandPlan{}
	p.total = alloc.compileScope("total", cfg.Total.Enabled, cfg.Total.Buckets)
	def := alloc.compileScope(defaultScopeName, cfg.Default.Enabled, cfg.Default.Buckets)
	p.ops[OperationConnect] = opLimit{base: alloc.baseScope(OperationConnect.String(), cfg.Connect.Enabled, cfg.Connect.Buckets, def)}
	wireOperations(alloc, &p.ops, &cfg.RateLimitOperations, def)

	p.numSlots = alloc.next
	p.maxInterval = alloc.maxInterval
	return p
}

// resolve returns buckets for operation op. key is a namespace name for channel operations
// and a method name for RPC. Nil means no limit.
func (p *CommandPlan) resolve(op Operation, key string) *scope {
	return p.ops[op].resolve(key)
}

// needsKey reports whether resolution of op depends on the key (namespace or method).
func (p *CommandPlan) needsKey(op Operation) bool {
	return p.ops[op].needsKey()
}

// allow checks the operation against its buckets and then against total. Nothing is consumed
// unless both allow it: a rejected command does not count against total.
func (p *CommandPlan) allow(slots []int64, now int64, op Operation, key string) bool {
	s := p.resolve(op, key)
	if s != nil && !s.allowed(slots, now) {
		return false
	}
	if p.total != nil && !p.total.allowed(slots, now) {
		return false
	}
	if s != nil {
		s.commit(slots, now)
	}
	if p.total != nil {
		p.total.commit(slots, now)
	}
	return true
}

// allowConnect checks and consumes one connect of a user. Only connect buckets apply, total does not.
func (p *CommandPlan) allowConnect(slots []int64, now int64) bool {
	s := p.ops[OperationConnect].base
	if s == nil {
		return true
	}
	if !s.allowed(slots, now) {
		return false
	}
	s.commit(slots, now)
	return true
}
