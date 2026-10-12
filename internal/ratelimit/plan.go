package ratelimit

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

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
type scope struct {
	buckets []slotBucket
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

// CommandPlan is a compiled rate limit of client commands. It is built once from configuration
// and is safe for concurrent use: all mutable state is kept in ConnState.
type CommandPlan struct {
	numSlots int
	total    *scope
	ops      [operationCount]opLimit
}

// slotAllocator assigns static slot indexes to buckets while a plan is compiled.
type slotAllocator struct {
	next int
}

// compileScope compiles enabled buckets into a scope. Returns nil when there is nothing to limit.
func (a *slotAllocator) compileScope(enabled bool, buckets configtypes.RateLimitBucketList) *scope {
	if !enabled || len(buckets) == 0 {
		return nil
	}
	s := &scope{buckets: make([]slotBucket, 0, len(buckets))}
	for _, item := range buckets {
		s.buckets = append(s.buckets, slotBucket{
			slot:   a.next,
			bucket: NewBucket(item.Interval.ToDuration(), item.Rate),
		})
		a.next++
	}
	return s
}

// baseScope returns buckets of the operation, or the default scope when the operation has none.
func (a *slotAllocator) baseScope(enabled bool, buckets configtypes.RateLimitBucketList, def *scope) *scope {
	if s := a.compileScope(enabled, buckets); s != nil {
		return s
	}
	return def
}

// overrideScope returns buckets of an enabled override. An override without buckets falls back to default.
func (a *slotAllocator) overrideScope(buckets configtypes.RateLimitBucketList, def *scope) *scope {
	if len(buckets) == 0 {
		return def
	}
	return a.compileScope(true, buckets)
}

func (a *slotAllocator) channelLimit(cfg configtypes.RateLimitChannelBuckets, def *scope) opLimit {
	limit := opLimit{base: a.baseScope(cfg.Enabled, cfg.Buckets, def)}
	for _, override := range cfg.NamespaceOverrides {
		if !override.Enabled {
			continue
		}
		if limit.overrides == nil {
			limit.overrides = make(map[string]*scope)
		}
		limit.overrides[override.NamespaceName] = a.overrideScope(override.Buckets, def)
	}
	return limit
}

func (a *slotAllocator) rpcLimit(cfg configtypes.RateLimitRPCBuckets, def *scope) opLimit {
	limit := opLimit{base: a.baseScope(cfg.Enabled, cfg.Buckets, def)}
	for _, override := range cfg.MethodOverrides {
		if !override.Enabled {
			continue
		}
		if limit.overrides == nil {
			limit.overrides = make(map[string]*scope)
		}
		limit.overrides[override.Method] = a.overrideScope(override.Buckets, def)
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
	p.total = alloc.compileScope(cfg.Total.Enabled, cfg.Total.Buckets)
	def := alloc.compileScope(cfg.Default.Enabled, cfg.Default.Buckets)

	p.ops[OperationSubscribe] = alloc.channelLimit(cfg.Subscribe, def)
	p.ops[OperationUnsubscribe] = alloc.channelLimit(cfg.Unsubscribe, def)
	p.ops[OperationPublish] = alloc.channelLimit(cfg.Publish, def)
	p.ops[OperationHistory] = alloc.channelLimit(cfg.History, def)
	p.ops[OperationPresence] = alloc.channelLimit(cfg.Presence, def)
	p.ops[OperationPresenceStats] = alloc.channelLimit(cfg.PresenceStats, def)
	p.ops[OperationSubRefresh] = alloc.channelLimit(cfg.SubRefresh, def)
	p.ops[OperationRefresh] = opLimit{base: alloc.baseScope(cfg.Refresh.Enabled, cfg.Refresh.Buckets, def)}
	p.ops[OperationRPC] = alloc.rpcLimit(cfg.RPC, def)

	p.numSlots = alloc.next
	return p
}

// resolve returns buckets for operation op. key is a namespace name for channel operations
// and a method name for RPC. Nil means no limit.
func (p *CommandPlan) resolve(op Operation, key string) *scope {
	limit := &p.ops[op]
	if s, ok := limit.overrides[key]; ok {
		return s
	}
	return limit.base
}

// needsKey reports whether resolution of op depends on the key (namespace or method).
func (p *CommandPlan) needsKey(op Operation) bool {
	return len(p.ops[op].overrides) > 0
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
