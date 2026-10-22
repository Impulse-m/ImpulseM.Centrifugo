package ratelimit

import (
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/redis/rueidis"
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
	OperationMapPublish
	OperationMapRemove
	OperationTrack
	OperationUntrack
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
	OperationMapPublish:    "map_publish",
	OperationMapRemove:     "map_remove",
	OperationTrack:         "track",
	OperationUntrack:       "untrack",
	OperationConnect:       "connect",
}

// String returns the configuration name of the operation.
func (o Operation) String() string {
	return operationNames[o]
}

// slotBucket is a bucket with a static index of its state in connection slots.
type slotBucket struct {
	slot int
	lim  limit
}

// scope is a list of buckets which must all allow an operation, all of one algorithm. Nil scope means no limit.
// name, redisName, script and argv are used by Redis limits only: redisName is a part of the Redis key, script
// and argv are the Redis script of the algorithm and its arguments for the buckets.
type scope struct {
	// redisName is the scope name with the algorithm suffix. The Redis key is built from it, see redisKey.
	redisName string
	algo      Algorithm
	script    *rueidis.Lua
	// words is the number of state words of one bucket, see Algorithm.stateWords.
	words int
	// tokenOnly is set at compile time when algo is the token bucket. Such a scope calls the token bucket directly.
	tokenOnly bool
	buckets   []slotBucket
	argv      []string
}

// tokenWait returns the largest wait of token buckets at now, 0 when all of them allow an operation. It does not
// change state. It has no call except gcraWait, so the compiler inlines it into CommandPlan.allow, the hot path of the
// default limits, which then makes no call per bucket. Keep it free of other calls: a call makes it too big to inline.
func tokenWait(buckets []slotBucket, slots []int64, now int64) int64 {
	var worst int64
	for i := range buckets {
		sb := &buckets[i]
		wait := gcraWait(sb.lim.emission, sb.lim.tolerance, slots[sb.slot], now)
		if wait > worst {
			worst = wait
		}
	}
	return worst
}

// tokenCommit consumes one operation from token buckets. Must be called only after tokenWait returned 0. It is
// inlined into CommandPlan.allow for the same reason as tokenWait.
func tokenCommit(buckets []slotBucket, slots []int64, now int64) {
	for i := range buckets {
		sb := &buckets[i]
		slots[sb.slot] = gcraTAT(sb.lim.emission, slots[sb.slot], now)
	}
}

// wait returns the largest wait of the buckets of the scope at now, 0 when all of them allow an operation. It does not
// change state. Token bucket scopes call the token bucket directly, the others call their algorithm.
func (s *scope) wait(slots []int64, now int64) int64 {
	if s.tokenOnly {
		return tokenWait(s.buckets, slots, now)
	}
	return s.algoWait(slots, now)
}

// algoWait is wait for the scopes of the window algorithms.
func (s *scope) algoWait(slots []int64, now int64) int64 {
	var worst int64
	for i := range s.buckets {
		sb := &s.buckets[i]
		worst = max(worst, s.algo.wait(&sb.lim, slots[sb.slot:sb.slot+s.words], now))
	}
	return worst
}

// commit consumes one operation from all buckets of the scope. Must be called only after wait returned 0.
func (s *scope) commit(slots []int64, now int64) {
	if s.tokenOnly {
		tokenCommit(s.buckets, slots, now)
		return
	}
	s.algoCommit(slots, now)
}

// algoCommit is commit for the scopes of the window algorithms.
func (s *scope) algoCommit(slots []int64, now int64) {
	for i := range s.buckets {
		sb := &s.buckets[i]
		s.algo.take(&sb.lim, slots[sb.slot:sb.slot+s.words], now)
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
	// maxInterval is the longest interval of all buckets. Only the user plan sets and reads it, for userIdleTimeout.
	maxInterval time.Duration
	// tokenOnly is set when no scope of the plan uses a window algorithm. Only the user plan sets and reads it.
	tokenOnly bool
	total     *scope
	ops       [operationCount]opLimit
}

// slotAllocator assigns static slot indexes to buckets while a plan is compiled.
type slotAllocator struct {
	next        int
	maxInterval time.Duration
	windowed    bool
}

// HasBuckets reports whether a limit is enabled and has at least one bucket. Only such a limit is compiled,
// so code which describes compiled limits uses this check too.
func HasBuckets(enabled bool, buckets configtypes.RateLimitBucketList) bool {
	return enabled && len(buckets) > 0
}

// compileScope compiles enabled buckets of the algorithm named algorithm into a scope named name. Returns nil when
// there is nothing to limit. Validation runs before, so the algorithm name is known.
func (a *slotAllocator) compileScope(
	name string,
	enabled bool,
	buckets configtypes.RateLimitBucketList,
	algorithm string,
) *scope {
	if !HasBuckets(enabled, buckets) {
		return nil
	}
	algo := algorithmNamed(algorithm)
	_, tokenOnly := algo.(tokenBucket)
	s := &scope{
		algo:      algo,
		script:    algo.redisScript(),
		words:     algo.stateWords(),
		tokenOnly: tokenOnly,
		buckets:   make([]slotBucket, 0, len(buckets)),
	}
	lims := make([]limit, 0, len(buckets))
	var maxInterval time.Duration
	for _, item := range buckets {
		interval := item.Interval.ToDuration()
		maxInterval = max(maxInterval, interval)
		lim := algo.newLimit(interval, item.Rate)
		lims = append(lims, lim)
		s.buckets = append(s.buckets, slotBucket{slot: a.next, lim: lim})
		a.next += s.words
	}
	a.maxInterval = max(a.maxInterval, maxInterval)
	a.windowed = a.windowed || !tokenOnly
	s.redisName = name + algo.redisSuffix(lims)
	s.argv = algo.redisArgs(lims, maxInterval)
	return s
}

// baseScope returns buckets of the operation, or the default scope when the operation has none.
func (a *slotAllocator) baseScope(
	name string,
	enabled bool,
	buckets configtypes.RateLimitBucketList,
	algorithm string,
	def *scope,
) *scope {
	if s := a.compileScope(name, enabled, buckets, algorithm); s != nil {
		return s
	}
	return def
}

// channelLimit compiles a channel operation with its namespace overrides. An override counts only
// when it is enabled and has buckets: per the docs an enabled override without buckets is treated
// the same as no override, so the base operation buckets apply, or default when there are none.
func (a *slotAllocator) channelLimit(op Operation, cfg configtypes.RateLimitChannelBuckets, def *scope) opLimit {
	name := op.String()
	ol := opLimit{base: a.baseScope(name, cfg.Enabled, cfg.Buckets, cfg.Algorithm, def)}
	for _, override := range cfg.NamespaceOverrides {
		if !HasBuckets(override.Enabled, override.Buckets) {
			continue
		}
		if ol.overrides == nil {
			ol.overrides = make(map[string]*scope)
		}
		ol.overrides[override.NamespaceName] = a.compileScope(name+".ns."+override.NamespaceName, true, override.Buckets, override.Algorithm)
	}
	return ol
}

// rpcLimit compiles RPC buckets with method overrides, same resolution as channelLimit.
func (a *slotAllocator) rpcLimit(cfg configtypes.RateLimitRPCBuckets, def *scope) opLimit {
	ol := opLimit{base: a.baseScope(OperationRPC.String(), cfg.Enabled, cfg.Buckets, cfg.Algorithm, def)}
	for _, override := range cfg.MethodOverrides {
		if !HasBuckets(override.Enabled, override.Buckets) {
			continue
		}
		if ol.overrides == nil {
			ol.overrides = make(map[string]*scope)
		}
		ol.overrides[override.Method] = a.compileScope(OperationRPC.String()+".m."+override.Method, true, override.Buckets, override.Algorithm)
	}
	return ol
}

// newCommandPlan compiles client command rate limits. Configuration must be validated before.
// Buckets are compiled once here, every bucket in every scope gets a static slot index.
// The default scope is shared by all operations which fall back to it: it is one bucket
// for all such commands, as documented.
func newCommandPlan(cfg configtypes.RateLimitClientCommand) *CommandPlan {
	alloc := &slotAllocator{}
	p := &CommandPlan{}
	p.total = alloc.compileScope("total", cfg.Total.Enabled, cfg.Total.Buckets, cfg.Total.Algorithm)
	def := alloc.compileScope(defaultScopeName, cfg.Default.Enabled, cfg.Default.Buckets, cfg.Default.Algorithm)
	wireOperations(alloc, &p.ops, &cfg.RateLimitOperations, def)

	p.numSlots = alloc.next
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
	ops[OperationRefresh] = opLimit{base: alloc.baseScope(OperationRefresh.String(), cfg.Refresh.Enabled, cfg.Refresh.Buckets, cfg.Refresh.Algorithm, def)}
	ops[OperationRPC] = alloc.rpcLimit(cfg.RPC, def)
	ops[OperationMapPublish] = alloc.channelLimit(OperationMapPublish, cfg.MapPublish, def)
	ops[OperationMapRemove] = alloc.channelLimit(OperationMapRemove, cfg.MapRemove, def)
	ops[OperationTrack] = alloc.channelLimit(OperationTrack, cfg.Track, def)
	ops[OperationUntrack] = alloc.channelLimit(OperationUntrack, cfg.Untrack, def)
	// Map operations and track operations count against the operation they were before they existed, as publish and
	// sub_refresh. They share its scopes, so the budget is the same one.
	inheritUnset(ops, &cfg.MapPublish, OperationMapPublish, OperationPublish)
	inheritUnset(ops, &cfg.MapRemove, OperationMapRemove, OperationPublish)
	inheritUnset(ops, &cfg.Track, OperationTrack, OperationSubRefresh)
	inheritUnset(ops, &cfg.Untrack, OperationUntrack, OperationSubRefresh)
}

// inheritUnset makes op resolve the buckets it has no own of from parent, see wireOperations. The base is inherited
// when own has no base buckets. Namespace overrides of parent are inherited for the namespaces which own does not
// override, so a namespace override of op never leaves the other namespaces unlimited.
func inheritUnset(
	ops *[operationCount]opLimit,
	own *configtypes.RateLimitChannelBuckets,
	op Operation,
	parent Operation,
) {
	if !HasBuckets(own.Enabled, own.Buckets) {
		ops[op].base = ops[parent].base
	}
	for namespace, s := range ops[parent].overrides {
		if _, ok := ops[op].overrides[namespace]; ok {
			continue
		}
		if ops[op].overrides == nil {
			ops[op].overrides = make(map[string]*scope)
		}
		ops[op].overrides[namespace] = s
	}
}

// newUserPlan compiles user command rate limits. Resolution is the same as for client commands,
// connect is added. Connect resolves its own buckets, then default, and is not subject to total.
func newUserPlan(cfg configtypes.RateLimitUserCommand) *CommandPlan {
	alloc := &slotAllocator{}
	p := &CommandPlan{}
	p.total = alloc.compileScope("total", cfg.Total.Enabled, cfg.Total.Buckets, cfg.Total.Algorithm)
	def := alloc.compileScope(defaultScopeName, cfg.Default.Enabled, cfg.Default.Buckets, cfg.Default.Algorithm)
	p.ops[OperationConnect] = opLimit{base: alloc.baseScope(OperationConnect.String(), cfg.Connect.Enabled, cfg.Connect.Buckets, cfg.Connect.Algorithm, def)}
	wireOperations(alloc, &p.ops, &cfg.RateLimitOperations, def)

	p.numSlots = alloc.next
	p.maxInterval = alloc.maxInterval
	p.tokenOnly = !alloc.windowed
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

// allow checks the operation against its buckets and then against total. It returns 0 when the operation is allowed
// and consumes it from both. Otherwise it returns the largest wait of the two, and nothing is consumed: a rejected
// command does not count against total.
func (p *CommandPlan) allow(slots []int64, now int64, op Operation, key string) int64 {
	var wait int64
	s := p.resolve(op, key)
	// The token bucket is checked inline, see tokenWait; only the window algorithms make a call per bucket.
	if s != nil {
		if s.tokenOnly {
			wait = tokenWait(s.buckets, slots, now)
		} else {
			wait = s.algoWait(slots, now)
		}
	}
	if p.total != nil {
		wait = max(wait, p.total.wait(slots, now))
	}
	if wait > 0 {
		return wait
	}
	if s != nil {
		if s.tokenOnly {
			tokenCommit(s.buckets, slots, now)
		} else {
			s.algoCommit(slots, now)
		}
	}
	if p.total != nil {
		p.total.commit(slots, now)
	}
	return 0
}

// allowConnect checks and consumes one connect of a user. Only connect buckets apply, total does not.
func (p *CommandPlan) allowConnect(slots []int64, now int64) bool {
	s := p.ops[OperationConnect].base
	if s == nil {
		return true
	}
	if s.wait(slots, now) > 0 {
		return false
	}
	s.commit(slots, now)
	return true
}
