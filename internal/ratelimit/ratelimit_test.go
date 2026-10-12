package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func testList(interval time.Duration, rate int) configtypes.RateLimitBucketList {
	return configtypes.RateLimitBucketList{{Interval: configtypes.Duration(interval), Rate: rate}}
}

func testContainer(interval time.Duration, rate int) configtypes.RateLimitBuckets {
	return configtypes.RateLimitBuckets{Enabled: true, Buckets: testList(interval, rate)}
}

func testChannel(interval time.Duration, rate int) configtypes.RateLimitChannelBuckets {
	return configtypes.RateLimitChannelBuckets{Enabled: true, Buckets: testList(interval, rate)}
}

// testNow returns time for offset d from a fixed monotonic reference point.
func testNow(base time.Time, d time.Duration) time.Time {
	return base.Add(d)
}

func TestBucketBurstAndRefill(t *testing.T) {
	base := time.Now()
	b := NewBucket(time.Second, 5)
	var tat int64

	// Burst of rate operations is allowed at once.
	now := nowNano(base)
	for i := 0; i < 5; i++ {
		next, ok := b.next(now, tat)
		require.True(t, ok, "operation %d of the burst", i)
		tat = next
	}
	_, ok := b.next(now, tat)
	require.False(t, ok, "operation over the burst must be rejected")

	// One token is refilled every interval / rate.
	_, ok = b.next(nowNano(testNow(base, 199*time.Millisecond)), tat)
	require.False(t, ok, "token must not be refilled before emission interval")
	next, ok := b.next(nowNano(testNow(base, 200*time.Millisecond)), tat)
	require.True(t, ok, "token must be refilled after emission interval")
	tat = next
	_, ok = b.next(nowNano(testNow(base, 200*time.Millisecond)), tat)
	require.False(t, ok, "only one token must be refilled")

	// Idle bucket is full again after the whole interval.
	_, ok = b.next(nowNano(testNow(base, 10*time.Second)), tat)
	require.True(t, ok)
}

func TestBucketAllowAtomic(t *testing.T) {
	base := time.Now()
	b := NewBucket(time.Second, 3)
	var tat atomic.Int64
	now := nowNano(base)
	require.True(t, b.allowAtomic(&tat, now))
	require.True(t, b.allowAtomic(&tat, now))
	require.True(t, b.allowAtomic(&tat, now))
	require.False(t, b.allowAtomic(&tat, now))
}

func TestCommandPlanMultiBucketAllOrNothing(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: configtypes.RateLimitBucketList{
				{Interval: configtypes.Duration(time.Second), Rate: 5},
				{Interval: configtypes.Duration(10 * time.Second), Rate: 6},
			},
		},
	}
	p := newCommandPlan(cfg)
	slots := make([]int64, p.numSlots)

	// Short bucket allows 5 at once, long bucket allows 6 in total.
	for i := 0; i < 5; i++ {
		require.True(t, p.allow(slots, nowNano(base), OperationPublish, ""))
	}
	before := append([]int64(nil), slots...)

	// Rejected by the short bucket: nothing is consumed from the long one either.
	require.False(t, p.allow(slots, nowNano(base), OperationPublish, ""))
	require.Equal(t, before, slots)

	// After one second the short bucket refills and the 6th publish is allowed. The long bucket
	// (6 per 10s) then has no tokens left, so the 7th publish is rejected by it.
	later := nowNano(testNow(base, time.Second))
	require.True(t, p.allow(slots, later, OperationPublish, ""))
	require.False(t, p.allow(slots, later, OperationPublish, ""))
}

func TestCommandPlanTotalOrdering(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Total:   testContainer(time.Second, 3),
		Default: testContainer(time.Second, 10),
		Publish: testChannel(time.Second, 1),
	}
	p := newCommandPlan(cfg)
	slots := make([]int64, p.numSlots)
	now := nowNano(base)

	// Publish is limited to 1 per second by its own bucket.
	require.True(t, p.allow(slots, now, OperationPublish, ""))
	require.False(t, p.allow(slots, now, OperationPublish, ""))
	require.False(t, p.allow(slots, now, OperationPublish, ""))

	// Only one publish has consumed total so far, the two rejected publishes did not.
	require.True(t, p.allow(slots, now, OperationHistory, ""))
	require.True(t, p.allow(slots, now, OperationHistory, ""))

	// Total is exhausted now. A rejected by total command must not consume its default bucket.
	defaultSlot := p.ops[OperationHistory].base.buckets[0].slot
	before := slots[defaultSlot]
	require.False(t, p.allow(slots, now, OperationHistory, ""))
	require.Equal(t, before, slots[defaultSlot])
}

func TestCommandPlanDefaultIsShared(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Default: testContainer(time.Second, 2),
	}
	p := newCommandPlan(cfg)
	slots := make([]int64, p.numSlots)
	now := nowNano(base)

	// Default is one bucket for all operations which fall back to it.
	require.True(t, p.allow(slots, now, OperationSubscribe, ""))
	require.True(t, p.allow(slots, now, OperationHistory, ""))
	require.False(t, p.allow(slots, now, OperationPublish, ""))
}

func TestCommandPlanResolution(t *testing.T) {
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Default: testContainer(time.Second, 10),
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 5),
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				// Replaces base buckets.
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Second, 20)},
				// Enabled without buckets falls back to default, not to base.
				{NamespaceName: "notif", Enabled: true},
				// Disabled override is ignored.
				{NamespaceName: "off", Enabled: false, Buckets: testList(time.Second, 1)},
			},
		},
		Subscribe: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 3),
		},
		Refresh: testContainer(time.Second, 7),
		RPC: configtypes.RateLimitRPCBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 4),
			MethodOverrides: configtypes.RateLimitMethodOverrides{
				{Method: "slow", Enabled: true, Buckets: testList(20*time.Second, 1)},
				{Method: "empty", Enabled: true},
				{Method: "off", Enabled: false, Buckets: testList(time.Second, 1)},
			},
		},
	}
	p := newCommandPlan(cfg)

	def := NewBucket(time.Second, 10)
	base5 := NewBucket(time.Second, 5)
	chat20 := NewBucket(time.Second, 20)
	sub3 := NewBucket(time.Second, 3)
	ref7 := NewBucket(time.Second, 7)
	rpc4 := NewBucket(time.Second, 4)
	slow := NewBucket(20*time.Second, 1)

	tests := []struct {
		name string
		op   Operation
		key  string
		want []Bucket
	}{
		{name: "publish base", op: OperationPublish, key: "", want: []Bucket{base5}},
		{name: "publish namespace override replaces base", op: OperationPublish, key: "chat", want: []Bucket{chat20}},
		{name: "publish enabled override without buckets uses default", op: OperationPublish, key: "notif", want: []Bucket{def}},
		{name: "publish disabled override is ignored", op: OperationPublish, key: "off", want: []Bucket{base5}},
		{name: "publish unknown namespace uses base", op: OperationPublish, key: "other", want: []Bucket{base5}},
		{name: "subscribe base", op: OperationSubscribe, key: "", want: []Bucket{sub3}},
		{name: "history not configured uses default", op: OperationHistory, key: "", want: []Bucket{def}},
		{name: "presence not configured uses default", op: OperationPresence, key: "chat", want: []Bucket{def}},
		{name: "refresh base", op: OperationRefresh, key: "", want: []Bucket{ref7}},
		{name: "rpc base", op: OperationRPC, key: "", want: []Bucket{rpc4}},
		{name: "rpc method override", op: OperationRPC, key: "slow", want: []Bucket{slow}},
		{name: "rpc enabled override without buckets uses default", op: OperationRPC, key: "empty", want: []Bucket{def}},
		{name: "rpc disabled override is ignored", op: OperationRPC, key: "off", want: []Bucket{rpc4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := p.resolve(tt.op, tt.key)
			require.NotNil(t, s)
			got := make([]Bucket, 0, len(s.buckets))
			for _, sb := range s.buckets {
				got = append(got, sb.bucket)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCommandPlanResolutionUnlimited(t *testing.T) {
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true},
			},
		},
	}
	p := newCommandPlan(cfg)
	// No base and no default: operations are unlimited, including enabled override without buckets.
	require.Nil(t, p.resolve(OperationPublish, ""))
	require.Nil(t, p.resolve(OperationPublish, "chat"))
	require.Nil(t, p.resolve(OperationRPC, "any"))
	require.Nil(t, p.resolve(OperationRefresh, ""))
	require.True(t, p.allow(make([]int64, p.numSlots), 0, OperationPublish, "chat"))
}

func TestCommandPlanNamespaceOverrideReplacesBase(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 2),
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Second, 5)},
			},
		},
	}
	p := newCommandPlan(cfg)
	slots := make([]int64, p.numSlots)
	now := nowNano(base)

	// Base bucket (2) is not checked for chat: 5 publishes are allowed there.
	for i := 0; i < 5; i++ {
		require.True(t, p.allow(slots, now, OperationPublish, "chat"))
	}
	require.False(t, p.allow(slots, now, OperationPublish, "chat"))

	// Other namespaces are limited by base buckets.
	require.True(t, p.allow(slots, now, OperationPublish, ""))
	require.True(t, p.allow(slots, now, OperationPublish, ""))
	require.False(t, p.allow(slots, now, OperationPublish, ""))
}

func TestCommandPlanSlotsAreStaticAndUnique(t *testing.T) {
	cfg := configtypes.RateLimitClientCommand{
		Enabled: true,
		Total:   testContainer(time.Second, 100),
		Default: testContainer(time.Second, 10),
		Publish: configtypes.RateLimitChannelBuckets{
			Enabled: true,
			Buckets: testList(time.Second, 5),
			NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
				{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Second, 20)},
				{NamespaceName: "a", Enabled: true, Buckets: configtypes.RateLimitBucketList{
					{Interval: configtypes.Duration(time.Second), Rate: 1},
					{Interval: configtypes.Duration(time.Minute), Rate: 2},
				}},
			},
		},
	}
	p := newCommandPlan(cfg)
	seenSlots := map[int]bool{}
	seenScopes := map[*scope]bool{}
	mark := func(s *scope) {
		if s == nil || seenScopes[s] {
			return
		}
		seenScopes[s] = true
		for _, sb := range s.buckets {
			require.False(t, seenSlots[sb.slot], "slot %d is used twice", sb.slot)
			seenSlots[sb.slot] = true
		}
	}
	mark(p.total)
	for _, limit := range p.ops {
		mark(limit.base)
		for _, s := range limit.overrides {
			mark(s)
		}
	}
	// total, default, publish base, chat override, two buckets of override a.
	require.Len(t, seenSlots, 6)
	require.Equal(t, 6, p.numSlots)
}

func TestClientLimiterDisabled(t *testing.T) {
	l := NewClientLimiter(configtypes.RateLimit{})
	st := l.NewConnState()
	require.Nil(t, st.slots)
	require.True(t, l.CommandAllowed(st, time.Now(), OperationPublish, ""))
	require.True(t, l.ErrorAllowed(st, time.Now()))
	require.False(t, l.NeedsKey(OperationPublish))
}

func TestClientLimiterClientError(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimit{
		ClientError: configtypes.RateLimitClientError{
			Enabled: true,
			Total:   testContainer(5*time.Second, 3),
		},
	}
	l := NewClientLimiter(cfg)
	st := l.NewConnState()
	require.Len(t, st.errorTAT, 1)

	for i := 0; i < 3; i++ {
		require.True(t, l.ErrorAllowed(st, base))
	}
	require.False(t, l.ErrorAllowed(st, base))

	// Errors are refilled over time.
	require.True(t, l.ErrorAllowed(st, testNow(base, 5*time.Second/3)))
}

func TestClientLimiterClientErrorConcurrent(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimit{
		ClientError: configtypes.RateLimitClientError{
			Enabled: true,
			Total:   testContainer(time.Minute, 20),
		},
	}
	l := NewClientLimiter(cfg)
	st := l.NewConnState()

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.ErrorAllowed(st, base) {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(20), allowed.Load())
}

func TestClientLimiterCommandAllowedAndNeedsKey(t *testing.T) {
	base := time.Now()
	cfg := configtypes.RateLimit{
		ClientCommand: configtypes.RateLimitClientCommand{
			Enabled: true,
			Publish: configtypes.RateLimitChannelBuckets{
				Enabled: true,
				Buckets: testList(time.Second, 1),
				NamespaceOverrides: configtypes.RateLimitNamespaceOverrides{
					{NamespaceName: "chat", Enabled: true, Buckets: testList(time.Second, 2)},
				},
			},
		},
	}
	l := NewClientLimiter(cfg)
	require.True(t, l.NeedsKey(OperationPublish))
	require.False(t, l.NeedsKey(OperationHistory))

	st := l.NewConnState()
	require.True(t, l.CommandAllowed(st, base, OperationPublish, "chat"))
	require.True(t, l.CommandAllowed(st, base, OperationPublish, "chat"))
	require.False(t, l.CommandAllowed(st, base, OperationPublish, "chat"))
	require.True(t, l.CommandAllowed(st, base, OperationPublish, ""))
	require.False(t, l.CommandAllowed(st, base, OperationPublish, ""))
	require.True(t, l.CommandAllowed(st, base, OperationHistory, ""))
}

func TestOperationString(t *testing.T) {
	require.Equal(t, "presence_stats", OperationPresenceStats.String())
	require.Equal(t, "rpc", OperationRPC.String())
}
