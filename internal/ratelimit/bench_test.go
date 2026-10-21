package ratelimit

import (
	"fmt"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// bucketsForBench returns a client command config with n identical buckets in the default
// scope. Rate is high so every check is allowed and commits, which measures the full path.
func bucketsForBench(n int) configtypes.RateLimitClientCommand {
	list := make(configtypes.RateLimitBucketList, 0, n)
	for i := 0; i < n; i++ {
		list = append(list, configtypes.RateLimitBucket{
			Interval: configtypes.Duration(1e9),
			Rate:     1e9,
		})
	}
	return configtypes.RateLimitClientCommand{
		Enabled: true,
		RateLimitOperations: configtypes.RateLimitOperations{
			Default: configtypes.RateLimitBuckets{Enabled: true, Buckets: list},
		},
	}
}

func TestClientBucketCheckNoAllocs(t *testing.T) {
	p := newCommandPlan(bucketsForBench(4))
	slots := make([]int64, p.numSlots)
	var now int64
	allocs := testing.AllocsPerRun(1000, func() {
		now += 1000
		p.allow(slots, now, OperationPublish, "")
	})
	require.Zero(t, allocs)
}

func BenchmarkClientBucketCheck(b *testing.B) {
	for _, n := range []int{1, 4} {
		b.Run(fmt.Sprintf("buckets=%d", n), func(b *testing.B) {
			p := newCommandPlan(bucketsForBench(n))
			slots := make([]int64, p.numSlots)
			var now int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now += 1000
				if p.allow(slots, now, OperationPublish, "") != 0 {
					b.Fatal("unexpected rejection in benchmark")
				}
			}
		})
	}
}
