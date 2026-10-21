package ratelimit

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkUserAttachDetach measures a connection which attaches and detaches its user, so every detach is a release to
// zero: the state is idle until the next attach. The eviction run of the limiter goes on for the life of the benchmark.
func BenchmarkUserAttachDetach(b *testing.B) {
	limiter := NewUserLimiter(userConfig(time.Second, 10))
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- limiter.Run(ctx)
	}()
	var seq atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		userID := fmt.Sprintf("user-%d", seq.Add(1))
		st := &ConnState{}
		for pb.Next() {
			limiter.Attach(st, userID)
			limiter.Detach(st)
		}
	})
	cancel()
	<-runDone
}

// BenchmarkUserEvict measures one eviction run over 1M attached states, with no idle state and with 1,000 idle states
// which are not due yet. The run walks the shards and the idle lists, not the attached states.
func BenchmarkUserEvict(b *testing.B) {
	const attached int = 1000000
	for _, idle := range []int{0, 1000} {
		b.Run(fmt.Sprintf("attached=%d,idle=%d", attached, idle), func(b *testing.B) {
			base := time.Now()
			limiter := NewUserLimiter(userConfig(time.Second, 10))
			limiter.store.now = func() time.Time {
				return base
			}
			store := limiter.store
			for i := range attached {
				store.acquire(fmt.Sprintf("attached-%d", i))
			}
			for i := range idle {
				store.release(store.acquire(fmt.Sprintf("idle-%d", i)))
			}
			now := nowNano(base)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				store.evict(now)
			}
		})
	}
}

// BenchmarkUserCommandCheck measures the user path with every parallel goroutine on its own
// connection. same_user puts all goroutines on one user state (contention on its mutex),
// many_users spreads them over 1024 users (contention on the shards only).
func BenchmarkUserCommandCheck(b *testing.B) {
	// Rate is high so every check is allowed and commits, which measures the full path.
	cfg := userConfig(time.Second, 1e9)

	b.Run("same_user", func(b *testing.B) {
		limiter := NewUserLimiter(cfg)
		var rejected atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			st := &ConnState{}
			limiter.Attach(st, "user")
			defer limiter.Detach(st)
			for pb.Next() {
				if limiter.CommandAllowed(st, time.Time{}, OperationPublish, "") != 0 {
					rejected.Add(1)
				}
			}
		})
		if rejected.Load() != 0 {
			b.Fatalf("unexpected rejections: %d", rejected.Load())
		}
	})

	b.Run("many_users", func(b *testing.B) {
		limiter := NewUserLimiter(cfg)
		var seq atomic.Int64
		var rejected atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			userID := fmt.Sprintf("user-%d", seq.Add(1)%1024)
			st := &ConnState{}
			limiter.Attach(st, userID)
			defer limiter.Detach(st)
			for pb.Next() {
				if limiter.CommandAllowed(st, time.Time{}, OperationPublish, "") != 0 {
					rejected.Add(1)
				}
			}
		})
		if rejected.Load() != 0 {
			b.Fatalf("unexpected rejections: %d", rejected.Load())
		}
	})
}
