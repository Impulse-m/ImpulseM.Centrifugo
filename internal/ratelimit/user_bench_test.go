package ratelimit

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

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
				if !limiter.CommandAllowed(st, time.Time{}, OperationPublish, "") {
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
				if !limiter.CommandAllowed(st, time.Time{}, OperationPublish, "") {
					rejected.Add(1)
				}
			}
		})
		if rejected.Load() != 0 {
			b.Fatalf("unexpected rejections: %d", rejected.Load())
		}
	})
}
