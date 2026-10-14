package rpcdedup

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// benchMemoryOptions returns the options of the benchmarks: the configured defaults, with the budgets large enough
// that no benchmark reaches them, so the cost measured is the call path.
func benchMemoryOptions() MemoryOptions {
	return MemoryOptions{
		MaxEntries:         200000,
		MaxBytes:           1 << 30,
		MaxRecordsPerScope: 1 << 20,
		TTL:                120 * time.Second,
		MaxReplyBytes:      65536,
	}
}

// BenchmarkMemoryBeginCompleteParallel is BenchmarkMemoryBeginComplete run by GOMAXPROCS goroutines, over 1024
// scopes, as concurrent connections of a node use the store.
func BenchmarkMemoryBeginCompleteParallel(b *testing.B) {
	store := NewMemory(benchMemoryOptions())
	scopes := make([]string, 1024)
	for i := range scopes {
		scopes[i] = "s:" + strconv.Itoa(i)
	}
	reply := []byte("ok")
	fingerprint := Fingerprint("users.get", []byte("x"))
	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			key := Key{Scope: scopes[n%uint64(len(scopes))], MsgID: n}
			flight := NewFlightWithFingerprint(fingerprint)
			lookup, err := store.Begin(key, flight)
			if err != nil || lookup.Outcome != OutcomeMiss {
				b.Error("unexpected begin outcome")
				return
			}
			if err = store.Complete(key, flight, Record{Reply: reply}); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkMemoryBeginComplete is one tracked call in the node-local store: a miss which is completed. The
// keys do not repeat, so the store stays at its bound.
func BenchmarkMemoryBeginComplete(b *testing.B) {
	store := NewMemory(benchMemoryOptions())
	reply := []byte("ok")
	fingerprint := Fingerprint("users.get", []byte("x"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Key{Scope: "s:bench", MsgID: uint64(i)}
		flight := NewFlightWithFingerprint(fingerprint)
		var lookup Lookup
		var err error
		lookup, err = store.Begin(key, flight)
		if err != nil || lookup.Outcome != OutcomeMiss {
			b.Fatal("unexpected begin outcome")
		}
		if err = store.Complete(key, flight, Record{Reply: reply}); err != nil {
			b.Fatal(err)
		}
	}
}
