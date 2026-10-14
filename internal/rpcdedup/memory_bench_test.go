package rpcdedup

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkMemoryBeginCompleteParallel is BenchmarkMemoryBeginComplete run by GOMAXPROCS goroutines, over 1024
// scopes, as concurrent connections of a node use the store.
func BenchmarkMemoryBeginCompleteParallel(b *testing.B) {
	store := NewMemory(200000, 120*time.Second, 65536)
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
	store := NewMemory(200000, 120*time.Second, 65536)
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
