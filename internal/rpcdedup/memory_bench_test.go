package rpcdedup

import (
	"testing"
	"time"
)

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
