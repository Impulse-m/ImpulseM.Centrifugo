package rpcdedup

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// benchMemoryOptions returns the options of the benchmarks: the configured defaults, with the budgets large enough
// that no benchmark reaches them, so the cost measured is the call path.
func benchMemoryOptions() MemoryOptions {
	return MemoryOptions{
		MaxEntries:         200000,
		MaxBytes:           1 << 30,
		MaxRecordsPerScope: 1 << 20,
		ScopeReserve:       4,
		ScopeReserveBytes:  8192,
		MaxScopesPerUser:   8,
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

// BenchmarkMemoryInsertAtBudget is one tracked call on a store which is full: its scopes are over their reserve, so
// each insert evicts the oldest record of its own scope. The eviction is a constant number of list updates, so the
// cost does not grow with the records kept, with one shard or with 64.
func BenchmarkMemoryInsertAtBudget(b *testing.B) {
	for _, shards := range []int{1, 64} {
		b.Run("shards="+strconv.Itoa(shards), func(b *testing.B) {
			options := benchMemoryOptions()
			options.MaxEntries = 4096
			options.Shards = shards
			store := NewMemory(options)
			scopes := make([]string, 64)
			for i := range scopes {
				scopes[i] = testScope("u"+strconv.Itoa(i), "a")
				for msgID := uint64(1); msgID <= 64; msgID++ {
					testBenchAnswer(store, Key{Scope: scopes[i], MsgID: msgID})
				}
			}
			reply := []byte("ok")
			fingerprint := Fingerprint("users.get", []byte("x"))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := Key{Scope: scopes[i%len(scopes)], MsgID: uint64(1000 + i)}
				flight := NewFlightWithFingerprint(fingerprint)
				lookup, err := store.Begin(key, flight)
				if err != nil || lookup.Outcome != OutcomeMiss {
					b.Fatal("unexpected begin outcome")
				}
				if err = store.Complete(key, flight, Record{Reply: reply}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// testBenchAnswer runs a call of key which is kept with an empty reply, for the setup of a benchmark.
func testBenchAnswer(store *Memory, key Key) {
	flight := NewFlight()
	if _, err := store.Begin(key, flight); err != nil {
		panic(err)
	}
	if err := store.Complete(key, flight, Record{}); err != nil {
		panic(err)
	}
}

// TestMemoryAllocsPerCall guards the allocations of a tracked call. On a warm scope a miss which is completed costs
// one allocation, the record. On a new scope it costs two, the record and the scope.
func TestMemoryAllocsPerCall(t *testing.T) {
	const runs = 1000
	fingerprint := Fingerprint("users.get", []byte("x"))
	reply := []byte("ok")

	warm := NewMemory(benchMemoryOptions())
	warmScope := testScope("u1", "warm")
	warmKeys := make([]Key, runs+1)
	warmFlights := make([]*Flight, runs+1)
	for i := range warmKeys {
		warmKeys[i] = Key{Scope: warmScope, MsgID: uint64(i + 1)}
		warmFlights[i] = NewFlightWithFingerprint(fingerprint)
	}
	next := 0
	warmAllocs := testing.AllocsPerRun(runs, func() {
		lookup, err := warm.Begin(warmKeys[next], warmFlights[next])
		if err != nil || lookup.Outcome != OutcomeMiss {
			t.Error("unexpected begin outcome")
			return
		}
		if err = warm.Complete(warmKeys[next], warmFlights[next], Record{Reply: reply}); err != nil {
			t.Error(err)
		}
		next++
	})
	require.LessOrEqual(t, warmAllocs, 1.0)

	fresh := NewMemory(benchMemoryOptions())
	// Scopes are admitted and released before the measurement, so the maps of the shards have grown to their size
	// and the measured scopes do not pay for their growth.
	for i := 0; i < 2*(runs+1); i++ {
		primed := testScope("w"+strconv.Itoa(i), "a")
		testBenchAnswer(fresh, Key{Scope: primed, MsgID: 1})
		require.NoError(t, fresh.Ack(primed, []uint64{1}))
	}
	freshKeys := make([]Key, runs+1)
	freshFlights := make([]*Flight, runs+1)
	for i := range freshKeys {
		freshKeys[i] = Key{Scope: testScope("u"+strconv.Itoa(i), "a"), MsgID: 1}
		freshFlights[i] = NewFlightWithFingerprint(fingerprint)
	}
	next = 0
	freshAllocs := testing.AllocsPerRun(runs, func() {
		lookup, err := fresh.Begin(freshKeys[next], freshFlights[next])
		if err != nil || lookup.Outcome != OutcomeMiss {
			t.Error("unexpected begin outcome")
			return
		}
		if err = fresh.Complete(freshKeys[next], freshFlights[next], Record{Reply: reply}); err != nil {
			t.Error(err)
		}
		next++
	})
	require.LessOrEqual(t, freshAllocs, 2.0)
}
