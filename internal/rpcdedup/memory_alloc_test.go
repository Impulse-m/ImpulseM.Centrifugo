package rpcdedup

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

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
