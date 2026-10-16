package rpcregistry

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// testBenchBudget is a budget far above any benchmark cost, so the benchmarks measure the admit path without refusals.
const testBenchBudget int64 = 1 << 40

// testAdmission returns the admission of a budget and a watermark, with a sampler which the test sets.
func testAdmission(t *testing.T, maxInflight int64, watermark int64) *Admission {
	t.Helper()
	a := NewAdmission(configtypes.RPCRegistry{
		MaxInflightBytes:     maxInflight,
		MemoryWatermarkBytes: watermark,
	}, nil)
	require.NotNil(t, a)
	return a
}

func TestNewAdmissionOffIsNil(t *testing.T) {
	require.Nil(t, NewAdmission(configtypes.RPCRegistry{}, nil))
}

func TestAdmissionBudgetAdmitsUpToTheLimit(t *testing.T) {
	a := testAdmission(t, 10, 0)
	require.Empty(t, a.Admit(4))
	require.Empty(t, a.Admit(6))
	require.Equal(t, AdmissionRefusedInflight, a.Admit(1))
	a.Release(6)
	require.Empty(t, a.Admit(6), "release restores the budget")
}

func TestAdmissionBudgetRefusesCostAboveTheLimit(t *testing.T) {
	a := testAdmission(t, 10, 0)
	require.Equal(t, AdmissionRefusedInflight, a.Admit(11))
	require.Equal(t, int64(0), a.inflight.Load(), "a refused call charges nothing")
}

func TestAdmissionBudgetNoSpuriousRefusal(t *testing.T) {
	// With max 10 and 8 in flight, a call of 2 always fits. An add-then-undo budget would let the provisional add of a
	// call of 5 make it look refused, so this checks that no call which fits is refused by a racing one.
	a := testAdmission(t, 10, 0)
	for round := 0; round < 1000; round++ {
		require.Empty(t, a.Admit(8))
		var wg sync.WaitGroup
		var smallAdmitted atomic.Bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			if reason := a.Admit(5); reason == "" {
				a.Release(5)
			}
		}()
		go func() {
			defer wg.Done()
			smallAdmitted.Store(a.Admit(2) == "")
		}()
		wg.Wait()
		require.True(t, smallAdmitted.Load(), "round %d: a call of 2 fits in the budget beside 8", round)
		a.Release(2)
		a.Release(8)
	}
}

func TestAdmissionBudgetConcurrentNeverOverTheLimit(t *testing.T) {
	const limit int64 = 1 << 20
	a := testAdmission(t, limit, 0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if reason := a.Admit(1 << 16); reason != "" {
					continue
				}
				if inflight := a.inflight.Load(); inflight > limit || inflight < 0 {
					t.Errorf("inflight %d outside [0, %d]", inflight, limit)
				}
				a.Release(1 << 16)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), a.inflight.Load())
}

func TestAdmissionWatermarkTripsAndRecovers(t *testing.T) {
	a := testAdmission(t, 0, 1000)
	var sample atomic.Int64
	a.sampleFunc = sample.Load
	var now int64
	step := func(value int64) bool {
		sample.Store(value)
		now += int64(100 * time.Millisecond)
		return a.overWatermark(now)
	}
	require.False(t, step(500), "under the watermark admits")
	require.True(t, step(1000), "at the watermark refuses")
	require.True(t, step(950), "between 90% and the watermark keeps refusing")
	require.False(t, step(899), "under 90% of the watermark admits again")
	require.False(t, step(950), "between 90% and the watermark keeps admitting")
}

func TestAdmissionWatermarkSampleIsNotReadWithin100ms(t *testing.T) {
	a := testAdmission(t, 0, 1000)
	var reads atomic.Int64
	a.sampleFunc = func() int64 {
		reads.Add(1)
		return 0
	}
	base := int64(time.Second)
	a.overWatermark(base)
	a.overWatermark(base + int64(50*time.Millisecond))
	a.overWatermark(base + int64(99*time.Millisecond))
	require.Equal(t, int64(1), reads.Load())
	a.overWatermark(base + int64(100*time.Millisecond))
	require.Equal(t, int64(2), reads.Load())
}

func TestAdmissionWatermarkRefusesWithMemoryReason(t *testing.T) {
	a := testAdmission(t, 1<<20, 1000)
	a.sampleFunc = func() int64 { return 2000 }
	require.Equal(t, AdmissionRefusedMemory, a.Admit(1))
	require.Equal(t, int64(0), a.inflight.Load(), "a call refused by memory charges no budget")
}

func TestRegistryAdmissionReadAtStart(t *testing.T) {
	var nilRegistry *Registry
	require.Nil(t, nilRegistry.Admission())

	off := New()
	apply, err := off.Prepare(testRegistry())
	require.NoError(t, err)
	apply()
	require.Nil(t, off.Admission(), "both keys off: no admission")

	on := New()
	reg := testRegistry()
	reg.MaxInflightBytes = 1 << 20
	apply, err = on.Prepare(reg)
	require.NoError(t, err)
	apply()
	first := on.Admission()
	require.NotNil(t, first)

	reg.MaxInflightBytes = 0
	apply, err = on.Prepare(reg)
	require.NoError(t, err)
	apply()
	require.Same(t, first, on.Admission(), "a later prepare does not rebuild the admission")
}

func TestAdmissionReadRuntimeMemory(t *testing.T) {
	a := testAdmission(t, 0, 1<<40)
	value := a.readRuntimeMemory()
	require.Greater(t, value, int64(0))
}

func BenchmarkAdmission(b *testing.B) {
	a := NewAdmission(configtypes.RPCRegistry{
		MaxInflightBytes:     testBenchBudget,
		MemoryWatermarkBytes: testBenchBudget,
	}, nil)
	a.sampleFunc = func() int64 { return 0 }
	const cost int64 = 1 << 20
	b.ReportAllocs()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if reason := a.Admit(cost); reason != "" {
				b.Error("unexpected refusal")
			}
			a.Release(cost)
		}
	})
}

// BenchmarkAdmissionSemaphore is BenchmarkAdmission over golang.org/x/sync/semaphore, the reference the CAS loop is
// compared with: the same budget and cost, with a mutex per call.
func BenchmarkAdmissionSemaphore(b *testing.B) {
	sem := semaphore.NewWeighted(testBenchBudget)
	const cost int64 = 1 << 20
	b.ReportAllocs()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !sem.TryAcquire(cost) {
				b.Error("unexpected refusal")
			}
			sem.Release(cost)
		}
	})
}
