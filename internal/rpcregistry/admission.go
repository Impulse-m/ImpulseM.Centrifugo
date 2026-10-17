package rpcregistry

import (
	"math"
	runtimemetrics "runtime/metrics"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// Reasons of an admission refusal, the values of the reason label of the refusals counter.
const (
	AdmissionRefusedInflight = metrics.RPCRegistryRefusedInflight
	AdmissionRefusedMemory   = metrics.RPCRegistryRefusedMemory
)

// admissionSampleInterval is how old a memory sample may be before the admission path takes a new one.
const admissionSampleInterval = int64(100 * time.Millisecond)

// admissionUnsampled is the timestamp of a memory sample which was never taken. It is far enough in the past for
// the first admission to take one.
const admissionUnsampled int64 = math.MinInt64 / 2

// memoryTotalMetric and memoryReleasedMetric are the runtime metrics of the resident memory: the quantity GOMEMLIMIT
// is measured against.
const (
	memoryTotalMetric    = "/memory/classes/total:bytes"
	memoryReleasedMetric = "/memory/classes/heap/released:bytes"
)

// Admission decides whether a registry call may run. It holds a node-wide budget of bytes in flight, and a watermark
// of resident memory. The budget is charged before a call runs and released when its reply is returned to the client
// handler, so the reply bytes queued on a connection are not in it. The watermark is
// sampled lazily on the admission path, so no goroutine or timer runs for it. A refused call is answered with the
// overloaded error and the retry signal later. The budget is measured against golang.org/x/sync/semaphore in
// BenchmarkAdmissionSemaphore: the CAS loop is faster and allocates nothing.
type Admission struct {
	maxInflight int64
	watermark   int64
	// inflight is the number of bytes charged and not released. It is changed only by a CAS which keeps it at or
	// under maxInflight, so no caller sees a provisional sum.
	inflight atomic.Int64
	// epoch is the origin of the sample timestamps, which are durations since it.
	epoch time.Time
	// sampledAt is the timestamp of the last sample, changed by a CAS so one caller takes the next sample.
	sampledAt atomic.Int64
	// sample is the last resident memory sample in bytes.
	sample atomic.Int64
	// tripped is true while the watermark refuses calls. It starts refusing at the watermark and stops under 90% of it.
	tripped atomic.Bool
	// sampleFunc returns the resident memory in bytes. It is the runtime reader, unless a test replaces it.
	sampleFunc func() int64
	// counters are the admission metrics, nil when the metrics are not initialised.
	counters *metrics.RPCRegistryAdmissionMetrics
}

// NewAdmission returns the admission of cfg. It returns nil when both the budget and the watermark are off, so the
// admission path of a registry without them costs one nil check.
func NewAdmission(cfg configtypes.RPCRegistry, m *metrics.RPCRegistryAdmissionMetrics) *Admission {
	if cfg.MaxInflightBytes <= 0 && cfg.MemoryWatermarkBytes <= 0 {
		return nil
	}
	a := &Admission{
		maxInflight: cfg.MaxInflightBytes,
		watermark:   cfg.MemoryWatermarkBytes,
		epoch:       time.Now(),
		counters:    m,
	}
	a.sampledAt.Store(admissionUnsampled)
	a.sampleFunc = a.readRuntimeMemory
	return a
}

// Admit reports whether a call of cost bytes may run now. It returns the empty string when it may, and the reason of
// the refusal otherwise. The memory watermark is checked first. A call admitted by the budget must be released with
// Release and the same cost.
func (a *Admission) Admit(cost int64) (reason string) {
	if a.watermark > 0 && a.overWatermark(time.Since(a.epoch).Nanoseconds()) {
		a.counters.RefusedMemory()
		return AdmissionRefusedMemory
	}
	if a.maxInflight > 0 && !a.charge(cost) {
		a.counters.RefusedInflight()
		return AdmissionRefusedInflight
	}
	return ""
}

// Release returns the cost of a call admitted by Admit, when the budget is on.
func (a *Admission) Release(cost int64) {
	if a.maxInflight <= 0 {
		return
	}
	a.inflight.Add(-cost)
	a.counters.AddInflight(-cost)
}

// charge adds cost to the bytes in flight when the sum stays within the budget. It is a CAS loop, so a sum which is
// over the budget is never published, not even for a moment, and no caller is refused by another's provisional add.
func (a *Admission) charge(cost int64) bool {
	for {
		current := a.inflight.Load()
		if current+cost > a.maxInflight {
			return false
		}
		if a.inflight.CompareAndSwap(current, current+cost) {
			a.counters.AddInflight(cost)
			return true
		}
	}
}

// overWatermark reports whether the memory watermark refuses calls at now, given in nanoseconds since the epoch.
// It refreshes the sample when it is older than admissionSampleInterval, and applies the hysteresis: the watermark
// trips at the watermark, and recovers under 90% of it.
func (a *Admission) overWatermark(now int64) bool {
	a.refreshSample(now)
	sample := a.sample.Load()
	was := a.tripped.Load()
	tripped := was
	switch {
	case sample >= a.watermark:
		tripped = true
	case sample < a.watermark/10*9:
		tripped = false
	}
	if tripped != was {
		a.tripped.Store(tripped)
	}
	return tripped
}

// refreshSample takes a new memory sample when the last one is older than admissionSampleInterval. Only the caller
// whose CAS on sampledAt wins takes it, the others use the sample in place.
func (a *Admission) refreshSample(now int64) {
	last := a.sampledAt.Load()
	if now-last < admissionSampleInterval {
		return
	}
	if !a.sampledAt.CompareAndSwap(last, now) {
		return
	}
	a.sample.Store(a.sampleFunc())
}

// readRuntimeMemory returns the resident memory of the Go runtime: the total memory of its classes less the heap
// memory which is released to the OS. The samples are read into a buffer of the call, so two callers never share one.
func (a *Admission) readRuntimeMemory() int64 {
	samples := [2]runtimemetrics.Sample{
		{Name: memoryTotalMetric},
		{Name: memoryReleasedMetric},
	}
	runtimemetrics.Read(samples[:])
	total := samples[0].Value.Uint64()
	released := samples[1].Value.Uint64()
	if released > total {
		return 0
	}
	return int64(total - released)
}
