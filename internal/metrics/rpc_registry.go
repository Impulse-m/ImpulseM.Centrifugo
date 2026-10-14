package metrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// RPCRegistryReloadsTotal counts the reloads of the RPC method registry. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var RPCRegistryReloadsTotal *prometheus.CounterVec

// rpcRegistryMaxAppTypes caps the distinct app_type label values per process. Values beyond the cap
// are reported as rpcRegistryOtherAppType.
const rpcRegistryMaxAppTypes = 256

// rpcRegistryOtherAppType is the app_type label of error types beyond rpcRegistryMaxAppTypes.
const rpcRegistryOtherAppType = "other"

// rpcRegistryNoAppType is the app_type label of errors without an application error type.
const rpcRegistryNoAppType = "none"

// Results of a RPC method registry call, the values of the result label.
const (
	RPCRegistryResultOK        = "ok"
	RPCRegistryResultError     = "error"
	RPCRegistryResultCancelled = "cancelled"
	RPCRegistryResultTimeout   = "timeout"
	RPCRegistryResultDenied    = "denied"
	RPCRegistryResultUnknown   = "unknown"
)

// RPCRegistryMethodMetrics holds the calls_total children of one registry method, one per result.
// They are resolved once, when the method is built, so a call does no label lookup. A nil value does nothing.
type RPCRegistryMethodMetrics struct {
	calls       [len(rpcRegistryResults)]prometheus.Counter
	retransmits prometheus.Counter
}

// RPCRegistryRetransmitsTotal counts the calls of a registry method which the client retransmitted after a
// transport loss. It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryRetransmitsTotal *prometheus.CounterVec

// NewRPCRegistryMethodMetrics resolves the calls_total child of rpcMethod for every result. It returns
// nil when the registry metrics are not initialised.
func NewRPCRegistryMethodMetrics(rpcMethod string) *RPCRegistryMethodMetrics {
	if RPCRegistryCallsTotal == nil {
		return nil
	}
	m := &RPCRegistryMethodMetrics{}
	for i, result := range rpcRegistryResults {
		m.calls[i] = RPCRegistryCallsTotal.WithLabelValues(rpcMethod, result)
	}
	if RPCRegistryRetransmitsTotal != nil {
		m.retransmits = RPCRegistryRetransmitsTotal.WithLabelValues(rpcMethod)
	}
	return m
}

// IncRetransmit counts one retransmitted call of the method.
func (m *RPCRegistryMethodMetrics) IncRetransmit() {
	if m == nil || m.retransmits == nil {
		return
	}
	m.retransmits.Inc()
}

// IncCall counts one call with its result. Results other than the RPCRegistryResult values are
// counted as RPCRegistryResultError, as IncRPCRegistryCall does.
func (m *RPCRegistryMethodMetrics) IncCall(result string) {
	if m == nil {
		return
	}
	m.calls[rpcRegistryResultIndex(result)].Inc()
}

// rpcRegistryResults is the only list of the RPCRegistryResult values. The calls_total children of a
// method are kept in this order.
var rpcRegistryResults = [...]string{
	RPCRegistryResultOK,
	RPCRegistryResultError,
	RPCRegistryResultCancelled,
	RPCRegistryResultTimeout,
	RPCRegistryResultDenied,
	RPCRegistryResultUnknown,
}

// Triggers of a RPC method registry reload, the values of the trigger label.
const (
	RPCRegistryReloadTriggerSignal = "signal"
	RPCRegistryReloadTriggerPoll   = "poll"
)

// RPCRegistryReloadMetrics holds the reloads_total children of one trigger, one for each result. They are
// resolved once, when the registry is created. A nil value does nothing.
type RPCRegistryReloadMetrics struct {
	ok    prometheus.Counter
	error prometheus.Counter
}

// NewRPCRegistryReloadMetrics resolves the reloads_total children of trigger. It returns nil when the
// registry metrics are not initialised.
func NewRPCRegistryReloadMetrics(trigger string) *RPCRegistryReloadMetrics {
	if RPCRegistryReloadsTotal == nil {
		return nil
	}
	return &RPCRegistryReloadMetrics{
		ok:    RPCRegistryReloadsTotal.WithLabelValues(trigger, RPCRegistryResultOK),
		error: RPCRegistryReloadsTotal.WithLabelValues(trigger, RPCRegistryResultError),
	}
}

// Inc counts one reload which succeeded when ok is true, and one which failed otherwise.
func (m *RPCRegistryReloadMetrics) Inc(ok bool) {
	if m == nil {
		return
	}
	if ok {
		m.ok.Inc()
		return
	}
	m.error.Inc()
}

// rpcRegistryResultErrorIndex is the index of RPCRegistryResultError in rpcRegistryResults, the
// result of values which are not a result.
const rpcRegistryResultErrorIndex = 1

// rpcRegistryResultIndex returns the index of result in rpcRegistryResults, or rpcRegistryResultErrorIndex
// for other values.
func rpcRegistryResultIndex(result string) int {
	for i, known := range rpcRegistryResults {
		if known == result {
			return i
		}
	}
	return rpcRegistryResultErrorIndex
}

// rpcRegistryAppTypeSet remembers the app_type label values seen by this process.
type rpcRegistryAppTypeSet struct {
	mu     sync.RWMutex
	values map[string]struct{}
}

var rpcRegistryAppTypes = &rpcRegistryAppTypeSet{
	values: map[string]struct{}{},
}

// IncRPCRegistryCall counts one RPC method registry call with its result. Results other than the
// RPCRegistryResult values are counted as RPCRegistryResultError. rpcMethod must be a name from the
// registry, or "unknown" for names which are not in it.
func IncRPCRegistryCall(rpcMethod string, result string) {
	if RPCRegistryCallsTotal == nil {
		return
	}
	RPCRegistryCallsTotal.WithLabelValues(rpcMethod, rpcRegistryResultLabel(result)).Inc()
}

// IncRPCRegistryError counts one failed RPC method registry call with its application error type.
// The type is normalised by rpcRegistryAppTypeLabel.
func IncRPCRegistryError(rpcMethod string, appType string) {
	if RPCRegistryErrorsTotal == nil {
		return
	}
	RPCRegistryErrorsTotal.WithLabelValues(rpcMethod, rpcRegistryAppTypeLabel(appType)).Inc()
}

// rpcRegistryResultLabel returns result when it is a known result, and RPCRegistryResultError otherwise.
func rpcRegistryResultLabel(result string) string {
	return rpcRegistryResults[rpcRegistryResultIndex(result)]
}

// rpcRegistryAppTypeLabel returns the bounded app_type label. A trailing _<digits> becomes _X, so
// QUOTA_EXCEEDED_17 and QUOTA_EXCEEDED_30 share one series. Distinct values are kept up to
// rpcRegistryMaxAppTypes per process, the rest are reported as rpcRegistryOtherAppType.
func rpcRegistryAppTypeLabel(appType string) string {
	if appType == "" {
		return rpcRegistryNoAppType
	}
	return rpcRegistryAppTypes.label(normalizeNumericSuffix(appType))
}

// normalizeNumericSuffix replaces digits after the last underscore with X. A value without such
// a suffix is returned unchanged.
func normalizeNumericSuffix(value string) string {
	index := strings.LastIndexByte(value, '_')
	if index < 0 || index == len(value)-1 {
		return value
	}
	for i := index + 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return value
		}
	}
	return value[:index] + "_X"
}

// label returns value when it is known or still fits under the cap, and rpcRegistryOtherAppType otherwise.
// A known value is found under the read lock, so concurrent errors do not queue behind each other.
func (s *rpcRegistryAppTypeSet) label(value string) string {
	s.mu.RLock()
	_, known := s.values[value]
	s.mu.RUnlock()
	if known {
		return value
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[value]; ok {
		return value
	}
	if len(s.values) >= rpcRegistryMaxAppTypes {
		return rpcRegistryOtherAppType
	}
	s.values[value] = struct{}{}
	return value
}

// RPCRegistryDedupTotal counts the calls of exactly-once RPC by outcome. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupTotal *prometheus.CounterVec

// RPCRegistryDedupWaitSeconds is the time duplicates wait for the call they joined. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupWaitSeconds prometheus.Histogram

// RPCRegistryDedupEntries is the number of answers the dedup store keeps. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupEntries prometheus.Gauge

// RPCRegistryDedupOutcome is an outcome of a call of exactly-once RPC, the value of the outcome label.
type RPCRegistryDedupOutcome int

// Outcomes of exactly-once RPC calls. Each is counted once per tracked call, or once per cancel command.
const (
	RPCRegistryDedupMiss RPCRegistryDedupOutcome = iota
	RPCRegistryDedupHitDone
	RPCRegistryDedupHitInFlightLocal
	RPCRegistryDedupEvicted
	RPCRegistryDedupCancelled
	RPCRegistryDedupStoreError
	RPCRegistryDedupMsgIDInvalid
	RPCRegistryDedupMsgIDReused
	RPCRegistryDedupNoScope
)

// rpcRegistryDedupOutcomeLabels holds the outcome label of each RPCRegistryDedupOutcome, in the same order.
var rpcRegistryDedupOutcomeLabels = [...]string{
	"miss",
	"hit_done",
	"hit_inflight_local",
	"evicted",
	"cancelled",
	"store_error",
	"msg_id_invalid",
	"msg_id_reused",
	"no_scope",
}

// RPCRegistryDedupMetrics holds the outcome counters of exactly-once RPC, resolved once. A nil value does
// nothing.
type RPCRegistryDedupMetrics struct {
	outcomes    [len(rpcRegistryDedupOutcomeLabels)]prometheus.Counter
	waitSeconds prometheus.Histogram
	entries     prometheus.Gauge
}

// NewRPCRegistryDedupMetrics resolves the outcome counters. It returns nil when the metrics are not
// initialised.
func NewRPCRegistryDedupMetrics() *RPCRegistryDedupMetrics {
	if RPCRegistryDedupTotal == nil {
		return nil
	}
	var m *RPCRegistryDedupMetrics = &RPCRegistryDedupMetrics{
		waitSeconds: RPCRegistryDedupWaitSeconds,
		entries:     RPCRegistryDedupEntries,
	}
	for i, label := range rpcRegistryDedupOutcomeLabels {
		m.outcomes[i] = RPCRegistryDedupTotal.WithLabelValues(label)
	}
	return m
}

// Inc counts one outcome.
func (m *RPCRegistryDedupMetrics) Inc(outcome RPCRegistryDedupOutcome) {
	if m == nil {
		return
	}
	m.outcomes[outcome].Inc()
}

// ObserveWait records the time a duplicate waited for the call it joined.
func (m *RPCRegistryDedupMetrics) ObserveWait(seconds float64) {
	if m == nil || m.waitSeconds == nil {
		return
	}
	m.waitSeconds.Observe(seconds)
}

// SetEntries sets the number of answers the store keeps.
func (m *RPCRegistryDedupMetrics) SetEntries(n int) {
	if m == nil || m.entries == nil {
		return
	}
	m.entries.Set(float64(n))
}
