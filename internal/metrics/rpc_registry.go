package metrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

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
	calls [len(rpcRegistryResults)]prometheus.Counter
}

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
	return m
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
