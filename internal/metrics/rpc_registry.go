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

// rpcRegistryResultCount is the number of RPCRegistryResult values.
const rpcRegistryResultCount = 6

// RPCRegistryTargetMetrics holds the proxy metric children of one registry target. They are resolved
// once, when the target is built, so a call does no label lookup. A nil value does nothing.
type RPCRegistryTargetMetrics struct {
	duration prometheus.Observer
	summary  prometheus.Observer
	errors   prometheus.Counter
	inflight prometheus.Gauge
}

// NewRPCRegistryTargetMetrics resolves the proxy metric children of target, with the grpc protocol
// and the rpc type. It returns nil when the proxy metrics are not initialised.
func NewRPCRegistryTargetMetrics(target string) *RPCRegistryTargetMetrics {
	if ProxyCallDurationHistogram == nil || ProxyCallDurationSummary == nil ||
		ProxyCallErrorCount == nil || ProxyCallInflightRequests == nil {
		return nil
	}
	return &RPCRegistryTargetMetrics{
		duration: ProxyCallDurationHistogram.WithLabelValues("grpc", "rpc", target),
		summary:  ProxyCallDurationSummary.WithLabelValues("grpc", "rpc", target),
		errors:   ProxyCallErrorCount.WithLabelValues("grpc", "rpc", target),
		inflight: ProxyCallInflightRequests.WithLabelValues("grpc", "rpc", target),
	}
}

// ObserveCall records the duration of one call to the target, and counts it as an error when failed.
func (m *RPCRegistryTargetMetrics) ObserveCall(seconds float64, failed bool) {
	if m == nil {
		return
	}
	m.duration.Observe(seconds)
	m.summary.Observe(seconds)
	if failed {
		m.errors.Inc()
	}
}

// IncInflight counts a call to the target which started.
func (m *RPCRegistryTargetMetrics) IncInflight() {
	if m == nil {
		return
	}
	m.inflight.Inc()
}

// DecInflight counts a call to the target which ended.
func (m *RPCRegistryTargetMetrics) DecInflight() {
	if m == nil {
		return
	}
	m.inflight.Dec()
}

// RPCRegistryMethodMetrics holds the calls_total children of one registry method, one per result.
// A nil value does nothing.
type RPCRegistryMethodMetrics struct {
	calls [rpcRegistryResultCount]prometheus.Counter
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

// rpcRegistryResults lists the RPCRegistryResult values in the order of RPCRegistryMethodMetrics.calls.
var rpcRegistryResults = [rpcRegistryResultCount]string{
	RPCRegistryResultOK,
	RPCRegistryResultError,
	RPCRegistryResultCancelled,
	RPCRegistryResultTimeout,
	RPCRegistryResultDenied,
	RPCRegistryResultUnknown,
}

// rpcRegistryResultIndex returns the index of result in rpcRegistryResults, or the index of
// RPCRegistryResultError for other values.
func rpcRegistryResultIndex(result string) int {
	switch result {
	case RPCRegistryResultOK:
		return 0
	case RPCRegistryResultCancelled:
		return 2
	case RPCRegistryResultTimeout:
		return 3
	case RPCRegistryResultDenied:
		return 4
	case RPCRegistryResultUnknown:
		return 5
	default:
		return 1
	}
}

// rpcRegistryAppTypeSet remembers the app_type label values seen by this process.
type rpcRegistryAppTypeSet struct {
	mu     sync.Mutex
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
	switch result {
	case RPCRegistryResultOK, RPCRegistryResultError, RPCRegistryResultCancelled,
		RPCRegistryResultTimeout, RPCRegistryResultDenied, RPCRegistryResultUnknown:
		return result
	default:
		return RPCRegistryResultError
	}
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
func (s *rpcRegistryAppTypeSet) label(value string) string {
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
