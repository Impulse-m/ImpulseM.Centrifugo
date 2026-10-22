package metrics

import (
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

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
	RPCRegistryResultOK         = "ok"
	RPCRegistryResultError      = "error"
	RPCRegistryResultCancelled  = "cancelled"
	RPCRegistryResultTimeout    = "timeout"
	RPCRegistryResultDenied     = "denied"
	RPCRegistryResultUnknown    = "unknown"
	RPCRegistryResultOverloaded = "overloaded"
	// RPCRegistryResultExpired is a call refused because its token had expired. It is not in rpcRegistryResults: its
	// series is created at the first refusal, see ExpiredRefusal.
	RPCRegistryResultExpired = "expired"
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

// rpcRegistryDropped holds the names of the methods whose series a reload dropped, until the method is built again.
// A count by name from a call which was running when its series were dropped would create them again for the life of
// the process, so such a count is skipped.
var rpcRegistryDropped = &rpcRegistryDroppedSet{}

// rpcRegistryDroppedMax bounds the set of dropped names. A drop which would exceed it starts the set again, so the
// names dropped before are counted again only by a call which was still running when they were dropped.
const rpcRegistryDroppedMax = 4096

// rpcRegistryDroppedSet is the set of the dropped method names, see rpcRegistryDropped. The set is immutable: a drop or
// a build stores a copy, so a count reads it with one atomic load and takes no lock. A count which loads the set before
// a drop stores its copy can still create the series of that method once, after the drop.
type rpcRegistryDroppedSet struct {
	// mu serialises the stores of new sets.
	mu    sync.Mutex
	names atomic.Pointer[map[string]struct{}]
}

// has reports whether name is dropped.
func (s *rpcRegistryDroppedSet) has(name string) bool {
	names := s.names.Load()
	if names == nil {
		return false
	}
	_, found := (*names)[name]
	return found
}

// count runs inc unless name is dropped.
func (s *rpcRegistryDroppedSet) count(name string, inc func()) {
	if s.has(name) {
		return
	}
	inc()
}

// drop adds name to the set, and starts the set again when it is full.
func (s *rpcRegistryDroppedSet) drop(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]struct{})
	if current := s.names.Load(); current != nil {
		maps.Copy(next, *current)
	}
	if _, found := next[name]; !found && len(next) >= rpcRegistryDroppedMax {
		clear(next)
	}
	next[name] = struct{}{}
	s.names.Store(&next)
}

// remove deletes name from the set, when the method is built again.
func (s *rpcRegistryDroppedSet) remove(name string) {
	if !s.has(name) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]struct{})
	maps.Copy(next, *s.names.Load())
	delete(next, name)
	s.names.Store(&next)
}

// NewRPCRegistryMethodMetrics resolves the calls_total child of rpcMethod for every result. It returns
// nil when the registry metrics are not initialised.
func NewRPCRegistryMethodMetrics(rpcMethod string) *RPCRegistryMethodMetrics {
	rpcRegistryDropped.remove(rpcMethod)
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

// DropRPCRegistryMethodSeries deletes the calls_total, errors_total and retransmits series of a method which
// left the registry, so a removed name does not keep its series for the life of the process. A call of the old
// method which is still running keeps counting on the detached children, which are not exported, and its counts
// by name are skipped until the method is built again.
func DropRPCRegistryMethodSeries(rpcMethod string) {
	rpcRegistryDropped.drop(rpcMethod)
	if RPCRegistryCallsTotal != nil {
		RPCRegistryCallsTotal.DeletePartialMatch(prometheus.Labels{"rpc_method": rpcMethod})
	}
	if RPCRegistryErrorsTotal != nil {
		RPCRegistryErrorsTotal.DeletePartialMatch(prometheus.Labels{"rpc_method": rpcMethod})
	}
	if RPCRegistryRetransmitsTotal != nil {
		RPCRegistryRetransmitsTotal.DeleteLabelValues(rpcMethod)
	}
}

// DropRPCRegistryTargetSeries deletes the proxy metric series of a target which left the registry. It
// deletes the children which NewProxyCallChildren resolves with the grpc protocol and the rpc kind.
func DropRPCRegistryTargetSeries(target string) {
	DeleteProxyCallChildren("grpc", "rpc", target)
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
	RPCRegistryResultOverloaded,
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
	label := rpcRegistryAppTypeLabel(appType)
	rpcRegistryDropped.count(rpcMethod, func() {
		RPCRegistryErrorsTotal.WithLabelValues(rpcMethod, label).Inc()
	})
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

// The values of the reason label of RPCRegistryAdmissionRefusalsTotal: the in-flight budget and the memory watermark.
const (
	RPCRegistryRefusedInflight = "inflight"
	RPCRegistryRefusedMemory   = "memory"
)

// rpcRegistryRefusedReasons are the values of the reason label of RPCRegistryAdmissionRefusalsTotal, which has a series
// for each of them from the start.
var rpcRegistryRefusedReasons = [...]string{
	RPCRegistryRefusedInflight,
	RPCRegistryRefusedMemory,
}

// RPCRegistryInflightBytes is the bytes of registry calls in flight on the node, by the cost of admission. It is a gauge
// func which reads the bound source at scrape time, so a call does not write it. It is set by the fork metrics setup, and
// is nil when the metrics are not initialised.
var RPCRegistryInflightBytes prometheus.Collector

// rpcRegistryInflightSource is the reader of the bytes in flight, bound by the admission which the registry runs.
var rpcRegistryInflightSource boundReader[int64]

// readRPCRegistryInflight returns the bytes in flight of the bound admission, or zero when none is bound.
func readRPCRegistryInflight() float64 {
	return float64(rpcRegistryInflightSource.Read())
}

// RPCRegistryAdmissionRefusalsTotal counts the registry calls which admission refused, by reason. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryAdmissionRefusalsTotal *prometheus.CounterVec

// RPCRegistryAdmissionMetrics holds the admission metrics, resolved once. A nil value does nothing.
type RPCRegistryAdmissionMetrics struct {
	refusedInflight prometheus.Counter
	refusedMemory   prometheus.Counter
}

// NewRPCRegistryAdmissionMetrics resolves the admission metrics. It returns nil when the metrics are not initialised.
func NewRPCRegistryAdmissionMetrics() *RPCRegistryAdmissionMetrics {
	if RPCRegistryAdmissionRefusalsTotal == nil || RPCRegistryInflightBytes == nil {
		return nil
	}
	return &RPCRegistryAdmissionMetrics{
		refusedInflight: RPCRegistryAdmissionRefusalsTotal.WithLabelValues(RPCRegistryRefusedInflight),
		refusedMemory:   RPCRegistryAdmissionRefusalsTotal.WithLabelValues(RPCRegistryRefusedMemory),
	}
}

// BindInflight makes the in-flight gauge read the bytes in flight by read at each scrape. A call does not write the gauge,
// so the admission path costs no shared write for it. The admission of a registry binds its own reader when it is built,
// so the last admission built is the one read.
func (m *RPCRegistryAdmissionMetrics) BindInflight(read func() int64) {
	if m == nil {
		return
	}
	rpcRegistryInflightSource.Bind(read)
}

// RefusedInflight counts one call which the in-flight budget refused.
func (m *RPCRegistryAdmissionMetrics) RefusedInflight() {
	if m == nil {
		return
	}
	m.refusedInflight.Inc()
}

// RefusedMemory counts one call which the memory watermark refused.
func (m *RPCRegistryAdmissionMetrics) RefusedMemory() {
	if m == nil {
		return
	}
	m.refusedMemory.Inc()
}

// RPCRegistryDedupWaitSeconds is the time duplicates wait for the call they joined. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupWaitSeconds prometheus.Histogram

// RPCRegistryDedupEntries is the number of answers the dedup store keeps, of both pools. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupEntries prometheus.Gauge

// RPCRegistryDedupEvictions counts the answers which the dedup store evicts to make room, by reason. It is set by
// the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupEvictions *prometheus.CounterVec

// RPCRegistryDedupScopes is the number of scopes the dedup store admits, per pool. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupScopes *prometheus.GaugeVec

// RPCRegistryDedupBytes is the number of bytes the dedup store keeps, per pool. It is set by the fork metrics
// setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupBytes *prometheus.GaugeVec

// RPCRegistryDedupRedisErrorsTotal counts the failed Redis operations of the shared tier of exactly-once RPC. It is
// set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRedisErrorsTotal prometheus.Counter

// RPCRegistryDedupRedisDuration is the duration of the Redis operations of the shared tier. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRedisDuration prometheus.Histogram

// RPCRegistryDedupRedisAckDropsTotal counts the acks which the shared tier dropped because its ack queue was full.
// It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRedisAckDropsTotal prometheus.Counter

// RPCRegistryDedupCancelDropsTotal counts the cancel notifications to other nodes which the shared tier dropped because
// its notification queue was full. It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupCancelDropsTotal prometheus.Counter

// RPCRegistryDedupRemoteReadsTotal counts the reads of the shared tier which the waits for calls on other nodes make.
// It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRemoteReadsTotal prometheus.Counter

// RPCRegistryDedupRemoteWaitsTotal counts the waits for calls on other nodes, by how they ended. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRemoteWaitsTotal *prometheus.CounterVec

// RPCRegistryDedupNotificationsTotal counts the node notifications of exactly-once RPC which this node sends, by op and
// result. It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupNotificationsTotal *prometheus.CounterVec

// RPCRegistryDedupWakesTotal counts the wakes which this node writes for the waiters of its calls, by result. It is set
// by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupWakesTotal *prometheus.CounterVec

// RPCRegistryDedupWakeWatchers is the number of wake watchers of this node which read the wake streams. It is set by
// the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupWakeWatchers prometheus.Gauge

// The values of the op label of RPCRegistryDedupNotificationsTotal, one per notification op.
const (
	RPCRegistryDedupNotifyCancel = "cancel"
)

// The values of the result label of RPCRegistryDedupWakesTotal.
const (
	rpcRegistryDedupWakeOK    = "ok"
	rpcRegistryDedupWakeError = "error"
)

// rpcRegistryDedupWaitEnds are the values of the end label of RPCRegistryDedupRemoteWaitsTotal, which has a series for
// each of them from the start.
var rpcRegistryDedupWaitEnds = [...]string{
	rpcdedup.WatchEndWoken,
	rpcdedup.WatchEndChecked,
	rpcdedup.WatchEndPolled,
	rpcdedup.WatchEndDeadline,
	rpcdedup.WatchEndRefused,
}

// rpcRegistryDedupNotifyOps are the values of the op label of RPCRegistryDedupNotificationsTotal.
var rpcRegistryDedupNotifyOps = [...]string{
	RPCRegistryDedupNotifyCancel,
}

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
	RPCRegistryDedupRefusedPool
	RPCRegistryDedupRefusedUser
	RPCRegistryDedupHitInFlightRemote
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
	"refused_pool",
	"refused_user",
	"hit_inflight_remote",
}

// rpcRegistryDedupPoolLabels are the pool labels of the dedup gauges, the index of each is the pool index of the store.
var rpcRegistryDedupPoolLabels = [...]string{
	"authenticated",
	"anonymous",
}

// rpcRegistryDedupEvictionReasons are the reasons of the eviction counter, which has a series for each of them from
// the start.
var rpcRegistryDedupEvictionReasons = [...]string{
	rpcdedup.EvictionScopeCap,
	rpcdedup.EvictionOverReserve,
	rpcdedup.EvictionSurplus,
	rpcdedup.EvictionUncached,
	rpcdedup.EvictionReplySize,
}

// RPCRegistryDedupMetrics holds the outcome counters of exactly-once RPC, resolved once. A nil value does
// nothing.
type RPCRegistryDedupMetrics struct {
	outcomes         [len(rpcRegistryDedupOutcomeLabels)]prometheus.Counter
	evictScopeCap    prometheus.Counter
	evictOverReserve prometheus.Counter
	evictSurplus     prometheus.Counter
	evictUncached    prometheus.Counter
	evictReplySize   prometheus.Counter
	waitSeconds      prometheus.Histogram
	entries          prometheus.Gauge
	scopes           [len(rpcRegistryDedupPoolLabels)]prometheus.Gauge
	bytes            [len(rpcRegistryDedupPoolLabels)]prometheus.Gauge
	redisErrors      prometheus.Counter
	redisDuration    prometheus.Histogram
	ackDrops         prometheus.Counter
	cancelDrops      prometheus.Counter
	remoteReads      prometheus.Counter
	waitWoken        prometheus.Counter
	waitChecked      prometheus.Counter
	waitPolled       prometheus.Counter
	waitDeadline     prometheus.Counter
	waitRefused      prometheus.Counter
	notifyCancelOK   prometheus.Counter
	notifyCancelErr  prometheus.Counter
	wakesOK          prometheus.Counter
	wakesErr         prometheus.Counter
	wakeWatchers     prometheus.Gauge
}

// NewRPCRegistryDedupMetrics resolves the outcome counters. It returns nil when the metrics are not
// initialised.
func NewRPCRegistryDedupMetrics() *RPCRegistryDedupMetrics {
	if RPCRegistryDedupTotal == nil {
		return nil
	}
	m := &RPCRegistryDedupMetrics{
		waitSeconds:   RPCRegistryDedupWaitSeconds,
		entries:       RPCRegistryDedupEntries,
		redisErrors:   RPCRegistryDedupRedisErrorsTotal,
		redisDuration: RPCRegistryDedupRedisDuration,
		ackDrops:      RPCRegistryDedupRedisAckDropsTotal,
		cancelDrops:   RPCRegistryDedupCancelDropsTotal,
	}
	for i, label := range rpcRegistryDedupOutcomeLabels {
		m.outcomes[i] = RPCRegistryDedupTotal.WithLabelValues(label)
	}
	if RPCRegistryDedupEvictions != nil {
		m.evictScopeCap = RPCRegistryDedupEvictions.WithLabelValues(rpcdedup.EvictionScopeCap)
		m.evictOverReserve = RPCRegistryDedupEvictions.WithLabelValues(rpcdedup.EvictionOverReserve)
		m.evictSurplus = RPCRegistryDedupEvictions.WithLabelValues(rpcdedup.EvictionSurplus)
		m.evictUncached = RPCRegistryDedupEvictions.WithLabelValues(rpcdedup.EvictionUncached)
		m.evictReplySize = RPCRegistryDedupEvictions.WithLabelValues(rpcdedup.EvictionReplySize)
	}
	if RPCRegistryDedupScopes != nil && RPCRegistryDedupBytes != nil {
		for i, label := range rpcRegistryDedupPoolLabels {
			m.scopes[i] = RPCRegistryDedupScopes.WithLabelValues(label)
			m.bytes[i] = RPCRegistryDedupBytes.WithLabelValues(label)
		}
	}
	m.remoteReads = RPCRegistryDedupRemoteReadsTotal
	if RPCRegistryDedupRemoteWaitsTotal != nil {
		m.waitWoken = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndWoken)
		m.waitChecked = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndChecked)
		m.waitPolled = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndPolled)
		m.waitDeadline = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndDeadline)
		m.waitRefused = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndRefused)
	}
	if RPCRegistryDedupNotificationsTotal != nil {
		m.notifyCancelOK = RPCRegistryDedupNotificationsTotal.WithLabelValues(RPCRegistryDedupNotifyCancel, "ok")
		m.notifyCancelErr = RPCRegistryDedupNotificationsTotal.WithLabelValues(RPCRegistryDedupNotifyCancel, "error")
	}
	if RPCRegistryDedupWakesTotal != nil {
		m.wakesOK = RPCRegistryDedupWakesTotal.WithLabelValues(rpcRegistryDedupWakeOK)
		m.wakesErr = RPCRegistryDedupWakesTotal.WithLabelValues(rpcRegistryDedupWakeError)
	}
	m.wakeWatchers = RPCRegistryDedupWakeWatchers
	return m
}

// WatchRead counts a read of the shared tier which a wait for a call on another node makes. It implements
// rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) WatchRead() {
	if m == nil {
		return
	}
	incCounter(m.remoteReads)
}

// WatchEnded counts a wait for a call on another node by how it ended. It implements rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) WatchEnded(end string) {
	if m == nil {
		return
	}
	switch end {
	case rpcdedup.WatchEndWoken:
		incCounter(m.waitWoken)
	case rpcdedup.WatchEndChecked:
		incCounter(m.waitChecked)
	case rpcdedup.WatchEndPolled:
		incCounter(m.waitPolled)
	case rpcdedup.WatchEndDeadline:
		incCounter(m.waitDeadline)
	case rpcdedup.WatchEndRefused:
		incCounter(m.waitRefused)
	}
}

// Woke counts the wakes which this node wrote for a completed call, the written and the failed ones. It implements
// rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) Woke(written int, failed int) {
	if m == nil {
		return
	}
	addCounter(m.wakesOK, written)
	addCounter(m.wakesErr, failed)
}

// WakeWatching counts a wake watcher which starts reading the wake streams (+1) or stops (-1). It implements
// rpcdedup.BackendObserver.
func (m *RPCRegistryDedupMetrics) WakeWatching(delta int) {
	if m == nil || m.wakeWatchers == nil {
		return
	}
	m.wakeWatchers.Add(float64(delta))
}

// Notified counts a node notification of exactly-once RPC which this node sent, by its op, one of the
// RPCRegistryDedupNotify values, and whether it was sent.
func (m *RPCRegistryDedupMetrics) Notified(op string, ok bool) {
	if m == nil {
		return
	}
	switch {
	case op == RPCRegistryDedupNotifyCancel && ok:
		incCounter(m.notifyCancelOK)
	case op == RPCRegistryDedupNotifyCancel:
		incCounter(m.notifyCancelErr)
	}
}

// addCounter adds n to c when it is set.
func addCounter(c prometheus.Counter, n int) {
	if c != nil && n > 0 {
		c.Add(float64(n))
	}
}

// Evicted counts an answer which the dedup store evicted for reason. It implements rpcdedup.EvictionObserver.
func (m *RPCRegistryDedupMetrics) Evicted(reason string) {
	if m == nil {
		return
	}
	switch reason {
	case rpcdedup.EvictionScopeCap:
		incCounter(m.evictScopeCap)
	case rpcdedup.EvictionOverReserve:
		incCounter(m.evictOverReserve)
	case rpcdedup.EvictionSurplus:
		incCounter(m.evictSurplus)
	case rpcdedup.EvictionUncached:
		incCounter(m.evictUncached)
	case rpcdedup.EvictionReplySize:
		incCounter(m.evictReplySize)
	}
}

// incCounter increments c when it is set.
func incCounter(c prometheus.Counter) {
	if c != nil {
		c.Inc()
	}
}

// Inc counts one outcome.
func (m *RPCRegistryDedupMetrics) Inc(outcome RPCRegistryDedupOutcome) {
	if m == nil {
		return
	}
	m.outcomes[outcome].Inc()
}

// RedisCall observes the duration of one Redis operation of the shared tier. It implements rpcdedup.BackendObserver.
func (m *RPCRegistryDedupMetrics) RedisCall(seconds float64) {
	if m == nil || m.redisDuration == nil {
		return
	}
	m.redisDuration.Observe(seconds)
}

// RedisError counts one failed Redis operation of the shared tier. It implements rpcdedup.BackendObserver.
func (m *RPCRegistryDedupMetrics) RedisError() {
	if m == nil {
		return
	}
	incCounter(m.redisErrors)
}

// AckDropped counts one ack which the shared tier dropped because its queue was full. It implements
// rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) AckDropped() {
	if m == nil {
		return
	}
	incCounter(m.ackDrops)
}

// CancelDropped counts one cancel notification which the shared tier dropped because its queue was full. It implements
// rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) CancelDropped() {
	if m == nil {
		return
	}
	incCounter(m.cancelDrops)
}

// LostOwnership counts one call whose answer the shared tier does not hold for this node. It is counted as a Redis
// error, and implements rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) LostOwnership() {
	if m == nil {
		return
	}
	incCounter(m.redisErrors)
}

// ObserveWait records the time a duplicate waited for the call it joined.
func (m *RPCRegistryDedupMetrics) ObserveWait(seconds float64) {
	if m == nil || m.waitSeconds == nil {
		return
	}
	m.waitSeconds.Observe(seconds)
}

// SetStore sets the gauges of the store: its answers, and per pool its admitted scopes and its bytes. The gauges are
// read from the store without its locks, so this does not wait for the calls.
func (m *RPCRegistryDedupMetrics) SetStore(stats rpcdedup.Stats) {
	if m == nil {
		return
	}
	if m.entries != nil {
		m.entries.Set(float64(stats.Entries))
	}
	pools := [len(rpcRegistryDedupPoolLabels)]rpcdedup.PoolStats{stats.Authenticated, stats.Anonymous}
	for i, pool := range pools {
		if m.scopes[i] != nil {
			m.scopes[i].Set(float64(pool.Scopes))
		}
		if m.bytes[i] != nil {
			m.bytes[i].Set(float64(pool.Bytes))
		}
	}
}
