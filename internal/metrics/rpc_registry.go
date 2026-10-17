package metrics

import (
	"strings"
	"sync"

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

// RPCRegistryInflightBytes is the bytes of registry calls in flight on the node, by the cost of admission. It is changed by
// the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryInflightBytes prometheus.Gauge

// RPCRegistryAdmissionRefusalsTotal counts the registry calls which admission refused, by reason. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryAdmissionRefusalsTotal *prometheus.CounterVec

// RPCRegistryAdmissionMetrics holds the admission metrics, resolved once. A nil value does nothing.
type RPCRegistryAdmissionMetrics struct {
	inflightBytes   prometheus.Gauge
	refusedInflight prometheus.Counter
	refusedMemory   prometheus.Counter
}

// NewRPCRegistryAdmissionMetrics resolves the admission metrics. It returns nil when the metrics are not initialised.
func NewRPCRegistryAdmissionMetrics() *RPCRegistryAdmissionMetrics {
	if RPCRegistryAdmissionRefusalsTotal == nil || RPCRegistryInflightBytes == nil {
		return nil
	}
	return &RPCRegistryAdmissionMetrics{
		inflightBytes:   RPCRegistryInflightBytes,
		refusedInflight: RPCRegistryAdmissionRefusalsTotal.WithLabelValues(RPCRegistryRefusedInflight),
		refusedMemory:   RPCRegistryAdmissionRefusalsTotal.WithLabelValues(RPCRegistryRefusedMemory),
	}
}

// AddInflight changes the bytes of calls in flight by delta. A delta is order-independent, so concurrent charges and
// releases leave the gauge at the sum of the calls in flight.
func (m *RPCRegistryAdmissionMetrics) AddInflight(delta int64) {
	if m == nil {
		return
	}
	m.inflightBytes.Add(float64(delta))
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

// RPCRegistryDedupRemotePollsTotal counts the polls of the shared tier which the waits for calls on other nodes make.
// It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRemotePollsTotal prometheus.Counter

// RPCRegistryDedupRemoteWaitsTotal counts the waits for calls on other nodes, by how they ended. It is set by the fork
// metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupRemoteWaitsTotal *prometheus.CounterVec

// RPCRegistryDedupNotificationsTotal counts the node notifications of exactly-once RPC which this node sends, by op and
// result. It is set by the fork metrics setup, and is nil when the metrics are not initialised.
var RPCRegistryDedupNotificationsTotal *prometheus.CounterVec

// The values of the op label of RPCRegistryDedupNotificationsTotal, one per notification op.
const (
	RPCRegistryDedupNotifyCancel = "cancel"
	RPCRegistryDedupNotifyDone   = "done"
)

// rpcRegistryDedupWaitEnds are the values of the end label of RPCRegistryDedupRemoteWaitsTotal, which has a series for
// each of them from the start.
var rpcRegistryDedupWaitEnds = [...]string{
	rpcdedup.WatchEndNotified,
	rpcdedup.WatchEndPolled,
	rpcdedup.WatchEndDeadline,
}

// rpcRegistryDedupNotifyOps are the values of the op label of RPCRegistryDedupNotificationsTotal.
var rpcRegistryDedupNotifyOps = [...]string{
	RPCRegistryDedupNotifyCancel,
	RPCRegistryDedupNotifyDone,
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
	remotePolls      prometheus.Counter
	waitNotified     prometheus.Counter
	waitPolled       prometheus.Counter
	waitDeadline     prometheus.Counter
	notifyCancelOK   prometheus.Counter
	notifyCancelErr  prometheus.Counter
	notifyDoneOK     prometheus.Counter
	notifyDoneErr    prometheus.Counter
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
	m.remotePolls = RPCRegistryDedupRemotePollsTotal
	if RPCRegistryDedupRemoteWaitsTotal != nil {
		m.waitNotified = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndNotified)
		m.waitPolled = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndPolled)
		m.waitDeadline = RPCRegistryDedupRemoteWaitsTotal.WithLabelValues(rpcdedup.WatchEndDeadline)
	}
	if RPCRegistryDedupNotificationsTotal != nil {
		m.notifyCancelOK = RPCRegistryDedupNotificationsTotal.WithLabelValues(RPCRegistryDedupNotifyCancel, "ok")
		m.notifyCancelErr = RPCRegistryDedupNotificationsTotal.WithLabelValues(RPCRegistryDedupNotifyCancel, "error")
		m.notifyDoneOK = RPCRegistryDedupNotificationsTotal.WithLabelValues(RPCRegistryDedupNotifyDone, "ok")
		m.notifyDoneErr = RPCRegistryDedupNotificationsTotal.WithLabelValues(RPCRegistryDedupNotifyDone, "error")
	}
	return m
}

// WatchPolled counts a poll of the shared tier which a wait for a call on another node makes. It implements
// rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) WatchPolled() {
	if m == nil {
		return
	}
	incCounter(m.remotePolls)
}

// WatchEnded counts a wait for a call on another node by how it ended. It implements rpcdedup.SharedObserver.
func (m *RPCRegistryDedupMetrics) WatchEnded(end string) {
	if m == nil {
		return
	}
	switch end {
	case rpcdedup.WatchEndNotified:
		incCounter(m.waitNotified)
	case rpcdedup.WatchEndPolled:
		incCounter(m.waitPolled)
	case rpcdedup.WatchEndDeadline:
		incCounter(m.waitDeadline)
	}
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
	case op == RPCRegistryDedupNotifyDone && ok:
		incCounter(m.notifyDoneOK)
	case op == RPCRegistryDedupNotifyDone:
		incCounter(m.notifyDoneErr)
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
