package rpcdedup

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog/log"
)

// errNilCancelFailure is the result of a cancel without the error which the cancelled call answers with. Such a cancel
// would be kept as a success with an empty reply, so it is refused.
var errNilCancelFailure = errors.New("a cancel needs the error which the cancelled call answers with")

const (
	// sharedDefaultPollInitial and sharedDefaultPollMax bound the delay between two polls of a call which runs on
	// another node.
	sharedDefaultPollInitial = 50 * time.Millisecond
	sharedDefaultPollMax     = 500 * time.Millisecond
	// sharedDefaultWakeupPollInterval is the fallback poll interval of a wait while the wakeup by notification is on.
	sharedDefaultWakeupPollInterval = 2 * time.Second
	// sharedDefaultAckQueueSize is the number of acks which wait for a flush. Acks beyond it are dropped.
	sharedDefaultAckQueueSize = 65536
	// sharedDefaultAckFlushEvery is the interval of the ack flush, and sharedDefaultAckBatch the number of queued
	// acks which flush at once.
	sharedDefaultAckFlushEvery = 100 * time.Millisecond
	sharedDefaultAckBatch      = 1024
	// sharedCompleteAttempts is the number of tries of a record's write to the shared tier, and sharedCompleteBackoff
	// the delay before the retry, which doubles. The writes of all tries together stay within one IO timeout, so the
	// reply of the call is not held longer.
	sharedCompleteAttempts   = 2
	sharedCompleteBackoff    = 50 * time.Millisecond
	sharedCompleteMaxBackoff = 500 * time.Millisecond
)

// SharedObserver observes the shared tier. Methods may be called from any goroutine.
type SharedObserver interface {
	// AckDropped is called for every ack which is dropped because the ack queue is full, or which a shutdown leaves
	// unsent.
	AckDropped()
	// LostOwnership is called once for a call whose first write of its answer finds no in-flight record of this node,
	// so the answer is kept on this node only.
	LostOwnership()
	// WatchPolled is called for every poll of the shared tier which a wait for a call on another node makes.
	WatchPolled()
	// WatchEnded is called once per wait for a call on another node, with how it ended: WatchEndNotified,
	// WatchEndPolled or WatchEndDeadline.
	WatchEnded(end string)
}

// How a wait for a call on another node ended, the values of SharedObserver.WatchEnded.
const (
	// WatchEndNotified means a wakeup came, and the poll which followed it found the answer, or the key gone.
	WatchEndNotified = "notified"
	// WatchEndPolled means a poll found the answer, or the key gone, without a wakeup before it.
	WatchEndPolled = "polled"
	// WatchEndDeadline means the in-flight TTL passed with the call still in flight, or with the polls failing.
	WatchEndDeadline = "deadline"
)

// SharedOptions configure a Shared store.
type SharedOptions struct {
	// Local is the node-local store, which keeps the records of this node and the flights of its calls.
	Local *Memory
	// Backend is the shared tier.
	Backend Backend
	// NodeID is the id of this node, which owns the calls it runs.
	NodeID string
	// TTL is the time a done record and a tombstone are kept in the shared tier.
	TTL time.Duration
	// MaxReplyBytes is the largest reply kept in the shared tier. A larger reply is kept as uncached.
	MaxReplyBytes int
	// FailOpen runs calls with node-local dedup when the shared tier fails. Otherwise the calls are refused.
	FailOpen bool
	// Notifier asks the node which runs a call to cancel it. Nil means no cross-node cancel.
	Notifier func(nodeID string, key Key)
	// DoneNotifier tells a node which waits for a call of this node that its record is written. Nil means waiters are
	// not registered and not notified: they poll.
	DoneNotifier func(nodeID string, key Key)
	// Observer observes the ack queue. Nil means no observation.
	Observer SharedObserver
	// PollInitial and PollMax bound the delay between two polls of a call which runs on another node. Zero
	// means the defaults.
	PollInitial time.Duration
	PollMax     time.Duration
	// WakeupPollInterval is the fallback poll interval of a wait while DoneNotifier is set. Zero means 2s. Without
	// DoneNotifier the wait polls at PollInitial doubling to PollMax.
	WakeupPollInterval time.Duration
	// AckQueueSize, AckFlushEvery and AckBatch configure the batched acks to the shared tier. Zero means the
	// defaults.
	AckQueueSize  int
	AckFlushEvery time.Duration
	AckBatch      int
	// IOTimeout bounds the writes of a record to the shared tier which Complete makes, all tries together. Zero means
	// redisshard.DefaultIOTimeout.
	IOTimeout time.Duration
}

// Shared is the Store of a node which shares its records with other nodes. The node-local store is the cache of
// done records and the join point of the calls of the node, and the shared tier is authoritative for whether a call
// ran and what it answered. It implements Store, and Run flushes its acks.
type Shared struct {
	local         *Memory
	backend       Backend
	nodeID        string
	ttl           time.Duration
	maxReplyBytes int
	failOpen      bool
	notifier      func(nodeID string, key Key)
	doneNotifier  func(nodeID string, key Key)
	observer      SharedObserver
	pollInitial   time.Duration
	pollMax       time.Duration
	// wakeupPollInterval is the fallback poll interval of a wait while doneNotifier is set.
	wakeupPollInterval time.Duration
	// inflightGrace is the time an in-flight record outlives the method timeout of its call. It is InflightGrace,
	// and a test shortens it.
	inflightGrace time.Duration
	ackFlushEvery time.Duration
	ackBatch      int
	ioTimeout     time.Duration
	acks          chan Key
	errorLog      *tools.IntervalLimiter
}

// NewShared returns the store which composes opts.Local with opts.Backend. Run must run for the acks to reach the
// shared tier.
func NewShared(opts SharedOptions) *Shared {
	if opts.PollInitial <= 0 {
		opts.PollInitial = sharedDefaultPollInitial
	}
	if opts.PollMax <= 0 {
		opts.PollMax = sharedDefaultPollMax
	}
	if opts.AckQueueSize <= 0 {
		opts.AckQueueSize = sharedDefaultAckQueueSize
	}
	if opts.AckFlushEvery <= 0 {
		opts.AckFlushEvery = sharedDefaultAckFlushEvery
	}
	if opts.AckBatch <= 0 {
		opts.AckBatch = sharedDefaultAckBatch
	}
	if opts.IOTimeout <= 0 {
		opts.IOTimeout = redisshard.DefaultIOTimeout
	}
	if opts.WakeupPollInterval <= 0 {
		opts.WakeupPollInterval = sharedDefaultWakeupPollInterval
	}
	return &Shared{
		local:              opts.Local,
		backend:            opts.Backend,
		nodeID:             opts.NodeID,
		ttl:                opts.TTL,
		maxReplyBytes:      opts.MaxReplyBytes,
		failOpen:           opts.FailOpen,
		notifier:           opts.Notifier,
		doneNotifier:       opts.DoneNotifier,
		observer:           opts.Observer,
		pollInitial:        opts.PollInitial,
		pollMax:            opts.PollMax,
		wakeupPollInterval: opts.WakeupPollInterval,
		inflightGrace:      InflightGrace,
		ackFlushEvery:      opts.AckFlushEvery,
		ackBatch:           opts.AckBatch,
		ioTimeout:          opts.IOTimeout,
		acks:               make(chan Key, opts.AckQueueSize),
		errorLog:           tools.NewIntervalLimiter(redisshard.ErrorLogInterval),
	}
}

// Begin implements Store. A call which the node runs is found in the node-local store. A call which runs on another
// node is found in the shared tier, and its flight waits for the answer by polling. See the README of the RPC
// registry for the answers of the tiers.
func (s *Shared) Begin(key Key, flight *Flight) (Lookup, error) {
	lookup, err := s.local.Begin(key, flight)
	if err != nil {
		return lookup, err
	}
	switch lookup.Outcome {
	case OutcomeDone:
		if lookup.Record.Uncached && s.backend.Connected() {
			return s.pollUncached(key, lookup), nil
		}
		return lookup, nil
	case OutcomeMiss:
		return s.beginShared(key, flight)
	case OutcomeInFlight:
		// A duplicate of a call which runs on another node waits for it too, and counts as a remote wait. A duplicate
		// which joins the flight before beginShared marks it remote counts as a local join: the metric undercounts
		// that narrow window, and nothing else depends on it.
		if lookup.Flight.isRemote() {
			lookup.Outcome = OutcomeInFlightRemote
		}
	}
	return lookup, nil
}

// pollUncached answers a local record which kept no reply with the reply of the shared tier, when it has one. The
// reply is not cached locally: the local record stands for every other case.
func (s *Shared) pollUncached(key Key, local Lookup) Lookup {
	res, err := s.backend.Poll(context.Background(), key)
	if err != nil || (res.State != PollDone && res.State != PollCancelled) || res.Record.Uncached {
		return local
	}
	return Lookup{Outcome: OutcomeDone, Record: res.Record}
}

// beginShared starts the call of key in the shared tier, after its flight is registered locally. The call is a miss,
// done, mismatch or running on another node. A Redis error degrades to node-local dedup with fail-open, and refuses
// the call with fail-closed.
func (s *Shared) beginShared(key Key, flight *Flight) (Lookup, error) {
	flight.begun = time.Now().UnixNano()
	res, err := s.backend.Begin(context.Background(), key, flight.fingerprint, s.nodeID, flight.timeout+s.inflightGrace, s.doneNotifier != nil)
	if err != nil {
		s.logError("exactly-once RPC: the shared tier failed to begin a call", err)
		if s.failOpen {
			flight.localOnly = true
			return Lookup{Outcome: OutcomeMiss}, nil
		}
		_ = s.local.Complete(key, flight, Record{Abandoned: true, NotRun: true})
		return Lookup{}, err
	}
	switch res.State {
	case BeginDone:
		_ = s.local.Complete(key, flight, res.Record)
		return Lookup{Outcome: OutcomeDone, Record: res.Record}, nil
	case BeginCancelled:
		// The tombstone keeps no fingerprint, so this node answers any duplicate with it, as the shared tier does.
		s.local.finish(key, flight, res.Record, encodeError(res.Record.Err))
		return Lookup{Outcome: OutcomeDone, Record: res.Record}, nil
	case BeginMismatch:
		_ = s.local.Complete(key, flight, Record{Abandoned: true})
		return Lookup{Outcome: OutcomeMismatch}, nil
	case BeginInFlight:
		// A cancel which came while the flight had no call to cancel is sent to the node which runs the call now. The
		// notifier is called before the answer, so a slow notifier delays the duplicate by one publish.
		if flight.markRemote(s.doneNotifier != nil) && res.Owner != s.nodeID && s.notifier != nil {
			s.notifier(res.Owner, key)
		}
		go s.watch(key, flight)
		return Lookup{Outcome: OutcomeInFlightRemote, Flight: flight}, nil
	}
	return Lookup{Outcome: OutcomeMiss}, nil
}

// watch polls the shared tier until the call of key, which runs on another node, is done, and completes the local
// flight with its answer. A call whose record vanishes, whose polls fail, or which does not finish within the
// in-flight TTL, is abandoned without NotRun: its duplicates get no retry signal, since the call may have run on the
// other node. A tombstone completes the flight without a fingerprint, as BeginCancelled does, so
// this node answers any duplicate with it, as the shared tier does.
func (s *Shared) watch(key Key, flight *Flight) {
	deadline := flight.timeout + s.inflightGrace
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	// Without the wakeup the first poll is at once, and the delays ramp up. With it, the first poll waits one fallback
	// interval, or a wakeup which came before the watch started, and the polls are at the fallback interval.
	schedule := newRetryBackOff(s.pollInitial, s.pollMax)
	first := time.Duration(0)
	if s.doneNotifier != nil {
		schedule = newRetryBackOff(s.wakeupPollInterval, s.wakeupPollInterval)
		first = schedule.NextBackOff()
	}
	timer := time.NewTimer(first)
	defer timer.Stop()
	for {
		notified := false
		select {
		case <-ctx.Done():
			s.endWatch(key, flight, PollResult{}, WatchEndDeadline)
			return
		case <-flight.wake:
			notified = true
		case <-timer.C:
		}
		s.watchPolled()
		res, err := s.backend.Poll(ctx, key)
		if err == nil && res.State != PollInFlight {
			end := WatchEndPolled
			if notified {
				end = WatchEndNotified
			}
			s.endWatch(key, flight, res, end)
			return
		}
		// A failed poll is counted by the backend and retried at the schedule, as today.
		timer.Stop()
		timer.Reset(schedule.NextBackOff())
	}
}

// endWatch completes the local flight of a wait which ended as end, and reports the end to the observer. A missing key
// or the deadline abandons the flight without NotRun, as a poll which fails does; a tombstone completes it without a
// fingerprint, as BeginCancelled does; a done record is kept.
func (s *Shared) endWatch(key Key, flight *Flight, res PollResult, end string) {
	switch {
	case end == WatchEndDeadline || res.State == PollMissing:
		_ = s.local.Complete(key, flight, Record{Abandoned: true})
	case res.State == PollCancelled:
		s.local.finish(key, flight, res.Record, encodeError(res.Record.Err))
	default:
		_ = s.local.Complete(key, flight, res.Record)
	}
	if s.observer != nil {
		s.observer.WatchEnded(end)
	}
}

// watchPolled counts a poll of the shared tier made by a wait for a call on another node.
func (s *Shared) watchPolled() {
	if s.observer != nil {
		s.observer.WatchPolled()
	}
}

// Complete implements Store. The shared tier is written first, so a client which got the answer and reconnected to
// another node finds it there; then the node-local store keeps the record and wakes the local duplicates. A flight
// which runs without the shared tier skips it.
func (s *Shared) Complete(key Key, flight *Flight, rec Record) error {
	// The error is encoded once, and the encoded form is passed on to both tiers.
	errJSON := encodeError(rec.Err)
	var waiters []string
	if !flight.localOnly {
		waiters = s.completeShared(key, flight, rec, errJSON)
	}
	s.local.complete(key, flight, rec, errJSON)
	s.notifyDone(key, waiters)
	return nil
}

// notifyDone tells every waiter node of key but this node that the record is written. Nothing is sent without a
// DoneNotifier, or without waiters. It runs after the local flight is done, so the local duplicates are answered first.
func (s *Shared) notifyDone(key Key, waiters []string) {
	if s.doneNotifier == nil {
		return
	}
	for _, nodeID := range waiters {
		if nodeID != s.nodeID {
			s.doneNotifier(nodeID, key)
		}
	}
}

// Wake tells the wait for the call of key, when there is one on this node, that its record may be written. It is the
// receiver of the done notification. A key without a wait, and a key whose call runs on this node, are ignored.
func (s *Shared) Wake(key Key) {
	if flight := s.local.flightOf(key); flight != nil {
		flight.nudge()
	}
}

// completeShared writes the record of a call which this node owns to the shared tier, with errJSON, the encoded form of
// its error. A record over the limit is written as uncached. A failed write is retried once, with backoff: the owner
// check makes a retry idempotent. All the writes of the call stay within one IO timeout, and within the in-flight TTL
// of the call, which is counted from its begin, so the reply of the call is not held longer than that. When the writes
// fail, the record is still kept locally, and a retransmit on another node runs the call again once the in-flight
// record expires. A write which reports that this node does not own the in-flight record is not retried. It returns the
// waiters of the write which took effect, and nil when none did.
func (s *Shared) completeShared(key Key, flight *Flight, rec Record, errJSON string) []string {
	stored := rec
	if overReplyLimit(rec, errJSON, s.maxReplyBytes) {
		stored = Record{Uncached: true, Abandoned: rec.Abandoned}
		errJSON = ""
	}
	limit := time.Now().Add(s.ioTimeout)
	if flight.begun != 0 {
		if expiry := time.Unix(0, flight.begun).Add(flight.timeout + s.inflightGrace); expiry.Before(limit) {
			limit = expiry
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), limit)
	defer cancel()
	var attempts int
	res, err := backoff.Retry(ctx, func() (CompleteResult, error) {
		attempts++
		return s.backend.Complete(ctx, key, s.nodeID, stored, errJSON, s.ttl)
	}, backoff.WithBackOff(newRetryBackOff(sharedCompleteBackoff, sharedCompleteMaxBackoff)), backoff.WithMaxTries(sharedCompleteAttempts), backoff.WithMaxElapsedTime(time.Until(limit)))
	if err != nil {
		s.logError("exactly-once RPC: the shared tier failed to keep a record, a retransmit on another node may run the call again", err)
		return nil
	}
	if !res.Owned {
		if attempts == 1 {
			s.lostOwnership()
		}
		return nil
	}
	return res.Waiters
}

// Cancel implements Store. A call which runs on this node is cancelled here, and its owner keeps the record. Otherwise
// the shared tier decides: a call which runs on another node is cancelled by a notification to that node, and a call
// which did not start leaves a tombstone, which is also kept locally. A scope which this node refuses is refused
// before the shared tier, so nothing is written for it.
func (s *Shared) Cancel(key Key, failure *centrifuge.Error) error {
	if failure == nil {
		return errNilCancelFailure
	}
	if s.CancelFlight(key) {
		return nil
	}
	if err := s.local.CheckAdmit(key.Scope); err != nil {
		return err
	}
	res, err := s.backend.Cancel(context.Background(), key, failure, s.ttl)
	if err != nil {
		s.logError("exactly-once RPC: the shared tier failed to cancel a call", err)
		if s.failOpen {
			// No call of key ran on this node when CancelFlight checked, so the call runs on another node or has not
			// started, and one which begins here meanwhile is not cancelled. A local tombstone would answer a later
			// retransmit with the cancelled error even when the call ran to its end on the other node, and the client
			// could then run it again under a new msg_id. The cancel is not delivered instead, as the README says for
			// fail_open, whose deployments rely on the forwarded msg_id in the backend while the store fails.
			return nil
		}
		return err
	}
	switch res.State {
	case CancelTombstone:
		// The shared tier holds the cancelled error for key, so a done record which this node still caches for it is
		// replaced: the other nodes answer the tombstone, and so must this one. The tombstone fits both tiers: config
		// validation checks the encoded cancelled error against ReplyLimit, the limit of both tiers, so the cancel script
		// stores it with no check of its own (cancelScript). Like any record, the local copy can still be kept as
		// uncached under budget pressure, and a duplicate then reads the tombstone from the shared tier.
		return s.local.Tombstone(key, failure)
	case CancelInFlight:
		// A call which began on this node after the first check above has its flight now, and is cancelled here.
		if res.Owner == s.nodeID {
			s.CancelFlight(key)
		} else if s.notifier != nil {
			s.notifier(res.Owner, key)
		}
	}
	return nil
}

// CancelFlight cancels the call of key when it runs on this node, and reports whether it did. A flight which waits
// for a call on another node is not such a call. It is the receiver of a cancel notification.
func (s *Shared) CancelFlight(key Key) bool {
	flight := s.local.flightOf(key)
	if flight == nil || flight.isRemote() {
		return false
	}
	flight.Cancel()
	return true
}

// Ack implements Store. The acks are forgotten by the node at once, and queued for the shared tier. They are best
// effort: a dropped ack leaves its record to expire at its TTL.
func (s *Shared) Ack(scope string, msgIDs []uint64) error {
	if err := s.local.Ack(scope, msgIDs); err != nil {
		return err
	}
	for _, msgID := range msgIDs {
		select {
		case s.acks <- Key{Scope: scope, MsgID: msgID}:
		default:
			s.ackDropped()
		}
	}
	return nil
}

// Stats implements Store. The gauges are the ones of the node-local store.
func (s *Shared) Stats() Stats {
	return s.local.Stats()
}

// Run flushes the queued acks to the shared tier every AckFlushEvery, and as soon as a batch is full. When ctx is done
// it flushes what is queued once more and returns the context error. It implements service.Service.
func (s *Shared) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.ackFlushEvery)
	defer ticker.Stop()
	batch := make([]Key, 0, s.ackBatch)
	for {
		select {
		case <-ctx.Done():
			s.drainAcks(batch)
			return ctx.Err()
		case key := <-s.acks:
			batch = append(batch, key)
			if len(batch) >= s.ackBatch {
				s.flushAcks(context.Background(), batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			s.flushAcks(context.Background(), batch)
			batch = batch[:0]
		}
	}
}

// drainAcks flushes the acks which are queued, and the batch, without waiting, and then the acks which Ack enqueues
// while the drain runs, for as long as the budget lasts. It flushes in batches of ackBatch keys, as the live path does,
// so a long queue is not sent in one write which the IO timeout may cut. The whole drain has one budget of IOTimeout,
// so a Redis which does not answer holds shutdown for one IO timeout, not one per batch. The keys of a flush which
// fails are counted as dropped, as are the acks which the budget leaves unsent: the batch, and whatever is left in the
// queue when the budget ends. The drain ends when the queue is empty, so an ack which Ack enqueues after it is not
// counted.
func (s *Shared) drainAcks(batch []Key) {
	budget, cancel := context.WithTimeout(context.Background(), s.ioTimeout)
	defer cancel()
	for budget.Err() == nil {
		select {
		case key := <-s.acks:
			batch = append(batch, key)
			if len(batch) >= s.ackBatch {
				batch = s.flushDrained(budget, batch)
			}
		default:
			s.flushDrained(budget, batch)
			return
		}
	}
	// The budget ended before the queue was empty: the batch and the queue are not sent.
	for range len(batch) {
		s.ackDropped()
	}
	for {
		select {
		case <-s.acks:
			s.ackDropped()
		default:
			return
		}
	}
}

// flushDrained flushes batch within budget during a drain, and returns it emptied. The keys of a flush which fails are
// counted as dropped, because a drain does not retry.
func (s *Shared) flushDrained(budget context.Context, batch []Key) []Key {
	if !s.flushAcks(budget, batch) {
		for range len(batch) {
			s.ackDropped()
		}
	}
	return batch[:0]
}

// ackDropped counts an ack which is dropped, when there is an observer.
func (s *Shared) ackDropped() {
	if s.observer != nil {
		s.observer.AckDropped()
	}
}

// flushAcks sends the acks of keys to the shared tier, grouped by scope, as one batch, within ctx. It reports whether the
// batch was sent. A failed flush is logged and counted by the backend, and the records of its acks expire at their TTL.
func (s *Shared) flushAcks(ctx context.Context, keys []Key) bool {
	if len(keys) == 0 {
		return true
	}
	index := make(map[string]int)
	var acks []ScopeAcks
	for _, key := range keys {
		i, found := index[key.Scope]
		if !found {
			i = len(acks)
			index[key.Scope] = i
			acks = append(acks, ScopeAcks{Scope: key.Scope})
		}
		acks[i].MsgIDs = append(acks[i].MsgIDs, key.MsgID)
	}
	if err := s.backend.Ack(ctx, acks); err != nil {
		s.logError("exactly-once RPC: the shared tier failed to forget acked calls", err)
		return false
	}
	return true
}

// newRetryBackOff returns the policy of the retries of the shared tier: the delay starts at initial, doubles up to
// maxDelay, and is randomized by a fifth either way, so that the retries of many calls do not come in step.
func newRetryBackOff(initial time.Duration, maxDelay time.Duration) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initial
	b.MaxInterval = maxDelay
	b.Multiplier = 2
	b.RandomizationFactor = 0.2
	return b
}

// logError logs an error of the shared tier at most once per redisshard.ErrorLogInterval.
func (s *Shared) logError(msg string, err error) {
	if s.errorLog.Allow() {
		log.Error().Err(err).Msg(msg)
	}
}

// lostOwnership counts that the shared tier holds no in-flight record of this node for a call, so its answer is kept on
// this node only. It is not a Redis error, so it is logged at debug.
func (s *Shared) lostOwnership() {
	if s.observer != nil {
		s.observer.LostOwnership()
	}
	log.Debug().Msg("exactly-once RPC: the shared tier holds no in-flight record of this node for a call, the answer is kept on this node only")
}
