package rpcdedup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	// another node, when the wakeup is off.
	sharedDefaultPollInitial = 50 * time.Millisecond
	sharedDefaultPollMax     = 500 * time.Millisecond
	// sharedDefaultMaxWaiterNodes is the cap of the waiter list of a record when SharedOptions.MaxWaiterNodes is zero.
	sharedDefaultMaxWaiterNodes = 64
	// wakeCheckDelay is the time after which a wait which has not been woken reads the record once, see watchWake.
	wakeCheckDelay = 2 * time.Second
	// wakeWatchIdle is how long the wake watcher of a node runs after the node's last remote wait ended. It is the
	// lifetime of a wake stream, so a wake written while no watcher runs is still in the stream when one starts.
	wakeWatchIdle = wakeStreamTTL
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
	// WatchRead is called for every read of the shared tier which a wait for a call on another node makes.
	WatchRead()
	// WatchEnded is called once per wait for a call on another node, with how it ended: one of the WatchEnd values.
	WatchEnded(end string)
	// Woke is called once per wake write of a completed call, with the nodes written and the nodes which failed.
	Woke(written int, failed int)
}

// How a wait for a call on another node ended, the values of SharedObserver.WatchEnded.
const (
	// WatchEndWoken means a wake came, and the read which followed it found the answer, or the key gone.
	WatchEndWoken = "woken"
	// WatchEndChecked means the check found the answer, or the key gone, without a wake before it.
	WatchEndChecked = "checked"
	// WatchEndPolled means a poll found the answer, or the key gone, with the wakeup off.
	WatchEndPolled = "polled"
	// WatchEndDeadline means the in-flight TTL passed with the call still in flight, and no read after that.
	WatchEndDeadline = "deadline"
	// WatchEndRefused means the waiter list of the record was full: the wait did not start.
	WatchEndRefused = "refused"
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
	// Notifier asks the node which runs a call to cancel it. Nil means no cross-node cancel. It is called from a
	// goroutine of its own, so it may take as long as the transport needs.
	Notifier func(nodeID string, key Key)
	// Observer observes the ack queue. Nil means no observation.
	Observer SharedObserver
	// PollInitial and PollMax bound the delay between two polls of a call which runs on another node, when Wakeup is
	// off. Zero means the defaults.
	PollInitial time.Duration
	PollMax     time.Duration
	// Wakeup registers this node as a waiter of the calls it waits for in the shared tier, writes a wake for the
	// waiters of the calls it runs, and runs the wake watcher while this node has remote waits. Off, a wait polls at
	// PollInitial doubling to PollMax, and nothing else runs.
	Wakeup bool
	// MaxWaiterNodes is the cap of the waiter list of a record, as the number of nodes. Zero means 64. It is used with
	// Wakeup only.
	MaxWaiterNodes int
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
	observer      SharedObserver
	pollInitial   time.Duration
	pollMax       time.Duration
	// wakeup is set when the waits are woken by the wake streams, see SharedOptions.Wakeup.
	wakeup bool
	// maxWaiterNodes is the cap of the waiter list of a record, sent to the begin script when wakeup is set.
	maxWaiterNodes int
	// checkDelay is the time of the check of a wait, wakeCheckDelay unless a test shortens it.
	checkDelay time.Duration
	// watchIdleAfter is how long the wake watcher runs after the last remote wait, wakeWatchIdle unless a test shortens
	// it.
	watchIdleAfter time.Duration
	// inflightGrace is the time an in-flight record outlives the method timeout of its call. It is InflightGrace,
	// and a test shortens it.
	inflightGrace time.Duration
	ackFlushEvery time.Duration
	ackBatch      int
	ioTimeout     time.Duration
	acks          chan Key
	errorLog      *tools.IntervalLimiter
	refusedLog    *tools.IntervalLimiter
	// remoteWaits is the number of waits for calls on another node which run on this node.
	remoteWaits atomic.Int64
	// lastWaitEnd is the time in Unix nanoseconds when the last of them ended, zero when none did.
	lastWaitEnd atomic.Int64
	// watchMu guards watching, stopped, watchGen and watchCancel, which the wake watcher starts and stops under.
	watchMu sync.Mutex
	// watching is set while a wake watcher runs, or is started; watchGen numbers the watchers, and watchCancel stops the
	// current one.
	watching    bool
	watchGen    uint64
	watchCancel context.CancelFunc
	// stopped is set when Run ends: no wake watcher starts after it.
	stopped bool
	// watchWG waits for the wake watchers when Run ends.
	watchWG sync.WaitGroup
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
	if opts.MaxWaiterNodes <= 0 {
		opts.MaxWaiterNodes = sharedDefaultMaxWaiterNodes
	}
	return &Shared{
		local:          opts.Local,
		backend:        opts.Backend,
		nodeID:         opts.NodeID,
		ttl:            opts.TTL,
		maxReplyBytes:  opts.MaxReplyBytes,
		failOpen:       opts.FailOpen,
		notifier:       opts.Notifier,
		observer:       opts.Observer,
		pollInitial:    opts.PollInitial,
		pollMax:        opts.PollMax,
		wakeup:         opts.Wakeup,
		maxWaiterNodes: opts.MaxWaiterNodes,
		checkDelay:     wakeCheckDelay,
		watchIdleAfter: wakeWatchIdle,
		inflightGrace:  InflightGrace,
		ackFlushEvery:  opts.AckFlushEvery,
		ackBatch:       opts.AckBatch,
		ioTimeout:      opts.IOTimeout,
		acks:           make(chan Key, opts.AckQueueSize),
		errorLog:       tools.NewIntervalLimiter(redisshard.ErrorLogInterval),
		refusedLog:     tools.NewIntervalLimiter(redisshard.ErrorLogInterval),
	}
}

// Begin implements Store. A call which the node runs is found in the node-local store. A call which runs on another
// node is found in the shared tier, and its flight waits for the answer. See the README of the RPC registry for the
// answers of the tiers.
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
	res, err := s.backend.Begin(context.Background(), key, flight.fingerprint, s.nodeID, flight.timeout+s.inflightGrace, s.waiterCap())
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
		// A cancel which came while the flight had no call to cancel is sent to the node which runs the call now, from a
		// goroutine of its own, so the answer of this duplicate does not wait for the notification.
		if flight.markRemote(s.wakeup && !res.WaitersFull) && res.Owner != s.nodeID && s.notifier != nil {
			go s.notifier(res.Owner, key)
		}
		if res.WaitersFull {
			s.refuseWait(key, flight, res.Owner)
			return Lookup{Outcome: OutcomeInFlightRemote, Flight: flight}, nil
		}
		s.remoteWaits.Add(1)
		if s.wakeup {
			s.ensureWatching()
		}
		go s.watch(key, flight)
		return Lookup{Outcome: OutcomeInFlightRemote, Flight: flight}, nil
	}
	return Lookup{Outcome: OutcomeMiss}, nil
}

// waiterCap returns the cap of the waiter list which the begin script is given: zero, which registers nothing, when the
// wakeup is off.
func (s *Shared) waiterCap() int {
	if !s.wakeup {
		return 0
	}
	return s.maxWaiterNodes
}

// refuseWait completes the flight of a duplicate whose call runs on owner and whose record lists the maximum of waiter
// nodes already. The flight is an abandoned record which did not run, so its duplicates are answered with the
// temporary error at once. The refusal is counted and logged at debug once per interval.
func (s *Shared) refuseWait(key Key, flight *Flight, owner string) {
	_ = s.local.Complete(key, flight, Record{Abandoned: true, NotRun: true})
	if s.observer != nil {
		s.observer.WatchEnded(WatchEndRefused)
	}
	if s.refusedLog.Allow() {
		log.Debug().Int("max_waiter_nodes", s.maxWaiterNodes).Str("owner", owner).Msg("exactly-once RPC: the waiter nodes of a call are full, a duplicate on this node is answered with a later error")
	}
}

// watch is the wait of a flight for a call which runs on another node. With the wakeup off it polls, and with the
// wakeup on it reads on a wake, see watchWake.
func (s *Shared) watch(key Key, flight *Flight) {
	if s.wakeup {
		s.watchWake(key, flight)
		return
	}
	s.watchPolling(key, flight)
}

// watchPolling is the wait with the wakeup off: it polls the shared tier from the start, at the ramp from PollInitial
// doubling to PollMax, until the call is done or the in-flight deadline passes.
func (s *Shared) watchPolling(key Key, flight *Flight) {
	ctx, cancel := context.WithTimeout(context.Background(), flight.timeout+s.inflightGrace)
	defer cancel()
	schedule := newRetryBackOff(s.pollInitial, s.pollMax)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			s.endWatch(key, flight, PollResult{}, WatchEndDeadline)
			return
		case <-timer.C:
		}
		s.watchRead()
		res, err := s.backend.Poll(ctx, key)
		if err == nil && res.State != PollInFlight {
			s.endWatch(key, flight, res, WatchEndPolled)
			return
		}
		// A failed poll is counted by the backend and retried at the ramp, as the wait of a call which runs is.
		timer.Reset(schedule.NextBackOff())
	}
}

// watchWake is the wait with the wakeup on. It reads the shared tier when the wake of the wait comes, once at the check,
// which is wakeCheckDelay after the start jittered by a fifth, and never again: when the in-flight deadline passes with
// no answer read, the wait ends without a read. One timer per wait, re-armed once at the check. A read which finds the
// record in flight, or fails, keeps the wait.
func (s *Shared) watchWake(key Key, flight *Flight) {
	deadline := flight.timeout + s.inflightGrace
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	started := time.Now()
	// A backoff whose initial and maximum interval are both checkDelay yields checkDelay plus or minus a fifth, the
	// ramp's own jitter, so the checks of a wave spread over 0.8 s.
	timer := time.NewTimer(newRetryBackOff(s.checkDelay, s.checkDelay).NextBackOff())
	defer timer.Stop()
	checked := false
	for {
		select {
		case <-flight.wake:
			if s.read(ctx, key, flight, WatchEndWoken) {
				return
			}
		case <-timer.C:
			if checked {
				s.endWatch(key, flight, PollResult{}, WatchEndDeadline)
				return
			}
			checked = true
			if s.read(ctx, key, flight, WatchEndChecked) {
				return
			}
			timer.Reset(deadline - time.Since(started))
		}
	}
}

// read makes one read of the shared tier for a wait, and reports whether the read ended the wait with end. A record which
// is still in flight, or a failed read, keeps the wait; the failure is counted by the backend.
func (s *Shared) read(ctx context.Context, key Key, flight *Flight, end string) bool {
	s.watchRead()
	res, err := s.backend.Poll(ctx, key)
	if err != nil || res.State == PollInFlight {
		return false
	}
	s.endWatch(key, flight, res, end)
	return true
}

// endWatch completes the local flight of a wait which ended as end, and reports the end to the observer. A missing key
// or the deadline abandons the flight without NotRun, as a poll which fails does; a tombstone completes it without a
// fingerprint, as BeginCancelled does; a done record is kept. The wait stops counting as a remote wait, and the time of
// its end is kept for the idle stop of the wake watcher.
func (s *Shared) endWatch(key Key, flight *Flight, res PollResult, end string) {
	switch {
	case end == WatchEndDeadline || res.State == PollMissing:
		_ = s.local.Complete(key, flight, Record{Abandoned: true})
	case res.State == PollCancelled:
		s.local.finish(key, flight, res.Record, encodeError(res.Record.Err))
	default:
		_ = s.local.Complete(key, flight, res.Record)
	}
	s.remoteWaits.Add(-1)
	s.lastWaitEnd.Store(time.Now().UnixNano())
	if s.observer != nil {
		s.observer.WatchEnded(end)
	}
}

// watchRead counts a read of the shared tier made by a wait for a call on another node.
func (s *Shared) watchRead() {
	if s.observer != nil {
		s.observer.WatchRead()
	}
}

// ensureWatching starts the wake watcher of this node unless one runs, or Run has ended. The watcher reads from the
// start of the wake stream, so a wake written before it started is read too.
func (s *Shared) ensureWatching() {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.watching || s.stopped {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.watching = true
	s.watchGen++
	s.watchCancel = cancel
	s.watchWG.Add(1)
	go s.runWakes(ctx, s.watchGen)
}

// runWakes runs the wake watcher of generation gen until it returns. A watcher which returns without idling, because it
// could not start or ctx ended, clears the flag so that the next remote wait starts a new one.
func (s *Shared) runWakes(ctx context.Context, gen uint64) {
	defer s.watchWG.Done()
	if err := s.backend.WatchWakes(ctx, s.nodeID, s.Wake, s.watchIdle); err != nil {
		s.logError("exactly-once RPC: the wake watcher of this node did not start, the waits are checked until the next one starts", err)
	}
	s.watchEnded(gen)
}

// watchEnded clears the watching flag of generation gen, when that watcher is still the current one.
func (s *Shared) watchEnded(gen uint64) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.watching && s.watchGen == gen {
		s.watching = false
		s.watchCancel()
	}
}

// watchIdle reports whether the wake watcher may stop, which is the case when this node has no remote wait and none
// ended within watchIdleAfter. It is called at an empty read. When it reports true it clears the watching flag and
// cancels the watcher, under the same lock that ensureWatching takes, so a remote wait always has a watcher running or
// starting.
func (s *Shared) watchIdle() bool {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.remoteWaits.Load() > 0 {
		return false
	}
	if time.Since(time.Unix(0, s.lastWaitEnd.Load())) < s.watchIdleAfter {
		return false
	}
	s.watching = false
	s.watchCancel()
	return true
}

// stopWatching ends the wake watcher, if one runs, and forbids new ones. It waits for the watchers to return, so that
// Run returns after the last read of the shared tier of the watchers.
func (s *Shared) stopWatching() {
	s.watchMu.Lock()
	s.stopped = true
	if s.watching {
		s.watching = false
		s.watchCancel()
	}
	s.watchMu.Unlock()
	s.watchWG.Wait()
}

// Complete implements Store. The shared tier is written first, so a client which got the answer and reconnected to
// another node finds it there; then the node-local store keeps the record and wakes the local duplicates; then the
// waiters of the record are woken in the shared tier. A flight which runs without the shared tier skips it.
func (s *Shared) Complete(key Key, flight *Flight, rec Record) error {
	// The error is encoded once, and the encoded form is passed on to both tiers.
	errJSON := encodeError(rec.Err)
	if flight.localOnly {
		s.local.complete(key, flight, rec, errJSON)
		return nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), s.completeLimit(flight))
	defer cancel()
	waiters := s.completeShared(ctx, key, flight, rec, errJSON)
	s.local.complete(key, flight, rec, errJSON)
	s.wakeWaiters(ctx, key, waiters)
	return nil
}

// completeLimit returns the time by which the writes of the record of flight to the shared tier must end: the IO timeout,
// or the in-flight expiry of the call, whichever is sooner.
func (s *Shared) completeLimit(flight *Flight) time.Time {
	limit := time.Now().Add(s.ioTimeout)
	if flight.begun != 0 {
		if expiry := time.Unix(0, flight.begun).Add(flight.timeout + s.inflightGrace); expiry.Before(limit) {
			limit = expiry
		}
	}
	return limit
}

// wakeWaiters writes a wake for each of the waiter nodes of key, in the shared tier, within ctx. Nothing is written
// without the wakeup, or without waiters. It runs after the local flight is done, so the local duplicates are answered
// first. A failed wake is counted and logged; its wait is checked.
func (s *Shared) wakeWaiters(ctx context.Context, key Key, waiters []string) {
	if !s.wakeup || len(waiters) == 0 {
		return
	}
	written, err := s.backend.Wake(ctx, key, waiters)
	if s.observer != nil {
		s.observer.Woke(written, len(waiters)-written)
	}
	if err != nil {
		s.logError("exactly-once RPC: the shared tier failed to wake a waiter, its wait is checked", err)
	}
}

// Wake tells the wait for the call of key, when there is one on this node, that its record may be written. It is the
// receiver of the wakes of the wake watcher. A key without a wait, and a key whose call runs on this node, are ignored.
func (s *Shared) Wake(key Key) {
	if flight := s.local.flightOf(key); flight != nil {
		flight.nudge()
	}
}

// completeShared writes the record of a call which this node owns to the shared tier, with errJSON, the encoded form of
// its error. A record over the limit is written as uncached. A failed write is retried once, with backoff: the owner
// check makes a retry idempotent. All the writes of the call stay within ctx, which ends at the IO timeout or the
// in-flight TTL of the call, counted from its begin, so the reply of the call is not held longer than that. When the
// writes fail, the record is still kept locally, and a retransmit on another node runs the call again once the in-flight
// record expires. A write which reports that this node does not own the in-flight record is not retried, and neither is
// a write which finds no shard of the shared tier connected: a retry cannot reach a shard which is not there, so the
// reply does not wait for the backoff. It returns the waiters of the write which took effect, and nil when none did.
func (s *Shared) completeShared(ctx context.Context, key Key, flight *Flight, rec Record, errJSON string) []string {
	stored := rec
	if overReplyLimit(rec, errJSON, s.maxReplyBytes) {
		stored = Record{Uncached: true, Abandoned: rec.Abandoned}
		errJSON = ""
	}
	limit, _ := ctx.Deadline()
	var attempts int
	res, err := backoff.Retry(ctx, func() (CompleteResult, error) {
		attempts++
		res, err := s.backend.Complete(ctx, key, s.nodeID, stored, errJSON, s.ttl)
		if errors.Is(err, redisshard.ErrNotConnected) {
			return res, backoff.Permanent(err)
		}
		return res, err
	},backoff.WithBackOff(newRetryBackOff(sharedCompleteBackoff, sharedCompleteMaxBackoff)), backoff.WithMaxTries(sharedCompleteAttempts), backoff.WithMaxElapsedTime(time.Until(limit)))
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
			// The notification is sent from a goroutine of its own: the answer to the cancel does not wait for it.
			go s.notifier(res.Owner, key)
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

// Run flushes the queued acks to the shared tier, as soon as a batch is full, and otherwise AckFlushEvery after the first
// ack of the batch. The flush timer is armed only while a batch holds acks, so an idle node runs no timer. When ctx is
// done it stops the wake watcher, flushes what is queued once more and returns the context error. It starts no wake
// watcher itself: those start with the first remote wait. It implements service.Service.
func (s *Shared) Run(ctx context.Context) error {
	flush := time.NewTimer(s.ackFlushEvery)
	flush.Stop()
	defer flush.Stop()
	// flushC is the channel of flush while its batch holds acks, and nil otherwise, which blocks its case.
	var flushC <-chan time.Time
	batch := make([]Key, 0, s.ackBatch)
	for {
		select {
		case <-ctx.Done():
			s.stopWatching()
			s.drainAcks(batch)
			return ctx.Err()
		case key := <-s.acks:
			if len(batch) == 0 {
				flush.Reset(s.ackFlushEvery)
				flushC = flush.C
			}
			batch = append(batch, key)
			if len(batch) >= s.ackBatch {
				s.flushAcks(context.Background(), batch)
				batch = batch[:0]
				flush.Stop()
				flushC = nil
			}
		case <-flushC:
			s.flushAcks(context.Background(), batch)
			batch = batch[:0]
			flushC = nil
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
	logErrorWith(s.errorLog, msg, err)
}

// logErrorWith logs err with msg when limiter allows it, which keeps each limiter to one line per
// redisshard.ErrorLogInterval.
func logErrorWith(limiter *tools.IntervalLimiter, msg string, err error) {
	if limiter.Allow() {
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
