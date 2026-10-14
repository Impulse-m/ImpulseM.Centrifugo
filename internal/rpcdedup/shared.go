package rpcdedup

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog/log"
)

// errStillInFlight is the result of a poll of a call which still runs on another node. The watcher retries on it.
var errStillInFlight = errors.New("the call still runs on another node")

const (
	// sharedDefaultPollInitial and sharedDefaultPollMax bound the delay between two polls of a call which runs on
	// another node.
	sharedDefaultPollInitial = 50 * time.Millisecond
	sharedDefaultPollMax     = 500 * time.Millisecond
	// sharedDefaultAckQueueSize is the number of acks which wait for a flush. Acks beyond it are dropped.
	sharedDefaultAckQueueSize = 65536
	// sharedDefaultAckFlushEvery is the interval of the ack flush, and sharedDefaultAckBatch the number of queued
	// acks which flush at once.
	sharedDefaultAckFlushEvery = 100 * time.Millisecond
	sharedDefaultAckBatch      = 1024
	// sharedErrorLogInterval is the minimum time between two logged errors of the shared tier.
	sharedErrorLogInterval = 10 * time.Second
)

// SharedObserver observes the shared tier. Methods may be called from any goroutine.
type SharedObserver interface {
	// AckDropped is called for every ack which is dropped because the ack queue is full.
	AckDropped()
}

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
	// Observer observes the ack queue. Nil means no observation.
	Observer SharedObserver
	// PollInitial and PollMax bound the delay between two polls of a call which runs on another node. Zero
	// means the defaults.
	PollInitial time.Duration
	PollMax     time.Duration
	// AckQueueSize, AckFlushEvery and AckBatch configure the batched acks to the shared tier. Zero means the
	// defaults.
	AckQueueSize  int
	AckFlushEvery time.Duration
	AckBatch      int
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
	// inflightGrace is the time an in-flight record outlives the method timeout of its call. It is InflightGrace,
	// and a test shortens it.
	inflightGrace time.Duration
	ackFlushEvery time.Duration
	ackBatch      int
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
	return &Shared{
		local:         opts.Local,
		backend:       opts.Backend,
		nodeID:        opts.NodeID,
		ttl:           opts.TTL,
		maxReplyBytes: opts.MaxReplyBytes,
		failOpen:      opts.FailOpen,
		notifier:      opts.Notifier,
		observer:      opts.Observer,
		pollInitial:   opts.PollInitial,
		pollMax:       opts.PollMax,
		inflightGrace: InflightGrace,
		ackFlushEvery: opts.AckFlushEvery,
		ackBatch:      opts.AckBatch,
		acks:          make(chan Key, opts.AckQueueSize),
		errorLog:      tools.NewIntervalLimiter(sharedErrorLogInterval),
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
	}
	return lookup, nil
}

// pollUncached answers a local record which kept no reply with the reply of the shared tier, when it has one. The
// reply is not cached locally: the local record stands for every other case.
func (s *Shared) pollUncached(key Key, local Lookup) Lookup {
	res, err := s.backend.Poll(context.Background(), key)
	if err != nil || res.State != PollDone || res.Record.Uncached {
		return local
	}
	return Lookup{Outcome: OutcomeDone, Record: res.Record}
}

// beginShared starts the call of key in the shared tier, after its flight is registered locally. The call is a miss,
// done, mismatch or running on another node. A Redis error degrades to node-local dedup with fail-open, and refuses
// the call with fail-closed.
func (s *Shared) beginShared(key Key, flight *Flight) (Lookup, error) {
	res, err := s.backend.Begin(context.Background(), key, flight.fingerprint, s.nodeID, flight.timeout+s.inflightGrace)
	if err != nil {
		s.logError("exactly-once RPC: the shared tier failed to begin a call", err)
		if s.failOpen {
			flight.localOnly = true
			return Lookup{Outcome: OutcomeMiss}, nil
		}
		_ = s.local.Complete(key, flight, Record{Abandoned: true})
		return Lookup{}, err
	}
	switch res.State {
	case BeginDone:
		_ = s.local.Complete(key, flight, res.Record)
		return Lookup{Outcome: OutcomeDone, Record: res.Record}, nil
	case BeginMismatch:
		_ = s.local.Complete(key, flight, Record{Abandoned: true})
		return Lookup{Outcome: OutcomeMismatch}, nil
	case BeginInFlight:
		flight.markRemote()
		go s.watch(key, flight)
		return Lookup{Outcome: OutcomeInFlightRemote, Flight: flight}, nil
	}
	return Lookup{Outcome: OutcomeMiss}, nil
}

// watch polls the shared tier until the call of key, which runs on another node, is done, and completes the local
// flight with its answer. A call whose record vanishes, or which does not finish within the in-flight TTL, is
// abandoned: its duplicates retry.
func (s *Shared) watch(key Key, flight *Flight) {
	deadline := flight.timeout + s.inflightGrace
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = s.pollInitial
	b.MaxInterval = s.pollMax
	b.Multiplier = 2
	b.RandomizationFactor = 0.2
	rec, err := backoff.Retry(ctx, func() (Record, error) {
		res, err := s.backend.Poll(ctx, key)
		if err != nil {
			return Record{}, err
		}
		switch res.State {
		case PollDone:
			return res.Record, nil
		case PollMissing:
			return Record{Abandoned: true}, nil
		}
		return Record{}, errStillInFlight
	}, backoff.WithBackOff(b), backoff.WithMaxElapsedTime(deadline))
	if err != nil {
		rec = Record{Abandoned: true}
	}
	_ = s.local.Complete(key, flight, rec)
}

// Complete implements Store. The shared tier is written first, so a client which got the answer and reconnected to
// another node finds it there; then the node-local store keeps the record and wakes the local duplicates. A flight
// which runs without the shared tier skips it.
func (s *Shared) Complete(key Key, flight *Flight, rec Record) error {
	if !flight.localOnly {
		s.completeShared(key, flight, rec)
	}
	return s.local.Complete(key, flight, rec)
}

// completeShared writes the record of a call which this node owns to the shared tier. A reply over the limit is
// written as uncached. A Redis error is logged and counted, and the record is still kept locally.
func (s *Shared) completeShared(key Key, flight *Flight, rec Record) {
	stored := rec
	if rec.Err == nil && len(rec.Reply) > s.maxReplyBytes {
		stored = Record{Uncached: true, Abandoned: rec.Abandoned}
	}
	if _, err := s.backend.Complete(context.Background(), key, s.nodeID, stored, s.ttl); err != nil {
		s.logError("exactly-once RPC: the shared tier failed to keep a record", err)
	}
}

// Cancel implements Store. A call which runs on this node is cancelled here, and its owner keeps the record. Otherwise
// the shared tier decides: a call which runs on another node is cancelled by a notification to that node, and a call
// which did not start leaves a tombstone, which is also kept locally. A scope which this node refuses is refused
// before the shared tier, so nothing is written for it.
func (s *Shared) Cancel(key Key, failure *centrifuge.Error) error {
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
			return s.local.Cancel(key, failure)
		}
		return err
	}
	switch res.State {
	case CancelTombstone:
		return s.local.Cancel(key, failure)
	case CancelInFlight:
		if res.Owner != s.nodeID && s.notifier != nil {
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
			if s.observer != nil {
				s.observer.AckDropped()
			}
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
			batch = s.drainAcks(batch)
			s.flushAcks(batch)
			return ctx.Err()
		case key := <-s.acks:
			batch = append(batch, key)
			if len(batch) >= s.ackBatch {
				s.flushAcks(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			s.flushAcks(batch)
			batch = batch[:0]
		}
	}
}

// drainAcks appends the acks which are queued at the moment to batch, without waiting.
func (s *Shared) drainAcks(batch []Key) []Key {
	for {
		select {
		case key := <-s.acks:
			batch = append(batch, key)
		default:
			return batch
		}
	}
}

// flushAcks sends the acks of keys to the shared tier, grouped by scope, as one batch. Errors are counted by the
// backend, and the records of the failed acks expire at their TTL.
func (s *Shared) flushAcks(keys []Key) {
	if len(keys) == 0 {
		return
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
	if err := s.backend.Ack(context.Background(), acks); err != nil {
		s.logError("exactly-once RPC: the shared tier failed to forget acked calls", err)
	}
}

// logError logs an error of the shared tier at most once per sharedErrorLogInterval.
func (s *Shared) logError(msg string, err error) {
	if s.errorLog.Allow() {
		log.Error().Err(err).Msg(msg)
	}
}
