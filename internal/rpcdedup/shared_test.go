package rpcdedup

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// fakeEntry is a Redis record of the fake backend: state is 'i' in flight, 'd' done or 'c' cancelled.
type fakeEntry struct {
	state       byte
	fingerprint [32]byte
	owner       string
	rec         Record
	// waiters are the node ids which registered as waiters of the in-flight record, as the field w does.
	waiters []string
}

// addWaiter lists nodeID as a waiter of the in-flight record, once, as beginScript does. It reports whether the list
// holds nodeID after the call, which it does not when the list is at maxWaiters.
func (e *fakeEntry) addWaiter(nodeID string, maxWaiters int) bool {
	for _, waiter := range e.waiters {
		if waiter == nodeID {
			return true
		}
	}
	if len(e.waiters) >= maxWaiters {
		return false
	}
	e.waiters = append(e.waiters, nodeID)
	return true
}

// wakeCall is a call of the fake Wake: the key and the nodes it was written for.
type wakeCall struct {
	key   Key
	nodes []string
}

// fakeBackend is the shared tier in memory. Its scripts follow the semantics of redis_scripts.go, and it counts and
// logs its calls. The fail switch makes every call fail, as an unreachable Redis does.
type fakeBackend struct {
	mu        sync.Mutex
	entries   map[Key]*fakeEntry
	fail      error
	connected bool
	calls     []string
	acks      [][]ScopeAcks
	// completeFailures is the number of the next complete calls which fail, as a timed-out write does.
	completeFailures int
	// lostReplies is the number of the next complete calls which take effect and then fail, as a write whose reply
	// is lost does.
	lostReplies int
	// blockComplete makes every complete call wait until its context is done, as an unanswered Redis does.
	blockComplete bool
	// blockAck makes every ack call wait until its context is done, as an unanswered Redis does. It gives up after a
	// second, so that a flush which ignores its context fails the test instead of hanging it.
	blockAck bool
	// blockBegin makes every begin call wait until its context is done, as an unanswered Redis does. It gives up after a
	// second, so that a begin which ignores its context fails the test instead of hanging it.
	blockBegin bool
	// onBegin, onComplete, onCancel and onAck run when the call is made, before it takes effect and outside the lock.
	onBegin    func()
	onComplete func()
	onCancel   func()
	onAck      func()
	// afterBegin runs once a begin call has answered, outside the lock: a call which lands after the script does, such
	// as the owner's completion of a call which the begin found in flight.
	afterBegin func()
	// beforeRead runs when a poll of the shared tier starts, before it reads, outside the lock: a hook which blocks holds
	// the wait which polls there.
	beforeRead func()
	// wakes are the calls of Wake, in order, and wakeErr is the failure of every Wake call.
	wakes   []wakeCall
	wakeErr error
	// blockWake makes every wake call wait until wakeRelease is closed or its context is done, as an unanswered Redis does.
	// It gives up after a second, so that a wake which ignores its context fails the test instead of hanging it.
	blockWake   bool
	wakeRelease chan struct{}
	// wakeCh carries the keys which deliverWake sends to the wake watcher, and emptyCh the empty reads which emptyRead
	// makes it see.
	wakeCh  chan Key
	emptyCh chan struct{}
	// watchStarts is the number of times WatchWakes was entered.
	watchStarts int
}

// newFakeBackend returns a connected backend without records.
func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		entries:   make(map[Key]*fakeEntry),
		connected: true,
		wakeCh:    make(chan Key, 64),
		emptyCh:   make(chan struct{}),
	}
}

// deliverWake sends a wake of key to the wake watcher of the fake node, which reads it when it runs.
func (f *fakeBackend) deliverWake(key Key) {
	f.wakeCh <- key
}

// emptyRead makes the wake watcher of the fake node see an empty read, which lets it stop when it is idle. It gives up
// after a second when no watcher runs, so that a test which expects none does not hang.
func (f *fakeBackend) emptyRead() {
	select {
	case f.emptyCh <- struct{}{}:
	case <-time.After(time.Second):
	}
}

// watchStartCount returns how many times WatchWakes was entered.
func (f *fakeBackend) watchStartCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchStarts
}

// wakeCalls returns the calls of Wake so far.
func (f *fakeBackend) wakeCalls() []wakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wakeCall(nil), f.wakes...)
}

// enter logs a call and returns the failure of the backend.
func (f *fakeBackend) enter(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	return f.fail
}

// setFail makes every later call fail with err, or succeed when err is nil.
func (f *fakeBackend) setFail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = err
}

// callLog returns the names of the calls made so far.
func (f *fakeBackend) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// resetCalls forgets the names of the calls made so far.
func (f *fakeBackend) resetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

// ackBatches returns the batches of acks flushed so far.
func (f *fakeBackend) ackBatches() [][]ScopeAcks {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]ScopeAcks(nil), f.acks...)
}

// put sets the record of key.
func (f *fakeBackend) put(key Key, state byte, fingerprint [32]byte, owner string, rec Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[key] = &fakeEntry{state: state, fingerprint: fingerprint, owner: owner, rec: rec}
}

// finish makes the in-flight record of key a done record with rec, as the owner completes it.
func (f *fakeBackend) finish(key Key, rec Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[key]
	e.state = 'd'
	e.owner = ""
	e.waiters = nil
	e.rec = rec
}

// remove deletes the record of key, as its expiry does.
func (f *fakeBackend) remove(key Key) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, key)
}

// entry returns a copy of the record of key.
func (f *fakeBackend) entry(key Key) (fakeEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, found := f.entries[key]
	if !found {
		return fakeEntry{}, false
	}
	return *e, true
}

// Begin implements Backend with the semantics of beginScript.
func (f *fakeBackend) Begin(ctx context.Context, key Key, fingerprint [32]byte, nodeID string, inflightTTL time.Duration, maxWaiters int) (BeginResult, error) {
	if f.blockBegin {
		select {
		case <-ctx.Done():
			return BeginResult{}, ctx.Err()
		case <-time.After(time.Second):
			return BeginResult{}, errors.New("the begin was not cut by its context")
		}
	}
	res, err := f.begin(ctx, key, fingerprint, nodeID, inflightTTL, maxWaiters)
	if err == nil && f.afterBegin != nil {
		f.afterBegin()
	}
	return res, err
}

// begin is Begin without the afterBegin hook, which runs once the script has answered.
func (f *fakeBackend) begin(_ context.Context, key Key, fingerprint [32]byte, nodeID string, _ time.Duration, maxWaiters int) (BeginResult, error) {
	if err := f.enter("begin"); err != nil {
		return BeginResult{}, err
	}
	if f.onBegin != nil {
		f.onBegin()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[key]
	if e == nil {
		f.entries[key] = &fakeEntry{state: 'i', fingerprint: fingerprint, owner: nodeID}
		return BeginResult{State: BeginMiss}, nil
	}
	if e.state == 'c' {
		return BeginResult{State: BeginCancelled, Record: e.rec}, nil
	}
	if e.fingerprint != fingerprint {
		return BeginResult{State: BeginMismatch}, nil
	}
	if e.state == 'i' {
		if maxWaiters >= 1 && nodeID != e.owner && !e.addWaiter(nodeID, maxWaiters) {
			return BeginResult{State: BeginInFlight, Owner: e.owner, WaitersFull: true}, nil
		}
		return BeginResult{State: BeginInFlight, Owner: e.owner}, nil
	}
	return BeginResult{State: BeginDone, Record: e.rec}, nil
}

// Complete implements Backend with the semantics of completeScript: only the owner completes its record.
func (f *fakeBackend) Complete(ctx context.Context, key Key, nodeID string, rec Record, _ string, _ time.Duration) (CompleteResult, error) {
	if err := f.enter("complete"); err != nil {
		return CompleteResult{}, err
	}
	if f.onComplete != nil {
		f.onComplete()
	}
	if f.blockComplete {
		<-ctx.Done()
		return CompleteResult{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.completeFailures > 0 {
		f.completeFailures--
		return CompleteResult{}, errors.New("write timed out")
	}
	result := f.applyComplete(key, nodeID, rec)
	if f.lostReplies > 0 {
		f.lostReplies--
		return CompleteResult{}, errors.New("reply lost")
	}
	return result, nil
}

// applyComplete writes rec for key when nodeID owns the in-flight record of key, and returns the waiters of that
// record, as completeScript does. Caller holds mu.
func (f *fakeBackend) applyComplete(key Key, nodeID string, rec Record) CompleteResult {
	e := f.entries[key]
	if e == nil || e.state != 'i' || e.owner != nodeID {
		return CompleteResult{}
	}
	waiters := e.waiters
	if rec.Abandoned {
		delete(f.entries, key)
		return CompleteResult{Owned: true, Waiters: waiters}
	}
	e.state = 'd'
	e.owner = ""
	e.waiters = nil
	if rec.Uncached {
		e.rec = Record{Uncached: true}
	} else {
		e.rec = rec
	}
	return CompleteResult{Owned: true, Waiters: waiters}
}

// Wake implements Backend: it records the call, and fails with wakeErr when that is set.
func (f *fakeBackend) Wake(ctx context.Context, key Key, nodes []string) (int, error) {
	if err := f.enter("wake"); err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.wakes = append(f.wakes, wakeCall{key: key, nodes: append([]string(nil), nodes...)})
	wakeErr := f.wakeErr
	blocked := f.blockWake
	f.mu.Unlock()
	if blocked {
		select {
		case <-f.wakeRelease:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 0, errors.New("the wake was not cut by its context")
		}
	}
	if wakeErr != nil {
		return 0, wakeErr
	}
	return len(nodes), nil
}

// WatchWakes implements Backend: the fake watcher passes the delivered wakes to wake, and returns when idle reports
// true at an empty read, or when ctx is done.
func (f *fakeBackend) WatchWakes(ctx context.Context, _ string, wake func(Key), idle func() bool) error {
	f.mu.Lock()
	f.watchStarts++
	f.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return nil
		case key := <-f.wakeCh:
			wake(key)
		case <-f.emptyCh:
			if idle() {
				return nil
			}
		}
	}
}

// Cancel implements Backend with the semantics of cancelScript.
func (f *fakeBackend) Cancel(_ context.Context, key Key, failure *centrifuge.Error, _ time.Duration) (CancelResult, error) {
	if err := f.enter("cancel"); err != nil {
		return CancelResult{}, err
	}
	if f.onCancel != nil {
		f.onCancel()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[key]
	if e == nil {
		f.entries[key] = &fakeEntry{state: 'c', rec: Record{Err: failure}}
		return CancelResult{State: CancelTombstone}, nil
	}
	if e.state == 'i' {
		return CancelResult{State: CancelInFlight, Owner: e.owner}, nil
	}
	return CancelResult{State: CancelDone}, nil
}

// Poll implements Backend with the semantics of HMGET of the watcher.
func (f *fakeBackend) Poll(_ context.Context, key Key) (PollResult, error) {
	if err := f.enter("poll"); err != nil {
		return PollResult{}, err
	}
	if f.beforeRead != nil {
		f.beforeRead()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[key]
	if e == nil {
		return PollResult{State: PollMissing}, nil
	}
	if e.state == 'i' {
		return PollResult{State: PollInFlight}, nil
	}
	if e.state == 'c' {
		return PollResult{State: PollCancelled, Record: e.rec}, nil
	}
	return PollResult{State: PollDone, Record: e.rec}, nil
}

// Ack implements Backend: it deletes the done records and tombstones of the acks, and keeps the batch.
func (f *fakeBackend) Ack(ctx context.Context, acks []ScopeAcks) error {
	if err := f.enter("ack"); err != nil {
		return err
	}
	if f.onAck != nil {
		f.onAck()
	}
	if f.blockAck {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return errors.New("the ack was not cut by its context")
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, acks)
	for _, scope := range acks {
		for _, msgID := range scope.MsgIDs {
			key := Key{Scope: scope.Scope, MsgID: msgID}
			if e := f.entries[key]; e != nil && e.state == 'd' {
				delete(f.entries, key)
			}
		}
	}
	return nil
}

// Connected implements Backend.
func (f *fakeBackend) Connected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

// notifyCall is a cancel notification of the fake node.
type notifyCall struct {
	nodeID string
	key    Key
}

// sharedFixture is a Shared store over a node-local Memory and a fake backend, as node-a.
type sharedFixture struct {
	local   *Memory
	backend *fakeBackend
	shared  *Shared
	// notified are the cancel notifications of the fixture. The notifier runs in a goroutine of its own, so they are
	// read through notifiedCalls.
	notified   []notifyCall
	notifiedMu sync.Mutex
	dropped    int
	lost       int
	// mu guards polls, woke and ends, which the waits and the wake writes of the fixture count.
	mu sync.Mutex
	// polls is the number of reads of the shared tier which the waits of the fixture made, woke the wakes which the owner
	// wrote and which failed, and ends the number of waits by how they ended.
	polls int
	woke  [2]int
	ends  map[string]int
}

// notifiedCalls returns the cancel notifications of the fixture so far.
func (fx *sharedFixture) notifiedCalls() []notifyCall {
	fx.notifiedMu.Lock()
	defer fx.notifiedMu.Unlock()
	return append([]notifyCall(nil), fx.notified...)
}

// requireNotifiedEventually waits until the cancel notifications of the fixture are want, and fails the test when they
// are not, since the notifier runs in a goroutine of its own.
func requireNotifiedEventually(t *testing.T, fx *sharedFixture, want []notifyCall) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(fx.notifiedCalls()) >= len(want)
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, want, fx.notifiedCalls())
}

// Woke implements SharedObserver.
func (fx *sharedFixture) Woke(written int, failed int) {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	fx.woke[0] += written
	fx.woke[1] += failed
}

// AckDropped implements SharedObserver.
func (fx *sharedFixture) AckDropped() {
	fx.dropped++
}

// LostOwnership implements SharedObserver.
func (fx *sharedFixture) LostOwnership() {
	fx.lost++
}

// WatchRead implements SharedObserver.
func (fx *sharedFixture) WatchRead() {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	fx.polls++
}

// WatchEnded implements SharedObserver.
func (fx *sharedFixture) WatchEnded(end string) {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	if fx.ends == nil {
		fx.ends = make(map[string]int)
	}
	fx.ends[end]++
}

// pollCount returns the polls which the waits made so far.
func (fx *sharedFixture) pollCount() int {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.polls
}

// endCount returns the waits which ended as end so far.
func (fx *sharedFixture) endCount(end string) int {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.ends[end]
}

// newSharedFixture returns a shared store of node-a with the given failure mode, and the wakeup off.
func newSharedFixture(t *testing.T, failOpen bool) *sharedFixture {
	t.Helper()
	return newSharedFixtureWith(t, failOpen, false, nil)
}

// newSharedFixtureWithNotifier returns a shared store of node-a whose cancel notifications are sent by notifier.
func newSharedFixtureWithNotifier(t *testing.T, notifier func(nodeID string, key Key)) *sharedFixture {
	t.Helper()
	return newSharedFixtureWith(t, false, false, notifier)
}

// newSharedFixtureWakeup returns a shared store of node-a with the wakeup on. Its check and its idle stop are an hour
// away unless a test sets them. Its Run runs until the test ends, as it does in production, so the wake watcher runs
// when the node has remote waits.
func newSharedFixtureWakeup(t *testing.T) *sharedFixture {
	t.Helper()
	fx := newSharedFixtureWith(t, false, true, nil)
	fx.shared.checkDelay = time.Hour
	fx.shared.watchIdleAfter = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = fx.shared.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return fx
}

// newSharedFixtureWith returns a shared store of node-a. With wakeup set, its waiters are registered in the shared tier
// and woken by the wakes of the owner. Its cancel notifications go to notifier, or are recorded in the fixture when
// notifier is nil.
func newSharedFixtureWith(t *testing.T, failOpen bool, wakeup bool, notifier func(nodeID string, key Key)) *sharedFixture {
	t.Helper()
	local, _ := newTestMemory(1024)
	fx := &sharedFixture{local: local, backend: newFakeBackend()}
	if notifier == nil {
		notifier = func(nodeID string, key Key) {
			fx.notifiedMu.Lock()
			defer fx.notifiedMu.Unlock()
			fx.notified = append(fx.notified, notifyCall{nodeID: nodeID, key: key})
		}
	}
	fx.shared = NewShared(SharedOptions{
		Local:         local,
		Backend:       fx.backend,
		NodeID:        "node-a",
		TTL:           time.Minute,
		MaxReplyBytes: 1024,
		FailOpen:      failOpen,
		Notifier:      notifier,
		Observer:      fx,
		PollInitial:   time.Millisecond,
		PollMax:       2 * time.Millisecond,
		AckFlushEvery: time.Hour,
		Wakeup:        wakeup,
	})
	return fx
}

// sharedKey is the key of the tests, of a session scope of user u1.
var sharedKey = Key{Scope: testScope("u1", "a"), MsgID: 7}

var (
	sharedFingerprintA = Fingerprint("users.get", []byte("x"))
	sharedFingerprintB = Fingerprint("users.get", []byte("y"))
)

// waitDone waits for the flight to finish, and fails the test when it does not.
func waitDone(t *testing.T, flight *Flight) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-flight.Done():
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond)
}

func TestSharedImplementsStore(t *testing.T) {
	var store Store = &Shared{}
	require.NotNil(t, store)
}

func TestSharedLocalDoneHitMakesNoBackendCall(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))

	fx.backend.resetCalls()
	lookup, err = fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
	require.Empty(t, fx.backend.callLog())
}

func TestSharedLocalUncachedHitPollsBackendWithoutCaching(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	require.Equal(t, OutcomeMiss, testBegin(t, fx.local, sharedKey, owner).Outcome)
	require.NoError(t, fx.local.Complete(sharedKey, owner, Record{Reply: make([]byte, 2000)}))
	fx.backend.put(sharedKey, 'd', sharedFingerprintA, "", Record{Reply: []byte("from-redis")})

	lookup, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("from-redis"), lookup.Record.Reply)
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "poll"))

	// The local record stays uncached: the reply of Redis is not kept on this node.
	lookup, err = fx.local.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.True(t, lookup.Record.Uncached)
}

// countCalls returns how many times name is in calls.
func countCalls(calls []string, name string) int {
	n := 0
	for _, call := range calls {
		if call == name {
			n++
		}
	}
	return n
}

func TestSharedMissWritesRedisBeforeMemory(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	var localFlightRunning bool
	fx.backend.onComplete = func() {
		localFlightRunning = fx.local.flightOf(sharedKey) != nil
	}
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))

	// Redis was written while the local flight was still registered, so the local record came second.
	require.True(t, localFlightRunning)
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, byte('d'), entry.state)
	require.Equal(t, []byte("ok"), entry.rec.Reply)
	localLookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, localLookup.Outcome)
}

func TestSharedBackendDoneOnLocalMissIsCachedAndWakesDuplicates(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'd', sharedFingerprintA, "", Record{Reply: []byte("from-backend")})
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	var duplicateLookup Lookup
	fx.backend.onBegin = func() {
		var err error
		duplicateLookup, err = fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
		require.NoError(t, err)
	}

	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("from-backend"), lookup.Record.Reply)
	require.Equal(t, OutcomeInFlight, duplicateLookup.Outcome)
	require.Same(t, owner, duplicateLookup.Flight)
	waitDone(t, owner)
	require.Equal(t, []byte("from-backend"), owner.Record().Reply)

	// Cached: the next duplicate is a local hit.
	fx.backend.resetCalls()
	lookup, err = fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Empty(t, fx.backend.callLog())
}

func TestSharedMismatchAbandonsLocalFlight(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'd', sharedFingerprintB, "", Record{Reply: []byte("other")})

	lookup, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeMismatch, lookup.Outcome)
	require.Nil(t, lookup.Flight)
	require.True(t, testIsMiss(t, fx.local, sharedKey))
}

func TestSharedRemoteInFlightWaitsForAnswer(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	require.Same(t, owner, lookup.Flight)

	// A second duplicate on this node joins the same local flight.
	duplicate := NewCallFlight(sharedFingerprintA, time.Second)
	duplicateLookup, err := fx.shared.Begin(sharedKey, duplicate)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, duplicateLookup.Outcome)
	require.Same(t, owner, duplicateLookup.Flight)

	// Node-b completes the call; the watcher completes the local flight, which wakes both duplicates.
	fx.backend.finish(sharedKey, Record{Reply: []byte("answer")})
	waitDone(t, owner)
	require.Equal(t, []byte("answer"), owner.Record().Reply)

	lookup, err = fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
}

func TestSharedCancelBeforeRemoteMarkIsSentToOwner(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	// The cancel arrives within the round trip of the begin: the flight is registered, and has no call to cancel yet.
	fx.backend.onBegin = func() {
		require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	}

	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)
	requireNotifiedEventually(t, fx, []notifyCall{{nodeID: "node-b", key: sharedKey}})

	fx.backend.finish(sharedKey, Record{Reply: []byte("answer")})
	waitDone(t, owner)
}

func TestSharedRestoredTombstoneAnswersAnyFingerprint(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'c', [32]byte{}, "", Record{Err: testCancelled})

	lookup, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)

	// The tombstone is cached on this node without the fingerprint of the first caller, so a duplicate with another
	// fingerprint is answered with it too, and not refused as a reuse of the msg_id.
	lookup = testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintB))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
	checkInvariants(t, fx.local)
}

// A tombstone which the watcher of a remote flight polls is kept without the fingerprint of the flight, so a duplicate
// on the watching node with another fingerprint gets the cancelled error, and not msg_id_reused.
func TestSharedRemoteTombstoneAnswersAnyFingerprint(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

	// The owner's record is replaced by a tombstone, as a cancel on a node without the call writes it.
	fx.backend.put(sharedKey, 'c', [32]byte{}, "", Record{Err: testCancelled})
	waitDone(t, owner)
	require.Same(t, testCancelled, owner.Record().Err)
	require.Equal(t, [32]byte{}, owner.Record().Fingerprint)

	lookup = testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintB))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
	checkInvariants(t, fx.local)
}

func TestSharedRemoteKeyVanishingAbandonsFlight(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	owner := NewCallFlight(sharedFingerprintA, time.Second)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

	// The owner died and its record expired.
	fx.backend.remove(sharedKey)
	waitDone(t, owner)
	require.True(t, owner.Record().Abandoned)
	require.False(t, owner.Record().NotRun)
}

func TestSharedBeginFailureFailsClosedMarksNotRun(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.setFail(errors.New("redis is down"))
	owner := NewFlightWithFingerprint(sharedFingerprintA)

	_, err := fx.shared.Begin(sharedKey, owner)
	require.Error(t, err)
	require.True(t, owner.Record().Abandoned)
	require.True(t, owner.Record().NotRun)
}

func TestSharedRemoteWatcherDeadlineAbandonsFlight(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.inflightGrace = 20 * time.Millisecond
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})
	owner := NewCallFlight(sharedFingerprintA, 0)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeInFlightRemote, lookup.Outcome)

	// Node-b never completes the call: the watcher gives up at the in-flight TTL.
	waitDone(t, owner)
	require.True(t, owner.Record().Abandoned)
}

func TestSharedFailOpenDegradesToLocalDedup(t *testing.T) {
	fx := newSharedFixture(t, true)
	fx.backend.setFail(errors.New("redis is down"))
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	lookup, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.Equal(t, OutcomeMiss, lookup.Outcome)
	require.True(t, owner.localOnly)

	fx.backend.resetCalls()
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Empty(t, fx.backend.callLog(), "a local-only flight does not write Redis")

	// The record is kept on this node, so a duplicate is a local hit.
	lookup, err = fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.NoError(t, err)
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
}

func TestSharedFailClosedRefusesCall(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.setFail(errors.New("redis is down"))
	_, err := fx.shared.Begin(sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Error(t, err)

	// The flight is abandoned: the next call is a miss again.
	require.True(t, testIsMiss(t, fx.local, sharedKey))
}

func TestSharedCancelOfLocalFlightMakesNoBackendCall(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner.Attach(cancel)

	fx.backend.resetCalls()
	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Empty(t, fx.backend.callLog())
}

func TestSharedCancelTombstoneIsCachedLocally(t *testing.T) {
	fx := newSharedFixture(t, false)
	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.Equal(t, []string{"cancel"}, fx.backend.callLog())

	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err)
}

// A cancel without an error is refused before the shared tier: the record would answer a later begin with an empty
// success, and the local store would answer it the same way.
func TestSharedCancelWithoutFailureIsRefused(t *testing.T) {
	fx := newSharedFixture(t, false)

	require.Error(t, fx.shared.Cancel(sharedKey, nil))
	require.Zero(t, countCalls(fx.backend.callLog(), "cancel"))
	_, found := fx.backend.entry(sharedKey)
	require.False(t, found)
	require.True(t, testIsMiss(t, fx.local, sharedKey))
}

func TestSharedCancelInFlightElsewhereNotifiesOwner(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	requireNotifiedEventually(t, fx, []notifyCall{{nodeID: "node-b", key: sharedKey}})
	require.True(t, testIsMiss(t, fx.local, sharedKey), "nothing is recorded locally")
}

func TestSharedCancelOwnedByThisNodeWithoutFlightDoesNothing(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-a", Record{})

	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.Empty(t, fx.notifiedCalls())
	require.True(t, testIsMiss(t, fx.local, sharedKey))
}

func TestSharedCancelOfRefusedScopeMakesNoBackendCall(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	options := testMemoryOptions(clock, 1024)
	options.MaxScopesPerUser = 1
	local := NewMemory(options)
	backend := newFakeBackend()
	shared := NewShared(SharedOptions{
		Local:         local,
		Backend:       backend,
		NodeID:        "node-a",
		TTL:           time.Minute,
		MaxReplyBytes: 1024,
	})
	testAnswer(t, local, Key{Scope: testScope("u1", "a"), MsgID: 1}, "ok")

	refusedKey := Key{Scope: testScope("u1", "b"), MsgID: 2}
	err := shared.Cancel(refusedKey, testCancelled)
	require.Equal(t, RefusalUser, testRefused(t, err))
	require.Empty(t, backend.callLog(), "a refused scope is not written to the shared tier")
	_, found := backend.entry(refusedKey)
	require.False(t, found)
	require.Equal(t, 1, testEntries(local))
}

// Under fail_open a cancel which the shared tier fails is not delivered, and nothing is recorded locally: the call may
// run to its end on another node, and a local tombstone would answer its retransmit with the cancelled error.
func TestSharedCancelFailureFollowsOnStoreError(t *testing.T) {
	fx := newSharedFixture(t, true)
	fx.backend.setFail(errors.New("redis is down"))
	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.Empty(t, fx.notifiedCalls())
	require.True(t, testIsMiss(t, fx.local, sharedKey), "nothing is recorded locally")

	closed := newSharedFixture(t, false)
	closed.backend.setFail(errors.New("redis is down"))
	require.Error(t, closed.shared.Cancel(sharedKey, testCancelled))
}

func TestSharedCompleteOverReplyLimitStoresUncached(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: make([]byte, 2000)}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.True(t, entry.rec.Uncached)
	require.Nil(t, entry.rec.Reply)
}

func TestSharedCompleteOverLimitErrorStoresUncached(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	failure := &centrifuge.Error{Code: 107, Message: strings.Repeat("e", 2048)}
	require.Greater(t, len(encodeError(failure)), 2048)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Err: failure}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.True(t, entry.rec.Uncached)
	require.Nil(t, entry.rec.Err)
}

func TestSharedCompleteErrorUnderLimitIsKept(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Err: testCancelled}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.False(t, entry.rec.Uncached)
	require.Equal(t, testCancelled.Message, entry.rec.Err.Message)
}

func TestSharedCompleteRetriesFailedWrite(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.completeFailures = 1

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, sharedCompleteAttempts, countCalls(fx.backend.callLog(), "complete"))
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, byte('d'), entry.state)
	require.Equal(t, []byte("ok"), entry.rec.Reply)
}

func TestSharedCompleteGivesUpAndKeepsRecordLocally(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	fx.backend.completeFailures = 100

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, sharedCompleteAttempts, countCalls(fx.backend.callLog(), "complete"))
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, byte('i'), entry.state, "the shared tier kept no record")

	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Equal(t, []byte("ok"), lookup.Record.Reply)
}

func TestSharedCompleteIsCutAtItsBudget(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.inflightGrace = 400 * time.Millisecond
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// Redis accepts the write and never answers: the attempt is cut at the in-flight expiry of the call, which comes
	// before the IO timeout, and no retry starts after it.
	fx.backend.blockComplete = true

	start := time.Now()
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Less(t, time.Since(start), time.Second, "cut at the expiry, not at the IO timeout of 4 seconds")
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "complete"))
}

func TestSharedCompleteReplyReturnsWithinIOTimeout(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ioTimeout = 100 * time.Millisecond
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// Redis accepts the connection and never answers the write: the reply is not held beyond one IO timeout for all
	// tries together.
	fx.backend.blockComplete = true

	start := time.Now()
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Less(t, time.Since(start), 3*fx.shared.ioTimeout)
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "complete"), "the first write uses the whole budget")
}

func TestSharedCompleteRetryFitsInIOTimeout(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ioTimeout = 100 * time.Millisecond
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// The first write fails at once, and the retry is blackholed: the retry ends at the budget, and no third try is made.
	fx.backend.completeFailures = 1
	var writes int
	fx.backend.onComplete = func() {
		writes++
		if writes == 2 {
			fx.backend.blockComplete = true
		}
	}

	start := time.Now()
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Less(t, time.Since(start), 3*fx.shared.ioTimeout)
	require.Equal(t, sharedCompleteAttempts, countCalls(fx.backend.callLog(), "complete"))
}

func TestSharedCompleteStopsAtInflightDeadline(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// The in-flight record of the call expired at the grace from its begin, which has passed: a retry could only land
	// after it, so none is made.
	owner.begun = time.Now().Add(-fx.shared.inflightGrace).UnixNano()
	fx.backend.completeFailures = 100

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "complete"))
}

func TestSharedCompleteLostReplyIsNotAnError(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// The first write lands, and its reply is lost. The retry finds the record done: the answer is in the shared tier.
	fx.backend.lostReplies = 1

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, 2, countCalls(fx.backend.callLog(), "complete"))
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, byte('d'), entry.state)
	require.Equal(t, []byte("ok"), entry.rec.Reply)
	require.Zero(t, fx.lost, "a retried write which finds the record done is not a lost ownership")
}

func TestSharedCompleteLostOwnershipIsCountedOnce(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// The in-flight record now belongs to another node, as after an expiry and a retransmit there.
	fx.backend.entries[sharedKey].owner = "node-b"

	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte("ok")}))
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "complete"))
	require.Equal(t, 1, fx.lost)
	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.Equal(t, "node-b", entry.owner)
	require.Equal(t, byte('i'), entry.state)
}

func TestSharedCancelReachesCallThatBeganDuringCancel(t *testing.T) {
	fx := newSharedFixture(t, false)
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The call begins after the cancel found no flight, and before the shared tier answers it: the record is owned by
	// this node, and the cancel must reach the flight which the call registered.
	fx.backend.onCancel = func() {
		lookup, err := fx.shared.Begin(sharedKey, owner)
		require.NoError(t, err)
		require.Equal(t, OutcomeMiss, lookup.Outcome)
		owner.Attach(cancel)
	}

	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Empty(t, fx.notifiedCalls(), "the owner is this node, which is not notified")
}

func TestSharedCancelTombstoneReplacesCachedDoneRecord(t *testing.T) {
	fx := newSharedFixture(t, false)
	// This node answered the call, and its record was acked on another node, so the shared tier holds nothing.
	testAnswer(t, fx.local, sharedKey, "ok")

	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.Equal(t, []string{"cancel"}, fx.backend.callLog())
	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.Same(t, testCancelled, lookup.Record.Err, "the answer of this node is the one of the shared tier")
	checkInvariants(t, fx.local)
}

func TestSharedReplyOverLimitWithErrorIsUncachedInBothTiers(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.maxReplyBytes = 512
	fx.local.maxReplyBytes = 512
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	// The error is small, and the reply is over the limit: the record is kept without both, in each tier.
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Reply: []byte(strings.Repeat("r", 600)), Err: testCancelled}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.True(t, entry.rec.Uncached)
	require.Nil(t, entry.rec.Err)
	require.Empty(t, entry.rec.Reply)

	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.True(t, lookup.Record.Uncached)
	require.Nil(t, lookup.Record.Err)
	checkInvariants(t, fx.local)
}

func TestSharedErrorOverReplyLimitIsUncachedInBothTiers(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.maxReplyBytes = 512
	fx.local.maxReplyBytes = 512
	owner := NewFlightWithFingerprint(sharedFingerprintA)
	_, err := fx.shared.Begin(sharedKey, owner)
	require.NoError(t, err)
	failure := &centrifuge.Error{Code: 107, Message: strings.Repeat("e", 600)}
	require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Err: failure}))

	entry, found := fx.backend.entry(sharedKey)
	require.True(t, found)
	require.True(t, entry.rec.Uncached)
	require.Nil(t, entry.rec.Err)

	// This node holds the same answer, uncached, so a retransmit here and on another node get the same result.
	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)
	require.True(t, lookup.Record.Uncached)
	require.Nil(t, lookup.Record.Err)
	checkInvariants(t, fx.local)
}

// An error is kept by both tiers by the same rule, its encoded form against the limit: at the limit, one byte over it,
// and a message under the limit whose quote characters make the encoded form over it.
func TestSharedErrorBoundaryIsTheSameInBothTiers(t *testing.T) {
	limit := len(encodeError(&centrifuge.Error{Code: 108, Message: strings.Repeat("e", 512)}))
	tests := []struct {
		name     string
		message  string
		uncached bool
	}{
		{name: "encoded form at the limit", message: strings.Repeat("e", 512), uncached: false},
		{name: "encoded form one byte over", message: strings.Repeat("e", 513), uncached: true},
		{name: "escaped form over, message under", message: strings.Repeat(`"`, 300), uncached: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newSharedFixture(t, false)
			fx.shared.maxReplyBytes = limit
			fx.local.maxReplyBytes = limit
			owner := NewFlightWithFingerprint(sharedFingerprintA)
			_, err := fx.shared.Begin(sharedKey, owner)
			require.NoError(t, err)
			failure := &centrifuge.Error{Code: 108, Message: tc.message}
			require.NoError(t, fx.shared.Complete(sharedKey, owner, Record{Err: failure}))

			entry, found := fx.backend.entry(sharedKey)
			require.True(t, found)
			require.Equal(t, tc.uncached, entry.rec.Uncached, "the shared tier")

			lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
			require.Equal(t, OutcomeDone, lookup.Outcome)
			require.Equal(t, tc.uncached, lookup.Record.Uncached, "the node-local store")
			checkInvariants(t, fx.local)
		})
	}
}

func TestSharedAckRemovesLocallyAtOnceAndFlushesGroupedByScope(t *testing.T) {
	fx := newSharedFixture(t, false)
	scopeA := testScope("u1", "a")
	scopeB := testScope("u2", "b")
	for _, key := range []Key{{Scope: scopeA, MsgID: 1}, {Scope: scopeA, MsgID: 2}, {Scope: scopeB, MsgID: 1}} {
		testAnswer(t, fx.local, key, "ok")
		fx.backend.put(key, 'd', sharedFingerprintA, "", Record{Reply: []byte("ok")})
	}

	require.NoError(t, fx.shared.Ack(scopeA, []uint64{1, 2}))
	require.NoError(t, fx.shared.Ack(scopeB, []uint64{1}))
	require.True(t, testIsMiss(t, fx.local, Key{Scope: scopeA, MsgID: 1}))
	_, stillInRedis := fx.backend.entry(Key{Scope: scopeA, MsgID: 1})
	require.True(t, stillInRedis, "the ack is not sent before the flush")

	// Shutdown flushes the queue once more: one batch, grouped by scope.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fx.shared.Run(ctx) }()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	batches := fx.backend.ackBatches()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
	byScope := map[string][]uint64{}
	for _, acks := range batches[0] {
		byScope[acks.Scope] = acks.MsgIDs
	}
	require.ElementsMatch(t, []uint64{1, 2}, byScope[scopeA])
	require.ElementsMatch(t, []uint64{1}, byScope[scopeB])
	_, found := fx.backend.entry(Key{Scope: scopeA, MsgID: 1})
	require.False(t, found)
}

func TestSharedRunFlushesOnShutdown(t *testing.T) {
	fx := newSharedFixture(t, false)
	scope := testScope("u1", "a")
	for msgID := uint64(1); msgID <= 500; msgID++ {
		testAnswer(t, fx.local, Key{Scope: scope, MsgID: msgID}, "ok")
	}
	msgIDs := make([]uint64, 0, 500)
	for msgID := uint64(1); msgID <= 500; msgID++ {
		msgIDs = append(msgIDs, msgID)
	}
	require.NoError(t, fx.shared.Ack(scope, msgIDs))
	require.Empty(t, fx.backend.ackBatches(), "nothing is flushed before Run")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fx.shared.Run(ctx) }()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	var flushed int
	for _, batch := range fx.backend.ackBatches() {
		for _, acks := range batch {
			flushed += len(acks.MsgIDs)
		}
	}
	require.Equal(t, 500, flushed, "the acks queued before shutdown reach the shared tier")
}

func TestSharedRunFlushesQueueInBatchesOnShutdown(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackBatch = 10
	scope := testScope("u1", "a")
	msgIDs := make([]uint64, 0, 23)
	for msgID := uint64(1); msgID <= 23; msgID++ {
		testAnswer(t, fx.local, Key{Scope: scope, MsgID: msgID}, "ok")
		msgIDs = append(msgIDs, msgID)
	}
	require.NoError(t, fx.shared.Ack(scope, msgIDs))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fx.shared.Run(ctx) }()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	var flushed int
	for _, batch := range fx.backend.ackBatches() {
		var keys int
		for _, acks := range batch {
			keys += len(acks.MsgIDs)
		}
		require.LessOrEqual(t, keys, 10, "a shutdown flush is cut in batches of AckBatch")
		flushed += keys
	}
	require.Equal(t, 23, flushed, "every queued ack reaches the shared tier")
}

// A drain whose budget is spent before it starts sends nothing, and counts every queued ack as dropped, so the drop
// metric does not undercount the acks which a shutdown leaves unsent.
func TestSharedDrainCountsQueueLeftWhenBudgetIsSpent(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ioTimeout = -time.Second
	scope := testScope("u1", "a")
	msgIDs := []uint64{1, 2, 3, 4, 5}
	require.NoError(t, fx.shared.Ack(scope, msgIDs))

	fx.shared.drainAcks([]Key{{Scope: scope, MsgID: 6}})
	require.Equal(t, 0, countCalls(fx.backend.callLog(), "ack"), "no write starts after the budget is spent")
	require.Equal(t, 6, fx.dropped, "the batch and the queue are counted as dropped")
}

// A drain whose flushes fail counts the keys of each failed flush as dropped, since a shutdown does not retry them.
func TestSharedDrainCountsKeysOfFailedFlush(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackBatch = 2
	scope := testScope("u1", "a")
	require.NoError(t, fx.shared.Ack(scope, []uint64{1, 2, 3}))
	fx.backend.setFail(errors.New("redis is down"))

	fx.shared.drainAcks(nil)
	require.Equal(t, 3, fx.dropped, "the keys of the failed flushes are counted, the batch of two and the one left")
}

// The acks which Ack enqueues while a drain flushes are taken by the drain while its budget lasts, so they reach the
// shared tier too, and none is dropped.
func TestSharedDrainTakesAcksEnqueuedDuringDrain(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackBatch = 2
	scope := testScope("u1", "a")
	require.NoError(t, fx.shared.Ack(scope, []uint64{1, 2}))
	var enqueued bool
	fx.backend.onAck = func() {
		if !enqueued {
			enqueued = true
			require.NoError(t, fx.shared.Ack(scope, []uint64{3, 4}))
		}
	}

	fx.shared.drainAcks(nil)
	var flushed int
	for _, batch := range fx.backend.ackBatches() {
		for _, acks := range batch {
			flushed += len(acks.MsgIDs)
		}
	}
	require.Equal(t, 4, flushed, "the acks enqueued during the drain reach the shared tier")
	require.Equal(t, 0, fx.dropped)
}

// A shutdown whose Redis does not answer the acks is held for one IO timeout in all, not one per batch: the first batch
// is cut by the budget, and the acks which are left are counted as dropped. The keys of the cut batch are counted too,
// since its flush failed, so none of the 50 acks reaches the shared tier and all 50 are dropped.
func TestSharedDrainSharesOneIOTimeout(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackBatch = 10
	fx.shared.ioTimeout = 100 * time.Millisecond
	fx.backend.blockAck = true
	scope := testScope("u1", "a")
	msgIDs := make([]uint64, 0, 50)
	for msgID := uint64(1); msgID <= 50; msgID++ {
		msgIDs = append(msgIDs, msgID)
	}
	require.NoError(t, fx.shared.Ack(scope, msgIDs))

	start := time.Now()
	fx.shared.drainAcks(nil)
	require.Less(t, time.Since(start), 500*time.Millisecond, "the drain shares one IO timeout of 100ms")
	require.Equal(t, 1, countCalls(fx.backend.callLog(), "ack"), "the first batch is cut, the rest is not sent")
	require.Equal(t, 50, fx.dropped, "the keys of the cut batch and the acks which the budget leaves unsent are counted as dropped")
}

func TestSharedAckFlushesOnTicker(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.shared.ackFlushEvery = time.Millisecond
	scope := testScope("u1", "a")
	testAnswer(t, fx.local, Key{Scope: scope, MsgID: 1}, "ok")
	fx.backend.put(Key{Scope: scope, MsgID: 1}, 'd', sharedFingerprintA, "", Record{Reply: []byte("ok")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fx.shared.Run(ctx) }()
	require.NoError(t, fx.shared.Ack(scope, []uint64{1}))
	require.Eventually(t, func() bool {
		_, found := fx.backend.entry(Key{Scope: scope, MsgID: 1})
		return !found
	}, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// droppedAckObserver counts the dropped acks of a Shared store.
type droppedAckObserver struct {
	dropped atomic.Int32
}

// AckDropped implements SharedObserver.
func (o *droppedAckObserver) AckDropped() {
	o.dropped.Add(1)
}

// LostOwnership implements SharedObserver.
func (o *droppedAckObserver) LostOwnership() {
}

// WatchRead implements SharedObserver.
func (o *droppedAckObserver) WatchRead() {
}

// WatchEnded implements SharedObserver.
func (o *droppedAckObserver) WatchEnded(string) {
}

// Woke implements SharedObserver.
func (o *droppedAckObserver) Woke(int, int) {
}

// TestSharedCancelNotifierOffReplyPath checks that the cancel notification of a call on another node is sent from a
// goroutine of its own: a notifier which blocks does not hold the answer to the cancel.
func TestSharedCancelNotifierOffReplyPath(t *testing.T) {
	release := make(chan struct{})
	var fx *sharedFixture
	fx = newSharedFixtureWithNotifier(t, func(nodeID string, key Key) {
		<-release
		fx.notifiedMu.Lock()
		defer fx.notifiedMu.Unlock()
		fx.notified = append(fx.notified, notifyCall{nodeID: nodeID, key: key})
	})
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-b", Record{})

	cancelled := make(chan error, 1)
	go func() {
		cancelled <- fx.shared.Cancel(sharedKey, testCancelled)
	}()
	select {
	case err := <-cancelled:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("the cancel waits for its notification")
	}
	require.Empty(t, fx.notifiedCalls())

	close(release)
	requireNotifiedEventually(t, fx, []notifyCall{{nodeID: "node-b", key: sharedKey}})
}

func TestSharedAckQueueFullDropsAndCounts(t *testing.T) {
	local, _ := newTestMemory(1024)
	observer := &droppedAckObserver{}
	shared := NewShared(SharedOptions{
		Local:        local,
		Backend:      newFakeBackend(),
		NodeID:       "node-a",
		TTL:          time.Minute,
		Observer:     observer,
		AckQueueSize: 1,
	})
	scope := testScope("u1", "a")
	require.NoError(t, shared.Ack(scope, []uint64{1, 2, 3}))
	require.Equal(t, int32(2), observer.dropped.Load())
}

func TestSharedAllocsPerLocalHit(t *testing.T) {
	const runs = 500
	fx := newSharedFixture(t, false)
	keys := make([]Key, runs+1)
	for i := range keys {
		keys[i] = Key{Scope: testScope("u1", "a"), MsgID: uint64(i + 1)}
		testAnswer(t, fx.local, keys[i], "ok")
	}
	flights := make([]*Flight, runs+1)
	for i := range flights {
		flights[i] = NewFlightWithFingerprint(sharedFingerprintA)
	}

	next := 0
	sharedAllocs := testing.AllocsPerRun(runs, func() {
		lookup, err := fx.shared.Begin(keys[next], flights[next])
		if err != nil || lookup.Outcome != OutcomeDone {
			t.Error("unexpected shared outcome")
		}
		next++
	})

	next = 0
	memoryAllocs := testing.AllocsPerRun(runs, func() {
		lookup, err := fx.local.Begin(keys[next], flights[next])
		if err != nil || lookup.Outcome != OutcomeDone {
			t.Error("unexpected memory outcome")
		}
		next++
	})
	require.LessOrEqual(t, sharedAllocs, memoryAllocs)
	require.Zero(t, sharedAllocs)
}
