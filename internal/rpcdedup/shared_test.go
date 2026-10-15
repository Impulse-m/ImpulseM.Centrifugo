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
	// onBegin, onComplete and onCancel run when the call is made, before it takes effect and outside the lock.
	onBegin    func()
	onComplete func()
	onCancel   func()
}

// newFakeBackend returns a connected backend without records.
func newFakeBackend() *fakeBackend {
	return &fakeBackend{entries: make(map[Key]*fakeEntry), connected: true}
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
func (f *fakeBackend) Begin(_ context.Context, key Key, fingerprint [32]byte, nodeID string, _ time.Duration) (BeginResult, error) {
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
		return BeginResult{State: BeginInFlight, Owner: e.owner}, nil
	}
	return BeginResult{State: BeginDone, Record: e.rec}, nil
}

// Complete implements Backend with the semantics of completeScript: only the owner completes its record.
func (f *fakeBackend) Complete(ctx context.Context, key Key, nodeID string, rec Record, _ time.Duration) (bool, error) {
	if err := f.enter("complete"); err != nil {
		return false, err
	}
	if f.onComplete != nil {
		f.onComplete()
	}
	if f.blockComplete {
		<-ctx.Done()
		return false, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.completeFailures > 0 {
		f.completeFailures--
		return false, errors.New("write timed out")
	}
	owned := f.applyComplete(key, nodeID, rec)
	if f.lostReplies > 0 {
		f.lostReplies--
		return false, errors.New("reply lost")
	}
	return owned, nil
}

// applyComplete writes rec for key when nodeID owns the in-flight record of key. Caller holds mu.
func (f *fakeBackend) applyComplete(key Key, nodeID string, rec Record) bool {
	e := f.entries[key]
	if e == nil || e.state != 'i' || e.owner != nodeID {
		return false
	}
	if rec.Abandoned {
		delete(f.entries, key)
		return true
	}
	e.state = 'd'
	e.owner = ""
	if rec.Uncached {
		e.rec = Record{Uncached: true}
	} else {
		e.rec = rec
	}
	return true
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
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[key]
	if e == nil {
		return PollResult{State: PollMissing}, nil
	}
	if e.state == 'i' {
		return PollResult{State: PollInFlight}, nil
	}
	return PollResult{State: PollDone, Record: e.rec}, nil
}

// Ack implements Backend: it deletes the done records and tombstones of the acks, and keeps the batch.
func (f *fakeBackend) Ack(_ context.Context, acks []ScopeAcks) error {
	if err := f.enter("ack"); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, acks)
	for _, scope := range acks {
		for _, msgID := range scope.MsgIDs {
			key := Key{Scope: scope.Scope, MsgID: msgID}
			if e := f.entries[key]; e != nil && e.state != 'i' {
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
	local    *Memory
	backend  *fakeBackend
	shared   *Shared
	notified []notifyCall
	dropped  int
	lost     int
}

// AckDropped implements SharedObserver.
func (fx *sharedFixture) AckDropped() {
	fx.dropped++
}

// LostOwnership implements SharedObserver.
func (fx *sharedFixture) LostOwnership() {
	fx.lost++
}

// newSharedFixture returns a shared store of node-a with the given failure mode.
func newSharedFixture(t *testing.T, failOpen bool) *sharedFixture {
	t.Helper()
	local, _ := newTestMemory(1024)
	fx := &sharedFixture{local: local, backend: newFakeBackend()}
	fx.shared = NewShared(SharedOptions{
		Local:         local,
		Backend:       fx.backend,
		NodeID:        "node-a",
		TTL:           time.Minute,
		MaxReplyBytes: 1024,
		FailOpen:      failOpen,
		Notifier: func(nodeID string, key Key) {
			fx.notified = append(fx.notified, notifyCall{nodeID: nodeID, key: key})
		},
		Observer:      fx,
		PollInitial:   time.Millisecond,
		PollMax:       2 * time.Millisecond,
		AckFlushEvery: time.Hour,
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
	require.Equal(t, []notifyCall{{nodeID: "node-b", key: sharedKey}}, fx.notified)

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
	require.Equal(t, []notifyCall{{nodeID: "node-b", key: sharedKey}}, fx.notified)
	require.True(t, testIsMiss(t, fx.local, sharedKey), "nothing is recorded locally")
}

func TestSharedCancelOwnedByThisNodeWithoutFlightDoesNothing(t *testing.T) {
	fx := newSharedFixture(t, false)
	fx.backend.put(sharedKey, 'i', sharedFingerprintA, "node-a", Record{})

	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	require.Empty(t, fx.notified)
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

func TestSharedCancelFailureFollowsOnStoreError(t *testing.T) {
	fx := newSharedFixture(t, true)
	fx.backend.setFail(errors.New("redis is down"))
	require.NoError(t, fx.shared.Cancel(sharedKey, testCancelled))
	lookup := testBegin(t, fx.local, sharedKey, NewFlightWithFingerprint(sharedFingerprintA))
	require.Equal(t, OutcomeDone, lookup.Outcome)

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
	failure := &centrifuge.Error{Code: 107, Message: strings.Repeat("e", 2000)}
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
	require.Empty(t, fx.notified, "the owner is this node, which is not notified")
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
