package rpcdedup

import (
	"sync"
)

// cancelNotice is a cancel notification to the node which runs a call: the node and the key of the call.
type cancelNotice struct {
	nodeID string
	key    Key
}

// maxCancelPending is the number of notices which may wait for a worker. A notice beyond it is dropped: the call it
// cancels then runs until its own end, as it does without a cancel, and the drop is counted, see Shared.Cancel.
const maxCancelPending = 65536

// noticeState is the state of a notice which is pending in cancelNotifier.
type noticeState uint8

const (
	// noticeQueued is a notice which waits for a worker.
	noticeQueued noticeState = iota + 1
	// noticeSending is a notice which a worker is sending.
	noticeSending
	// noticeResend is a notice which is being sent, and for which a cancel came meanwhile: it is sent once more after.
	noticeResend
)

// cancelNotifier sends the cancel notifications of a node, in the background. A notice which is pending for the same
// node and key is not queued again: the cancels which come while it waits are coalesced into it, and the cancels which
// come while it is sent are coalesced into one more send after it. The owner may have handled the sent notice before
// its call was registered, so that one more send is what delivers the cancel. So at most one notice of a key is sent at
// a time, and at most one waits behind it. The queue is served by at most maxCancelWorkers goroutines, which start as
// notices come and exit when the queue is empty, so an idle node runs none. The queue holds at most maxCancelPending
// notices which wait for a worker.
type cancelNotifier struct {
	send    func(nodeID string, key Key)
	mu      sync.Mutex
	pending map[cancelNotice]noticeState
	queue   []cancelNotice
	workers int
}

// newCancelNotifier returns a notifier which sends each notice with send. send must not be nil: the callers check the
// notifier of the node before they queue a notice.
func newCancelNotifier(send func(nodeID string, key Key)) *cancelNotifier {
	return &cancelNotifier{send: send, pending: map[cancelNotice]noticeState{}}
}

// notify queues the cancel notification of key to nodeID, and returns at once. It starts a worker when the queue has
// none free, up to maxCancelWorkers. It returns false when the queue is full and the notice is dropped.
func (n *cancelNotifier) notify(nodeID string, key Key) bool {
	notice := cancelNotice{nodeID: nodeID, key: key}
	n.mu.Lock()
	defer n.mu.Unlock()
	if state, found := n.pending[notice]; found {
		if state == noticeSending {
			n.pending[notice] = noticeResend
		}
		return true
	}
	if len(n.queue) >= maxCancelPending {
		return false
	}
	n.pending[notice] = noticeQueued
	n.queue = append(n.queue, notice)
	if n.workers < maxCancelWorkers {
		n.workers++
		go n.work()
	}
	return true
}

// work sends the queued notices, one at a time, until the queue is empty. A notice whose cancel came while it was sent
// is queued once more, by the worker which sent it, and the resend is not counted against maxCancelPending: at most one
// per worker is outstanding. The backing array of an emptied queue is released, so a burst does not pin its memory.
func (n *cancelNotifier) work() {
	for {
		n.mu.Lock()
		if len(n.queue) == 0 {
			n.queue = nil
			n.workers--
			n.mu.Unlock()
			return
		}
		notice := n.queue[0]
		n.queue = n.queue[1:]
		n.pending[notice] = noticeSending
		n.mu.Unlock()

		n.send(notice.nodeID, notice.key)

		n.mu.Lock()
		if n.pending[notice] == noticeResend {
			n.pending[notice] = noticeQueued
			n.queue = append(n.queue, notice)
		} else {
			delete(n.pending, notice)
		}
		n.mu.Unlock()
	}
}
