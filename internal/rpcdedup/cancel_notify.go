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

// cancelNotifier sends the cancel notifications of a node, in the background. A notification which is pending for the
// same node and key is not queued again: a cancel of the call which is already on its way covers the next one. The
// queue is served by at most maxCancelWorkers goroutines, which start as notices come and exit when the queue is
// empty, so an idle node runs none. The queue holds at most maxCancelPending notices which wait for a worker.
type cancelNotifier struct {
	send    func(nodeID string, key Key)
	mu      sync.Mutex
	pending map[cancelNotice]struct{}
	queue   []cancelNotice
	workers int
}

// newCancelNotifier returns a notifier which sends each notice with send.
func newCancelNotifier(send func(nodeID string, key Key)) *cancelNotifier {
	return &cancelNotifier{send: send, pending: map[cancelNotice]struct{}{}}
}

// notify queues the cancel notification of key to nodeID, and returns at once. It starts a worker when the queue has
// none free, up to maxCancelWorkers. It returns false when the queue is full and the notice is dropped.
func (n *cancelNotifier) notify(nodeID string, key Key) bool {
	notice := cancelNotice{nodeID: nodeID, key: key}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, found := n.pending[notice]; found {
		return true
	}
	if len(n.queue) >= maxCancelPending {
		return false
	}
	n.pending[notice] = struct{}{}
	n.queue = append(n.queue, notice)
	if n.workers < maxCancelWorkers {
		n.workers++
		go n.work()
	}
	return true
}

// work sends the queued notices, one at a time, until the queue is empty. A notice stays pending while it is sent.
func (n *cancelNotifier) work() {
	for {
		n.mu.Lock()
		if len(n.queue) == 0 {
			n.workers--
			n.mu.Unlock()
			return
		}
		notice := n.queue[0]
		n.queue = n.queue[1:]
		n.mu.Unlock()

		n.send(notice.nodeID, notice.key)

		n.mu.Lock()
		delete(n.pending, notice)
		n.mu.Unlock()
	}
}
