package rpcdedup

import (
	"sync"
)

// cancelNotice is a cancel notification to the node which runs a call: the node and the key of the call.
type cancelNotice struct {
	nodeID string
	key    Key
}

// cancelNotifier sends the cancel notifications of a node, in the background. A notification which is pending for the
// same node and key is not queued again: a cancel of the call which is already on its way covers the next one. The
// queue is served by at most maxCancelWorkers goroutines, which start as notices come and exit when the queue is empty,
// so an idle node runs none. A notice which finds the queue full is not sent, and its call runs to its end, as a cancel
// which the shared tier fails to deliver does.
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
// none free, up to maxCancelWorkers.
func (n *cancelNotifier) notify(nodeID string, key Key) {
	notice := cancelNotice{nodeID: nodeID, key: key}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, found := n.pending[notice]; found || len(n.queue) >= maxQueuedCancels {
		return
	}
	n.pending[notice] = struct{}{}
	n.queue = append(n.queue, notice)
	if n.workers < maxCancelWorkers {
		n.workers++
		go n.work()
	}
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
