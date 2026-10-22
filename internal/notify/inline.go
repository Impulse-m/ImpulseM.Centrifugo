package notify

import (
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"
)

// inlineOps returns the ops whose handlers run on the goroutine which delivers the event, without a queue. The access
// revocation handler writes the store and enqueues its disconnects without blocking, so it runs inline: a full
// disconnect queue never holds the Core control reader, and the events of the op keep their order.
func inlineOps() map[string]bool {
	return map[string]bool{
		accessrevoke.NotifyOp: true,
	}
}

// bestEffortOps returns the ops whose events may be dropped when their queue is full. The cancel of an exactly-once RPC call
// is best effort: the sender accepts a lost cancel, and the call then runs to its end on its node. A full queue of such an
// op never holds the publisher, which is the Core control reader.
func bestEffortOps() map[string]bool {
	return map[string]bool{
		rpcdedup.CancelNotificationOp: true,
	}
}
