package notify

import (
	"github.com/centrifugal/centrifugo/v6/internal/accessrevoke"
)

// inlineOps returns the ops whose handlers run on the goroutine which delivers the event, without a queue. The access
// revocation handler writes the store and enqueues its disconnects without blocking, so it runs inline: a full
// disconnect queue never holds the Core control reader, and the events of the op keep their order.
func inlineOps() map[string]bool {
	return map[string]bool{
		accessrevoke.NotifyOp: true,
	}
}
