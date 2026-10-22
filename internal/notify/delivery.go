package notify

import (
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/tools"
	"github.com/rs/zerolog/log"
)

// queueSize is how many events of one op are buffered before the publisher waits for the handler.
const queueSize = 1024

// fullLogInterval limits the log line about a full queue to one line per interval.
const fullLogInterval = 10 * time.Second

// delivery runs the handlers of notifications through one queue per op. The queue of an op has one
// consumer goroutine, which starts on the first event of the op and stays for the life of the process,
// so the goroutines are constant per node, and the events of one op are handled in the order they were
// received. A slow handler delays only the events of its own op.
//
// A full queue blocks the publisher instead of dropping the event. The events are notifications which must
// reach every node, such as access revocations: a dropped one would leave a revoked access working on this
// node. The publisher is the node's notification path, so handlers must stay fast and must not call
// Node.Notify: a handler which waits on its own full queue would deadlock.
//
// An inline op has no queue. Its handler runs on the goroutine which delivers the event, so it must never block.
// The events of an inline op which one goroutine delivers keep their order. The Core control reader delivers the events
// of other nodes, one at a time. A local event runs on the calling goroutine, which is not ordered against the reader,
// so the order of an inline op holds per delivering goroutine only.
type delivery struct {
	queues map[string]*opQueue
}

type opQueue struct {
	op        string
	handler   centrifuge.NotificationHandler
	inline    bool
	events    chan centrifuge.NotificationEvent
	startOnce sync.Once
	fullLog   *tools.IntervalLimiter
}

// newDelivery returns a delivery which runs handlers, each with a queue of size events. The ops in inline have no
// queue, and their handlers run on the delivering goroutine.
func newDelivery(
	handlers map[string]centrifuge.NotificationHandler,
	size int,
	inline map[string]bool,
) *delivery {
	d := &delivery{
		queues: make(map[string]*opQueue, len(handlers)),
	}
	for op, handler := range handlers {
		q := &opQueue{
			op:      op,
			handler: handler,
			inline:  inline[op],
		}
		if !q.inline {
			q.events = make(chan centrifuge.NotificationEvent, size)
			q.fullLog = tools.NewIntervalLimiter(fullLogInterval)
		}
		d.queues[op] = q
	}
	return d
}

// deliver queues event for the handler of its op, or runs the handler at once for an inline op. An event of an op
// without a handler is ignored.
func (d *delivery) deliver(event centrifuge.NotificationEvent) {
	q, ok := d.queues[event.Op]
	if !ok {
		return
	}
	if q.inline {
		q.handler(event)
		return
	}
	q.startOnce.Do(func() {
		go q.consume()
	})
	select {
	case q.events <- event:
		return
	default:
	}
	if q.fullLog.Allow() {
		log.Warn().Str("op", q.op).Int("queue_size", cap(q.events)).Msg("notification queue is full, waiting for the handler")
	}
	q.events <- event
}

// consume runs the handler for the queued events in order. It never returns: the queue is not closed.
func (q *opQueue) consume() {
	for event := range q.events {
		q.handler(event)
	}
}
