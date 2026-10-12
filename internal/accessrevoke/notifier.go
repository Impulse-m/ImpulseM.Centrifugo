package accessrevoke

import (
	"github.com/centrifugal/centrifuge"
	"golang.org/x/time/rate"
)

// Notifier sends access revocation notifications to all nodes. Sends are limited by a rate so that
// a burst of API calls can't flood the cluster.
type Notifier struct {
	node    *centrifuge.Node
	limiter *rate.Limiter
}

// NewNotifier creates a Notifier which allows ratePerSecond notifications per second.
func NewNotifier(node *centrifuge.Node, ratePerSecond int) *Notifier {
	return &Notifier{
		node:    node,
		limiter: rate.NewLimiter(rate.Limit(ratePerSecond), ratePerSecond),
	}
}

// Allow reserves one notification. It returns false when the rate limit is exceeded, in which case
// the caller must not apply the operation.
func (n *Notifier) Allow() bool {
	return n.limiter.Allow()
}

// Notify sends the notification to all nodes. The current node receives it too and ignores it.
func (n *Notifier) Notify(notification Notification) error {
	data, err := EncodeNotification(notification)
	if err != nil {
		return err
	}
	return n.node.Notify(NotifyOp, data, "")
}
