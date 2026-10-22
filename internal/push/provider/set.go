package provider

import (
	"context"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// limitedSender is a Sender with the in-flight limit of its provider on this node.
type limitedSender struct {
	sender Sender
	slots  *semaphore.Weighted
}

// Set holds the senders of the providers of a node, each with its own in-flight limit. The limits of two
// providers are never pooled.
type Set struct {
	limited map[string]*limitedSender
}

// NewSet returns an empty Set.
func NewSet() *Set {
	return &Set{
		limited: make(map[string]*limitedSender),
	}
}

// Add registers sender for the provider name with at most maxInflight provider requests in flight on the node.
func (s *Set) Add(name string, sender Sender, maxInflight int64) {
	s.limited[name] = &limitedSender{
		sender: sender,
		slots:  semaphore.NewWeighted(maxInflight),
	}
}

// Dispatch sends job to its provider. It waits for an in-flight slot until syncCtx ends: when no slot is free in
// time, the result is pending and no provider call is made. The provider call then runs on a context with its own
// requestTimeout, created after the slot is acquired, so a call that starts late still gets the full timeout. The
// call is not stopped by the end of syncCtx. The in-flight gauge of the provider counts the slot while it is held.
func (s *Set) Dispatch(syncCtx context.Context, requestTimeout time.Duration, job *Job) *apiproto.PushItemResult {
	name := job.Target.GetProvider()
	limited, ok := s.limited[name]
	if !ok {
		return NewResult(job, StatusRejected)
	}

	// An expired target is refused before it takes a slot, so no provider call can follow it.
	if Expired(job, time.Now()) {
		return NewCodeResult(job, CodeExpired)
	}

	err := limited.slots.Acquire(syncCtx, 1)
	if err != nil {
		return NewResult(job, StatusPending)
	}
	defer limited.slots.Release(1)

	// The gauge is nil while push metrics are not registered.
	inflight := metrics.PushDirectInflight
	if inflight != nil {
		inflight.WithLabelValues(name).Inc()
		defer inflight.WithLabelValues(name).Dec()
	}

	callCtx, cancel := context.WithTimeout(context.WithoutCancel(syncCtx), requestTimeout)
	defer cancel()

	return limited.sender.Send(callCtx, job)
}
