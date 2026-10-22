package provider

import (
	"context"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
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
// call is not stopped by the end of syncCtx.
func (s *Set) Dispatch(syncCtx context.Context, requestTimeout time.Duration, job *Job) *apiproto.PushItemResult {
	limited, ok := s.limited[job.Target.GetProvider()]
	if !ok {
		return NewResult(job, StatusRejected)
	}

	err := limited.slots.Acquire(syncCtx, 1)
	if err != nil {
		return NewResult(job, StatusPending)
	}
	defer limited.slots.Release(1)

	callCtx, cancel := context.WithTimeout(context.WithoutCancel(syncCtx), requestTimeout)
	defer cancel()

	return limited.sender.Send(callCtx, job)
}
