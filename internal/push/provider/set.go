package provider

import (
	"context"
	"strconv"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// Label values of the notification count: the recipient type of a token target and the platform of each provider.
const (
	platformIOS     = "ios"
	platformUnknown = "unknown"
	recipientToken  = "token"
)

// Values of the err_code label. They are an enumeration: a provider reason never becomes a label.
const (
	errCodeOK              = "ok"
	errCodeHTTP4xx         = "http_4xx"
	errCodeHTTP5xx         = "http_5xx"
	errCodeHTTPOther       = "http_other"
	errCodeUnanswered      = "unanswered"
	errCodePayloadTooLarge = "payload_too_large"
	errCodeExpired         = "expired"
	errCodeNotSent         = "not_sent"
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
	dryRun  bool
}

// NewSet returns an empty Set.
func NewSet() *Set {
	return &Set{
		limited: make(map[string]*limitedSender),
	}
}

// NewDryRunSet returns an empty Set in dry run mode: Dispatch checks each target and answers dry_run for a valid one,
// and no provider is called.
func NewDryRunSet() *Set {
	return &Set{
		limited: make(map[string]*limitedSender),
		dryRun:  true,
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
	result := s.dispatch(syncCtx, requestTimeout, job)
	// A dry run sends nothing, so it is not counted.
	if result.GetStatus() != StatusDryRun {
		countNotification(job, result)
	}
	return result
}

// dispatch is the body of Dispatch: the server checks, the wait for a slot and the provider call.
func (s *Set) dispatch(syncCtx context.Context, requestTimeout time.Duration, job *Job) *apiproto.PushItemResult {
	// An expired target is refused before it takes a slot, so no provider call can follow it.
	if Expired(job, time.Now()) {
		return NewCodeResult(job, CodeExpired)
	}

	// A dry run target is valid here. The dry run check comes before the provider lookup, because a dry run set holds
	// no provider: no slot is taken and no provider is called for it.
	if s.dryRun {
		return DryRun(job)
	}

	name := job.Target.GetProvider()
	limited, ok := s.limited[name]
	if !ok {
		return NewResult(job, StatusRejected)
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

// countNotification counts one result in the notification count, with the bounded labels of the result. Only the APNs
// and FCM providers are counted, so a provider name from a request never becomes a label value.
func countNotification(job *Job, result *apiproto.PushItemResult) {
	counter := metrics.PushNotificationCount
	if counter == nil {
		return
	}

	name := job.Target.GetProvider()
	var platform string
	switch name {
	case ProviderAPNs:
		platform = platformIOS
	case ProviderFCM:
		platform = platformUnknown
	default:
		return
	}

	success := strconv.FormatBool(result.GetStatus() == StatusSent)
	counter.WithLabelValues(name, recipientToken, platform, success, errCodeOf(result)).Inc()
}

// errCodeOf returns the err_code label of result: a status class, a refusal code or a pending wait. A provider reason is
// never a label. A rejected result with no status line and no code is counted as http_other.
func errCodeOf(result *apiproto.PushItemResult) string {
	status := result.GetStatus()
	code := result.GetCode()
	httpStatus := result.GetHttpStatus()

	switch {
	case status == StatusSent:
		return errCodeOK
	case status == StatusPending:
		return errCodeNotSent
	case status == StatusUnanswered:
		return errCodeUnanswered
	case code == CodePayloadTooLarge:
		return errCodePayloadTooLarge
	case code == CodeExpired:
		return errCodeExpired
	case httpStatus >= 400 && httpStatus < 500:
		return errCodeHTTP4xx
	case httpStatus >= 500 && httpStatus < 600:
		return errCodeHTTP5xx
	default:
		return errCodeHTTPOther
	}
}
