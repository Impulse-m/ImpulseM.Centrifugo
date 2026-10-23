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
	limited  map[string]*limitedSender
	dryRun   bool
	oversize func(*Job) bool
	shutdown context.Context
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

// WithOversizeCheck sets the payload ceiling check of the set and returns the set. An item the check reports is
// answered payload_too_large before the dry run answer and before any slot is taken. The check may also build the
// provider body of the item, once, so that the send does not copy it again.
func (s *Set) WithOversizeCheck(check func(*Job) bool) *Set {
	s.oversize = check
	return s
}

// WithShutdown sets the process-lifetime context of the set and returns the set. Its end classifies a target that was not
// sent as shutdown, not as deadline.
func (s *Set) WithShutdown(shutdown context.Context) *Set {
	s.shutdown = shutdown
	return s
}

// Add registers sender for the provider name with at most maxInflight provider requests in flight on the node.
func (s *Set) Add(name string, sender Sender, maxInflight int64) {
	s.limited[name] = &limitedSender{
		sender: sender,
		slots:  semaphore.NewWeighted(maxInflight),
	}
}

// Dispatch is DispatchWindow with a call context that never ends, so the provider call ends at its request timeout
// only. It is a test seam: the send path calls DispatchWindow with the call deadline of the request.
func (s *Set) Dispatch(syncCtx context.Context, requestTimeout time.Duration, job *Job) *apiproto.PushItemResult {
	return s.DispatchWindow(syncCtx, context.Background(), requestTimeout, job)
}

// DispatchWindow sends job to its provider. It waits for an in-flight slot until slot ends: when no slot is free in
// time, the target is not sent. The provider call then runs on a context with its own requestTimeout, created after the
// slot is acquired, so a call that starts late still gets the full timeout. The call also ends at the deadline of call,
// the call deadline of the request, and it is cancelled when call ends. The call is not stopped by the end of slot. The
// in-flight gauge of the provider counts the slot while it is held.
func (s *Set) DispatchWindow(
	slot context.Context,
	call context.Context,
	requestTimeout time.Duration,
	job *Job,
) *apiproto.PushItemResult {
	result := s.dispatch(slot, call, requestTimeout, job)
	// A dry run sends nothing, so it is not counted.
	if result.GetStatus() != StatusDryRun {
		countNotification(job, result)
	}
	return result
}

// dispatch is the body of Dispatch: the server checks, the wait for a slot and the provider call.
func (s *Set) dispatch(
	slot context.Context,
	call context.Context,
	requestTimeout time.Duration,
	job *Job,
) *apiproto.PushItemResult {
	// An expired target is refused before it takes a slot, so no provider call can follow it.
	if Expired(job, time.Now()) {
		return NewCodeResult(job, CodeExpired)
	}

	// The payload ceiling is checked before the dry run answer, so a dry run answers what a live send answers.
	if s.oversize != nil && s.oversize(job) {
		return NewCodeResult(job, CodePayloadTooLarge)
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

	err := limited.slots.Acquire(slot, 1)
	if err != nil {
		return NewNotSentResult(job, s.waitEndCode(slot, call))
	}
	defer limited.slots.Release(1)

	// Acquire takes a free slot even when slot has ended, so the end is checked again: no call starts after it.
	if slot.Err() != nil {
		return NewNotSentResult(job, s.waitEndCode(slot, call))
	}

	// The gauge is nil while push metrics are not registered.
	inflight := metrics.PushDirectInflight
	if inflight != nil {
		inflight.WithLabelValues(name).Inc()
		defer inflight.WithLabelValues(name).Dec()
	}

	callCtx, cancel := callContext(call, requestTimeout)
	defer cancel()

	result := limited.sender.Send(callCtx, job)
	// A provider returns not_sent with no code when its call context ended before the write: the set names the cause.
	if result.GetStatus() == StatusNotSent && result.GetCode() == "" {
		result.Code = s.endCode()
	}
	return result
}

// ShutdownContext returns the process-lifetime context of the set, or a context that never ends when there is none. Its
// end stops the waits and the calls of every send. A nil set has no shutdown, so it returns a context that never ends.
func (s *Set) ShutdownContext() context.Context {
	if s == nil || s.shutdown == nil {
		return context.Background()
	}
	return s.shutdown
}

// shutdownEnded reports whether the shutdown context of the set has ended.
func (s *Set) shutdownEnded() bool {
	return s.shutdown != nil && s.shutdown.Err() != nil
}

// endCode returns the code of a target that was not sent because a wait or a call ended. When the shutdown has ended,
// the code is shutdown, even if a call deadline has ended too: both ends are seen only after the fact, so the shutdown
// takes precedence. Otherwise the code is deadline.
func (s *Set) endCode() string {
	if s.shutdownEnded() {
		return CodeShutdown
	}
	return CodeDeadline
}

// waitEndCode returns the code of a target that got no slot because its slot wait ended. The shutdown takes precedence.
// Otherwise the code is deadline when the call deadline ended first, and no_slot when the sync timeout (the slot wait)
// ended first. The slot wait ends at the earlier of the sync timeout and the call deadline, so the call deadline ended
// first when it is not after the slot deadline.
func (s *Set) waitEndCode(slot context.Context, call context.Context) string {
	if s.shutdownEnded() {
		return CodeShutdown
	}
	if call.Err() == nil {
		return CodeNoSlot
	}
	callDeadline, hasCallDeadline := call.Deadline()
	slotDeadline, hasSlotDeadline := slot.Deadline()
	if hasCallDeadline && hasSlotDeadline && slotDeadline.Before(callDeadline) {
		return CodeNoSlot
	}
	return CodeDeadline
}

// callContext returns the context of one provider call. It ends at the earlier of its own request timeout, counted from
// now, and the deadline of call. It ends when call ends, so a shutdown or the call deadline cancels it. It is not bound to
// the slot wait, so the end of the slot wait does not stop a call that started.
func callContext(call context.Context, requestTimeout time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(requestTimeout)
	if callDeadline, ok := call.Deadline(); ok {
		deadline = earlierOf(deadline, callDeadline)
	}

	callCtx, cancel := context.WithDeadline(context.WithoutCancel(call), deadline)
	stop := context.AfterFunc(call, cancel)
	return callCtx, func() {
		stop()
		cancel()
	}
}

// earlierOf returns the earlier of two times.
func earlierOf(first time.Time, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
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

// errCodeOf returns the err_code label of result: a status class, a refusal code or a not_sent target. A provider reason is
// never a label. A rejected result with no status line and no code is counted as http_other.
func errCodeOf(result *apiproto.PushItemResult) string {
	status := result.GetStatus()
	code := result.GetCode()
	httpStatus := result.GetHttpStatus()

	switch {
	case status == StatusSent:
		return errCodeOK
	case status == StatusNotSent:
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
