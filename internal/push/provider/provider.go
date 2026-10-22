// Package provider sends direct push notifications to the APNs and FCM providers. A provider answers one
// target with one raw result: the provider status, reason, detail and message id. It classifies nothing and keeps
// no state, so the caller owns retries.
package provider

import (
	"context"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// Item statuses of a direct send result.
const (
	StatusSent       = "sent"
	StatusRejected   = "rejected"
	StatusUnanswered = "unanswered"
	StatusPending    = "pending"
	StatusDryRun     = "dry_run"
)

// Item codes of a server refusal, set only when the server refused a target before any provider call.
const (
	CodePayloadTooLarge = "payload_too_large"
	CodeExpired         = "expired"
)

// Provider names of a target.
const (
	ProviderAPNs = "apns"
	ProviderFCM  = "fcm"
)

// MaxResponseBodyBytes bounds the body of a provider response. The body is never returned.
const MaxResponseBodyBytes = 64 << 10

// MaxFieldBytes bounds each provider-supplied string of a result: the code, the detail and the message id.
const MaxFieldBytes = 256

// Truncate returns value cut to at most MaxFieldBytes. The cut never splits a UTF-8 rune: it backs off to the start of
// the rune that straddles the limit, because the result fields are proto3 strings and must stay valid UTF-8.
func Truncate(value string) string {
	if len(value) <= MaxFieldBytes {
		return value
	}
	end := MaxFieldBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

// ReadAnswer reads the body of a provider response, bounded by MaxResponseBodyBytes. The caller closes the body. It is
// called after the status line is taken, so a read error keeps the status.
func ReadAnswer(response *http.Response) ([]byte, error) {
	return io.ReadAll(io.LimitReader(response.Body, MaxResponseBodyBytes))
}

// Expired reports whether the notification of job has an expire_at at or before now. Such a target is refused with
// CodeExpired and no provider call is made.
func Expired(job *Job, now time.Time) bool {
	expireAt := job.Notification.GetExpireAt()
	return expireAt > 0 && expireAt <= now.Unix()
}

// DryRun returns the result of a valid job in dry run mode. No provider is called, so the HTTP status is zero and
// there is no message id.
func DryRun(job *Job) *apiproto.PushItemResult {
	return NewResult(job, StatusDryRun)
}

// Job is one target of a direct send, with the request values a provider reads.
type Job struct {
	// Index is the zero-based position of the target in the request.
	Index int
	// Target is the raw provider token of the job. Its APNs payload or FCM message overrides the request value.
	Target *apiproto.PushTarget
	// Notification is the notification of the request.
	Notification *apiproto.PushNotification
	// CollapseID is the resolved collapse id of the target: its own, else the request value, else empty.
	CollapseID string
}

// Sender sends one job to one provider. The context carries the deadline of the provider call.
type Sender interface {
	Send(ctx context.Context, job *Job) *apiproto.PushItemResult
}

// NewResult returns a result of job with status and without any provider data.
func NewResult(job *Job, status string) *apiproto.PushItemResult {
	return &apiproto.PushItemResult{
		Index:  uint32(job.Index),
		Ref:    job.Target.GetRef(),
		Status: status,
	}
}

// NewCodeResult returns a rejected result of job for a server refusal with code.
func NewCodeResult(job *Job, code string) *apiproto.PushItemResult {
	result := NewResult(job, StatusRejected)
	result.Code = code
	return result
}

// ApnsPayloadOf returns the APNs payload of job: the target's own payload when set, else the request payload.
func ApnsPayloadOf(job *Job) []byte {
	if len(job.Target.GetApnsPayload()) > 0 {
		return job.Target.GetApnsPayload()
	}
	return job.Notification.GetApns().GetPayload()
}

// FCMMessageOf returns the FCM message of job: the target's own message when set, else the request message.
func FCMMessageOf(job *Job) []byte {
	if len(job.Target.GetFcmMessage()) > 0 {
		return job.Target.GetFcmMessage()
	}
	return job.Notification.GetFcm().GetMessage()
}
