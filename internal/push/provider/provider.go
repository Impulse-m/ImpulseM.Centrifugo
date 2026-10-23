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

	"github.com/tidwall/gjson"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// Item statuses of a direct send result.
const (
	StatusSent       = "sent"
	StatusRejected   = "rejected"
	StatusUnanswered = "unanswered"
	StatusNotSent    = "not_sent"
	StatusDryRun     = "dry_run"
)

// Item codes. A server refusal sets CodePayloadTooLarge or CodeExpired, and no provider call is made. A not_sent item
// sets one of the other codes, which name why the target never left the node.
const (
	CodePayloadTooLarge = "payload_too_large"
	CodeExpired         = "expired"
	// CodeNoSlot is a target that got no in-flight slot before the slot wait ended.
	CodeNoSlot = "no_slot"
	// CodeConnectFailed is a transport error before the request was written.
	CodeConnectFailed = "connect_failed"
	// CodeDeadline is a target whose call deadline, or whose provider request timeout, came before the request was
	// written.
	CodeDeadline = "deadline"
	// CodeTokenFetch is an FCM target whose access token fetch failed.
	CodeTokenFetch = "token_fetch"
	// CodeShutdown is a target that the node's shutdown ended before the request was written. When the shutdown and a
	// deadline have both ended, the code is shutdown.
	CodeShutdown = "shutdown"
)

// Provider names of a target.
const (
	ProviderAPNs = "apns"
	ProviderFCM  = "fcm"
)

// Environments of an APNs target. The environment chooses the APNs host, and the same names are the values of the
// endpoint key of the configuration.
const (
	EnvironmentProduction  = "production"
	EnvironmentDevelopment = "development"
)

// Payload ceilings of the providers, in bytes. The APNs ceiling of a VoIP push is the larger one.
const (
	MaxAPNsPayloadBytes     = 4096
	MaxAPNsVoIPPayloadBytes = 5120
	MaxFCMMessageBytes      = 4096
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

// IsJSONObject reports whether data is one valid JSON object. The check of gjson looks at the first byte only, so the
// whole value is validated too.
func IsJSONObject(data []byte) bool {
	return gjson.ValidBytes(data) && gjson.ParseBytes(data).IsObject()
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
	// Body is the provider body of the job when the oversize check of the set has built it, so the provider sends it
	// without a second copy. It is nil until then.
	Body []byte
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

// NewNotSentResult returns a not_sent result of job: the target never left the node. The code names why, and it is
// empty when the wait ended and the set names the cause.
func NewNotSentResult(job *Job, code string) *apiproto.PushItemResult {
	result := NewResult(job, StatusNotSent)
	result.Code = code
	return result
}

// NoAnswerResult returns the result of job for an attempt that got no status line. When the request was written, the
// provider may have delivered it, so the result is unanswered. Otherwise the target was not sent: it is not_sent with
// connect_failed when the call context is still alive, which is a transport error, and with no code when the call
// context has ended, so the set names the deadline or the shutdown.
func NoAnswerResult(call context.Context, job *Job, written bool) *apiproto.PushItemResult {
	if written {
		return NewResult(job, StatusUnanswered)
	}
	if call.Err() != nil {
		return NewNotSentResult(job, "")
	}
	return NewNotSentResult(job, CodeConnectFailed)
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
