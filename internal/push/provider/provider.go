// Package provider sends direct push notifications to the APNs and FCM providers. A provider answers one
// target with one raw result: the provider status, reason, detail and message id. It classifies nothing and keeps
// no state, so the caller owns retries.
package provider

import (
	"context"

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
