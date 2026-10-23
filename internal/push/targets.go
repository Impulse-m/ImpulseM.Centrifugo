package push

import (
	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// recipientTargets returns the targets of a request. The targets list is used as it is. A shorthand list becomes one
// target per token: fcm_tokens are fcm targets, and apns_tokens are apns targets in the APNs environment of the default
// credential, the endpoint of the configuration. A request has one kind, so only one of these is returned.
func (s *Sender) recipientTargets(req *apiproto.SendPushNotificationRequest) []*apiproto.PushTarget {
	recipient := req.GetRecipient()
	if len(recipient.GetTargets()) > 0 {
		return recipient.GetTargets()
	}

	var targets []*apiproto.PushTarget
	for _, token := range recipient.GetFcmTokens() {
		targets = append(targets, &apiproto.PushTarget{
			Provider: provider.ProviderFCM,
			Token:    token,
		})
	}
	for _, token := range recipient.GetApnsTokens() {
		targets = append(targets, &apiproto.PushTarget{
			Provider:    provider.ProviderAPNs,
			Token:       token,
			Environment: s.apnsEndpoint,
		})
	}
	return targets
}

// jobsOf returns one job per target, in request order. The collapse id of a job is the collapse id of its target when
// set, else the collapse id of the request, else empty.
func jobsOf(targets []*apiproto.PushTarget, req *apiproto.SendPushNotificationRequest) []*provider.Job {
	jobs := make([]*provider.Job, len(targets))
	for index, target := range targets {
		collapseID := target.GetCollapseId()
		if collapseID == "" {
			collapseID = req.GetCollapseId()
		}
		jobs[index] = &provider.Job{
			Index:        index,
			Target:       target,
			Notification: req.GetNotification(),
			CollapseID:   collapseID,
		}
	}
	return jobs
}
