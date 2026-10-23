package push

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// apnsOnly returns a request with one valid APNs target.
func apnsOnly() *apiproto.SendPushNotificationRequest {
	return &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{
			Targets: []*apiproto.PushTarget{
				{Provider: provider.ProviderAPNs, Token: "apns-token", Environment: "production", Ref: "a"},
			},
		},
		Notification: &apiproto.PushNotification{
			Apns: &apiproto.ApnsPushNotification{Payload: testPayload()},
		},
	}
}

// fcmOnly returns a request with one valid FCM target.
func fcmOnly() *apiproto.SendPushNotificationRequest {
	return &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{
			Targets: []*apiproto.PushTarget{
				{Provider: provider.ProviderFCM, Token: "fcm-token", Ref: "f"},
			},
		},
		Notification: &apiproto.PushNotification{
			Fcm: &apiproto.FcmPushNotification{Message: testMessage()},
		},
	}
}

// parityCase is one request of the dry run parity table. A refused request sets refusal, and its refusal must be the same
// in the live send and in the dry run. An accepted request has one target, and its item must have the same status and
// code in both, except that a valid target is sent live and answers dry_run in dry run. status is empty for a valid
// target.
type parityCase struct {
	name    string
	build   func() *apiproto.SendPushNotificationRequest
	refusal uint32
	status  string
	code    string
}

// parityCases returns the requests of the parity table: valid targets, per-item refusals and request refusals.
func parityCases() []parityCase {
	return []parityCase{
		{
			name:  "valid apns target",
			build: apnsOnly,
		},
		{
			name:  "valid fcm target",
			build: fcmOnly,
		},
		{
			name: "apns payload over the ceiling",
			build: func() *apiproto.SendPushNotificationRequest {
				req := apnsOnly()
				req.Notification.Apns.Payload = []byte(`{"a":"` + strings.Repeat("x", 4100) + `"}`)
				return req
			},
			status: provider.StatusRejected,
			code:   provider.CodePayloadTooLarge,
		},
		{
			name: "fcm message over the ceiling after the server additions",
			build: func() *apiproto.SendPushNotificationRequest {
				req := fcmOnly()
				req.Notification.Fcm.Message = []byte(`{"data":{"k":"` + strings.Repeat("x", 4060) + `"}}`)
				return req
			},
			status: provider.StatusRejected,
			code:   provider.CodePayloadTooLarge,
		},
		{
			name: "expired notification",
			build: func() *apiproto.SendPushNotificationRequest {
				req := apnsOnly()
				req.Notification.ExpireAt = time.Now().Unix() - 10
				return req
			},
			status: provider.StatusRejected,
			code:   provider.CodeExpired,
		},
		{
			name: "apns header outside the allow-list",
			build: func() *apiproto.SendPushNotificationRequest {
				req := apnsOnly()
				req.Notification.Apns.Headers = map[string]string{"authorization": "x"}
				return req
			},
			refusal: 107,
		},
		{
			name: "apns collapse header differs from the request collapse id",
			build: func() *apiproto.SendPushNotificationRequest {
				req := apnsOnly()
				req.CollapseId = "a"
				req.Notification.Apns.Headers = map[string]string{"apns-collapse-id": "b"}
				return req
			},
			refusal: 107,
		},
		{
			name: "apns target without environment",
			build: func() *apiproto.SendPushNotificationRequest {
				req := apnsOnly()
				req.Recipient.Targets[0].Environment = ""
				return req
			},
			refusal: 107,
		},
		{
			name: "fcm message with a token",
			build: func() *apiproto.SendPushNotificationRequest {
				req := fcmOnly()
				req.Notification.Fcm.Message = []byte(`{"token":"caller","notification":{"title":"hello"}}`)
				return req
			},
			refusal: 107,
		},
		{
			name: "recipient count over max_recipient_items",
			build: func() *apiproto.SendPushNotificationRequest {
				req := apnsOnly()
				targets := make([]*apiproto.PushTarget, 1001)
				for index := range targets {
					targets[index] = &apiproto.PushTarget{Provider: provider.ProviderAPNs, Token: "t", Environment: "production"}
				}
				req.Recipient.Targets = targets
				return req
			},
			refusal: 107,
		},
	}
}

// TestDryRunAnswersLikeLive checks that a dry run runs the same checks as the live send, before any provider call: the
// refusals and the item statuses and codes of the live send and of the dry run are the same, for valid and invalid
// targets alike. The live send uses a fake provider, and the dry run uses a set without any provider.
func TestDryRunAnswersLikeLive(t *testing.T) {
	live := newTestSender(t, &countingSender{}, nil)

	dryCfg := testConfig()
	dryCfg.DryRun = true
	dry, err := Setup(context.Background(), dryCfg)
	require.NoError(t, err)

	for _, testCase := range parityCases() {
		t.Run(testCase.name, func(t *testing.T) {
			liveResp := live.Send(context.Background(), testCase.build())
			dryResp := dry.Send(context.Background(), testCase.build())

			if testCase.refusal != 0 {
				require.NotNil(t, liveResp.GetError())
				require.Equal(t, testCase.refusal, liveResp.GetError().GetCode())
				require.NotNil(t, dryResp.GetError())
				require.Equal(t, liveResp.GetError().GetCode(), dryResp.GetError().GetCode())
				require.Equal(t, liveResp.GetError().GetMessage(), dryResp.GetError().GetMessage())
				return
			}

			require.Nil(t, liveResp.GetError())
			require.Nil(t, dryResp.GetError())
			liveResults := liveResp.GetResult().GetResults()
			dryResults := dryResp.GetResult().GetResults()
			require.Len(t, liveResults, 1)
			require.Len(t, dryResults, 1)

			liveItem := liveResults[0]
			dryItem := dryResults[0]
			if testCase.status == "" {
				require.Equal(t, provider.StatusSent, liveItem.GetStatus())
				require.Equal(t, provider.StatusDryRun, dryItem.GetStatus())
				return
			}
			require.Equal(t, testCase.status, liveItem.GetStatus())
			require.Equal(t, testCase.code, liveItem.GetCode())
			require.Equal(t, liveItem.GetStatus(), dryItem.GetStatus())
			require.Equal(t, liveItem.GetCode(), dryItem.GetCode())
		})
	}
}
