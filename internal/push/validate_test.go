package push

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

func TestValidateValidRequestPlans(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	jobs, refusal := s.plan(validRequest())

	require.Nil(t, refusal)
	require.Len(t, jobs, 2)
	require.Equal(t, 0, jobs[0].Index)
	require.Equal(t, 1, jobs[1].Index)
}

func TestValidateRecipientKindExactlyOne(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	none := validRequest()
	none.Recipient = &apiproto.PushRecipient{}
	requireRefusal(t, s, none, 107, "exactly one")

	two := validRequest()
	two.Recipient.FcmTokens = []string{"f"}
	requireRefusal(t, s, two, 107, "exactly one")
}

func TestValidateNotBuiltAnswers108(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	for _, field := range []string{"filter", "fcm_topic", "webpush_tokens"} {
		req := &apiproto.SendPushNotificationRequest{
			Recipient: &apiproto.PushRecipient{},
			Notification: &apiproto.PushNotification{
				Apns: &apiproto.ApnsPushNotification{Payload: testPayload()},
			},
		}
		switch field {
		case "filter":
			req.Recipient.Filter = &apiproto.DeviceFilter{}
		case "fcm_topic":
			req.Recipient.FcmTopic = "topic"
		case "webpush_tokens":
			req.Recipient.WebpushTokens = []string{"sub"}
		}
		requireRefusal(t, s, req, 108, field)
	}
}

func TestValidateProviderValueFirst(t *testing.T) {
	s := newTestSender(t, &countingSender{}, func(cfg *configtypes.PushNotifications) {
		cfg.EnabledProviders = []string{provider.ProviderFCM}
	})

	req := validRequest()
	req.Recipient.Targets[0].Provider = "apn"
	requireRefusal(t, s, req, 107, "must be apns or fcm")
}

func TestValidateProviderNotEnabledAnswers108(t *testing.T) {
	fake := &countingSender{}
	s := newTestSender(t, fake, func(cfg *configtypes.PushNotifications) {
		cfg.EnabledProviders = []string{provider.ProviderFCM}
	})

	requireRefusal(t, s, validRequest(), 108, "provider apns is not enabled")
	require.Zero(t, fake.calls.Load())
}

func TestValidateEnvironmentRequiredForAPNs(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	missing := validRequest()
	missing.Recipient.Targets[0].Environment = ""
	requireRefusal(t, s, missing, 107, "targets[0].environment")

	onFCM := validRequest()
	onFCM.Recipient.Targets[1].Environment = "production"
	requireRefusal(t, s, onFCM, 107, "targets[1].environment")
}

func TestValidateSendAtRefused(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := validRequest()
	req.SendAt = 1
	requireRefusal(t, s, req, 107, "send_at")
}

func TestValidateRecipientLimit(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	over := &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{},
		Notification: &apiproto.PushNotification{
			Fcm: &apiproto.FcmPushNotification{Message: testMessage()},
		},
	}
	for index := 0; index < 1001; index++ {
		over.Recipient.FcmTokens = append(over.Recipient.FcmTokens, fmt.Sprintf("t%d", index))
	}
	requireRefusal(t, s, over, 107, "max_recipient_items")

	atLimit := &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{FcmTokens: over.Recipient.FcmTokens[:1000]},
		Notification: &apiproto.PushNotification{
			Fcm: &apiproto.FcmPushNotification{Message: testMessage()},
		},
	}
	jobs, refusal := s.plan(atLimit)
	require.Nil(t, refusal)
	require.Len(t, jobs, 1000)
}

func TestValidatePayloadMissingAnswers107(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	noPayload := validRequest()
	noPayload.Notification.Apns.Payload = nil
	requireRefusal(t, s, noPayload, 107, "apns payload")

	notObject := validRequest()
	notObject.Recipient.Targets[1].FcmMessage = []byte(`[1]`)
	requireRefusal(t, s, notObject, 107, "fcm message")
}

func TestValidateFCMMessageTokenRefused(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := validRequest()
	req.Notification.Fcm.Message = []byte(`{"token":"x"}`)
	requireRefusal(t, s, req, 107, "message.token is set by the server")
}

func TestValidateAPNsHeadersRefused(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := validRequest()
	req.Notification.Apns.Headers = map[string]string{"authorization": "x"}
	requireRefusal(t, s, req, 107, "not allowed")

	long := validRequest()
	long.Notification.Apns.Headers = map[string]string{"apns-thread-id": strings.Repeat("t", 65)}
	requireRefusal(t, s, long, 107, "apns-thread-id")
}

func TestValidateUIDLength(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := validRequest()
	req.Uid = strings.Repeat("u", 129)
	requireRefusal(t, s, req, 107, "uid")
}

func TestValidateNotBuiltOptionsAnswers108(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := validRequest()
	req.UseMeta = true
	requireRefusal(t, s, req, 108, "use_meta")
}

func TestCollapseIDPerTarget(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := validRequest()
	req.CollapseId = "request"
	req.Recipient.Targets[0].CollapseId = "target"

	jobs, refusal := s.plan(req)

	require.Nil(t, refusal)
	require.Equal(t, "target", jobs[0].CollapseID)
	require.Equal(t, "request", jobs[1].CollapseID)
}

func TestSharedShorthandUsesDefaultEndpoint(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	req := &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{ApnsTokens: []string{"a1"}},
		Notification: &apiproto.PushNotification{
			Apns: &apiproto.ApnsPushNotification{Payload: testPayload()},
		},
	}

	jobs, refusal := s.plan(req)

	require.Nil(t, refusal)
	require.Len(t, jobs, 1)
	require.Equal(t, "production", jobs[0].Target.GetEnvironment())
	require.Equal(t, provider.ProviderAPNs, jobs[0].Target.GetProvider())
}

func TestCheckRecipientCountBeforeDecode(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	entries := strings.Repeat(`{},`, 1000) + `{}`
	body := []byte(`{"recipient":{"targets":[` + entries + `]}}`)
	refusal := s.CheckRecipientCount(body)
	require.NotNil(t, refusal)
	require.Equal(t, uint32(107), refusal.GetCode())

	atLimit := []byte(`{"recipient":{"targets":[` + strings.Repeat(`{},`, 999) + `{}]}}`)
	require.Nil(t, s.CheckRecipientCount(atLimit))
}

func TestCheckRecipientCountLeavesOtherBodiesToDecoder(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	require.Nil(t, s.CheckRecipientCount([]byte(`{"recipient":{"targets":null}}`)))
	require.Nil(t, s.CheckRecipientCount([]byte(`{"recipient":{"targets":"x"}}`)))
	require.Nil(t, s.CheckRecipientCount([]byte(`{"recipient":`)))
}

func TestFCMMessageOverScanThresholdLeftToItem(t *testing.T) {
	s := newTestSender(t, &countingSender{}, nil)

	// A message over the scan threshold is not scanned here: its item answers payload_too_large.
	big := []byte(`{"data":"` + strings.Repeat("a", maxScannedFCMMessageBytes) + `"}`)
	req := validRequest()
	req.Notification.Fcm.Message = big

	jobs, refusal := s.plan(req)
	require.Nil(t, refusal)
	require.Len(t, jobs, 2)
}
