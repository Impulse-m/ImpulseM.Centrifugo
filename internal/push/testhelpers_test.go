package push

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/origin"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider/webpush"
)

// countingSender is a provider.Sender that counts its calls. It answers sent at once, or unanswered when hold is set, and
// then only when its context ends: a call that holds the in-flight slot of its provider.
type countingSender struct {
	calls atomic.Int32
	hold  bool
}

// Send counts the call and answers the job.
func (c *countingSender) Send(ctx context.Context, job *provider.Job) *apiproto.PushItemResult {
	c.calls.Add(1)
	if c.hold {
		<-ctx.Done()
		return provider.NewResult(job, provider.StatusUnanswered)
	}
	return provider.NewResult(job, provider.StatusSent)
}

// testConfig returns an enabled sender configuration with the defaults of the configuration keys.
func testConfig() configtypes.PushNotifications {
	return configtypes.PushNotifications{
		Enabled:               true,
		Sender:                true,
		EnabledProviders:      []string{provider.ProviderAPNs, provider.ProviderFCM, provider.ProviderWebPush},
		MaxRecipientItems:     1000,
		MaxRequestBytes:       1 << 20,
		MaxConcurrentRequests: 64,
		SyncTimeout:           configtypes.Duration(10 * time.Second),
		APNs: configtypes.PushAPNs{
			Endpoint:        "production",
			BundleID:        "com.example.app",
			TokensBatchSize: 100,
			MaxInflight:     500,
		},
		FCM: configtypes.PushFCM{
			TokensBatchSize: 500,
			MaxInflight:     200,
		},
		WebPush: configtypes.PushWebPush{
			TokensBatchSize: 100,
			MaxInflight:     500,
		},
		Providers: configtypes.PushProviders{
			RequestTimeout: configtypes.Duration(10 * time.Second),
		},
	}
}

// newTestSender returns a sender with sender as the provider of APNs, FCM and Web Push, without loading any credential.
// The Web Push origins are the built-in list, as a configuration with no origin keys gives.
func newTestSender(t *testing.T, fake provider.Sender, edit func(*configtypes.PushNotifications)) *Sender {
	t.Helper()

	cfg := testConfig()
	if edit != nil {
		edit(&cfg)
	}
	origins, err := origin.NewPatternChecker(webpush.EndpointOrigins(nil, nil))
	require.NoError(t, err)

	s := newSender(cfg)
	s.set = provider.NewSet().WithOversizeCheck(oversized).WithRefusalCheck(refusalOf(origins))
	s.set.Add(provider.ProviderAPNs, fake, int64(cfg.APNs.MaxInflight))
	s.set.Add(provider.ProviderFCM, fake, int64(cfg.FCM.MaxInflight))
	s.set.Add(provider.ProviderWebPush, fake, int64(cfg.WebPush.MaxInflight))
	return s
}

// The receiver of a Web Push subscription in the tests: the keys of RFC 8291 appendix A, which are a valid P-256 point
// and a valid authentication secret.
const (
	testReceiverPublic = "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcx" + "aOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
	testAuthSecret     = "BTBZMqHH6r4Tts7J_aSIgg"
)

// webPushSubscription returns the PushSubscription JSON of a Web Push target for endpoint, with the test receiver keys.
func webPushSubscription(endpoint string) string {
	return `{"endpoint":"` + endpoint + `","keys":{"p256dh":"` + testReceiverPublic + `","auth":"` + testAuthSecret + `"}}`
}

// testWebPushPayload returns a valid Web Push payload.
func testWebPushPayload() []byte {
	return []byte(`{"title":"hello"}`)
}

// testPayload returns a valid APNs payload.
func testPayload() []byte {
	return []byte(`{"aps":{"alert":"hello"}}`)
}

// testMessage returns a valid FCM message without a token.
func testMessage() []byte {
	return []byte(`{"notification":{"title":"hello"}}`)
}

// validRequest returns a request with one APNs target and one FCM target, both valid.
func validRequest() *apiproto.SendPushNotificationRequest {
	return &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{
			Targets: []*apiproto.PushTarget{
				{Provider: provider.ProviderAPNs, Token: "apns-token", Environment: "production", Ref: "a"},
				{Provider: provider.ProviderFCM, Token: "fcm-token", Ref: "f"},
			},
		},
		Notification: &apiproto.PushNotification{
			Apns: &apiproto.ApnsPushNotification{Payload: testPayload()},
			Fcm:  &apiproto.FcmPushNotification{Message: testMessage()},
		},
	}
}

// requireRefusal checks that the request is refused with code, and that the message contains text.
func requireRefusal(t *testing.T, s *Sender, req *apiproto.SendPushNotificationRequest, code uint32, text string) {
	t.Helper()

	jobs, refusal := s.plan(req)
	require.Nil(t, jobs)
	require.NotNil(t, refusal)
	require.Equal(t, code, refusal.GetCode())
	require.Contains(t, refusal.GetMessage(), text)
}
