package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/push"

	"github.com/stretchr/testify/require"
)

// newSendRoute returns the send route of a node whose push configuration is cfg. A dry run sender loads no credential,
// so the route runs without any network call.
func newSendRoute(t *testing.T, cfg configtypes.PushNotifications) http.HandlerFunc {
	t.Helper()

	n := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	sender, err := push.Setup(context.Background(), cfg)
	require.NoError(t, err)

	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	apiExecutor := NewExecutor(n, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", Push: sender})
	return NewHandler(n, apiExecutor, Config{}).Routes()["/send_push_notification"]
}

// sendTestConfig returns a sender configuration in dry run, with the given request size cap.
func sendTestConfig(maxRequestBytes int) configtypes.PushNotifications {
	return configtypes.PushNotifications{
		Enabled:               true,
		Sender:                true,
		DryRun:                true,
		EnabledProviders:      []string{"apns", "fcm"},
		MaxRecipientItems:     1000,
		MaxRequestBytes:       maxRequestBytes,
		MaxConcurrentRequests: 64,
		SyncTimeout:           configtypes.Duration(10 * time.Second),
		Providers: configtypes.PushProviders{
			RequestTimeout: configtypes.Duration(10 * time.Second),
		},
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
	}
}

// sendRequest returns the JSON body of a request to one APNs target.
func sendRequest(t *testing.T, uid string) []byte {
	t.Helper()

	req := &SendPushNotificationRequest{
		Uid: uid,
		Recipient: &PushRecipient{
			Targets: []*PushTarget{
				{Provider: "apns", Token: "device", Environment: "production"},
			},
		},
		Notification: &PushNotification{
			Apns: &ApnsPushNotification{Payload: []byte(`{"aps":{"alert":"hi"}}`)},
		},
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	return body
}

// serveSend posts body to the route and returns the recorder.
func serveSend(route http.HandlerFunc, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/send_push_notification", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	route(recorder, request)
	return recorder
}

// errorCodeOf returns the code of the Error JSON of a body, or zero when the body has no error.
func errorCodeOf(t *testing.T, body []byte) uint32 {
	t.Helper()

	var answer struct {
		Code  uint32 `json:"code"`
		Error *struct {
			Code uint32 `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &answer))
	if answer.Error != nil {
		return answer.Error.Code
	}
	return answer.Code
}

// TestSendRouteBodyOverCapAnswers107 checks that a body over max_request_bytes is refused with the bad request code and
// the status of that code, not with a server error.
func TestSendRouteBodyOverCapAnswers107(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1024))

	body := sendRequest(t, strings.Repeat("u", 2048))
	recorder := serveSend(route, body)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, uint32(107), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteRecipientCountBeforeDecode checks that a body with more recipients than max_recipient_items is refused
// on the raw body, with the bad request code.
func TestSendRouteRecipientCountBeforeDecode(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1<<20))

	body := []byte(`{"recipient":{"targets":[` + strings.Repeat(`{},`, 1000) + `{}]}}`)
	recorder := serveSend(route, body)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, uint32(107), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteSendsValidRequest checks that a valid request reaches the send path and gets one result per recipient.
func TestSendRouteSendsValidRequest(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1<<20))

	recorder := serveSend(route, sendRequest(t, "caller-uid"))

	require.Equal(t, http.StatusOK, recorder.Code)
	var answer SendPushNotificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
	require.Nil(t, answer.GetError())
	require.Equal(t, "caller-uid", answer.GetResult().GetUid())
	require.Len(t, answer.GetResult().GetResults(), 1)
	require.Equal(t, "dry_run", answer.GetResult().GetResults()[0].GetStatus())
}

// TestSendRouteNonSenderAnswers108 checks that a node that is not a sender answers the sender-only error.
func TestSendRouteNonSenderAnswers108(t *testing.T) {
	cfg := sendTestConfig(1 << 20)
	cfg.Sender = false
	route := newSendRoute(t, cfg)

	recorder := serveSend(route, sendRequest(t, ""))

	require.Equal(t, uint32(108), errorCodeOf(t, recorder.Body.Bytes()))
}
