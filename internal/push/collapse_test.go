package push

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// collapseAPNsCase is one case of the collapse id table of APNs. An empty caller value means the caller set no header.
type collapseAPNsCase struct {
	name            string
	requestCollapse string
	targetCollapse  string
	callerHeader    string
	refused         bool
}

// collapseFCMCase is one case of the collapse id table of FCM. An empty caller value means the caller set no field.
type collapseFCMCase struct {
	name            string
	requestCollapse string
	targetCollapse  string
	callerKey       string
	callerAPNs      string
	refused         bool
}

// apnsCollapseRequest returns a request with one APNs target and the caller header of the case.
func apnsCollapseRequest(testCase collapseAPNsCase) *apiproto.SendPushNotificationRequest {
	headers := map[string]string{}
	if testCase.callerHeader != "" {
		headers["apns-collapse-id"] = testCase.callerHeader
	}
	return &apiproto.SendPushNotificationRequest{
		CollapseId: testCase.requestCollapse,
		Recipient: &apiproto.PushRecipient{
			Targets: []*apiproto.PushTarget{
				{
					Provider:    provider.ProviderAPNs,
					Token:       "apns-token",
					Environment: "production",
					CollapseId:  testCase.targetCollapse,
				},
			},
		},
		Notification: &apiproto.PushNotification{
			Apns: &apiproto.ApnsPushNotification{
				Headers: headers,
				Payload: testPayload(),
			},
		},
	}
}

// fcmCollapseRequest returns a request with one FCM target and the caller fields of the case.
func fcmCollapseRequest(testCase collapseFCMCase) *apiproto.SendPushNotificationRequest {
	message := `{"notification":{"title":"hello"}`
	if testCase.callerKey != "" {
		message += fmt.Sprintf(`,"android":{"collapse_key":%q}`, testCase.callerKey)
	}
	if testCase.callerAPNs != "" {
		message += fmt.Sprintf(`,"apns":{"headers":{"apns-collapse-id":%q}}`, testCase.callerAPNs)
	}
	message += "}"

	return &apiproto.SendPushNotificationRequest{
		CollapseId: testCase.requestCollapse,
		Recipient: &apiproto.PushRecipient{
			Targets: []*apiproto.PushTarget{
				{
					Provider:   provider.ProviderFCM,
					Token:      "fcm-token",
					CollapseId: testCase.targetCollapse,
				},
			},
		},
		Notification: &apiproto.PushNotification{
			Fcm: &apiproto.FcmPushNotification{
				Message: []byte(message),
			},
		},
	}
}

// requireCollapseOutcome checks that the send answers the request with a 107 refusal when refused is set, and with
// results otherwise. Nothing else is refused, so a pass-through value never fails the request.
func requireCollapseOutcome(t *testing.T, req *apiproto.SendPushNotificationRequest, refused bool) {
	t.Helper()

	s := newTestSender(t, &countingSender{}, nil)
	resp := s.Send(context.Background(), req)

	if refused {
		require.NotNil(t, resp.GetError())
		require.Equal(t, uint32(107), resp.GetError().GetCode())
		return
	}
	require.Nil(t, resp.GetError())
	require.Len(t, resp.GetResult().GetResults(), 1)
	require.Equal(t, provider.StatusSent, resp.GetResult().GetResults()[0].GetStatus())
}

// TestCollapseIDTableAPNs checks the collapse id rule of an APNs target: when the resolved collapse id is set, a caller
// apns-collapse-id must equal it; when it is empty, the caller value passes unchanged.
func TestCollapseIDTableAPNs(t *testing.T) {
	testCases := []collapseAPNsCase{
		{name: "unset, no caller header", requestCollapse: "", targetCollapse: "", callerHeader: ""},
		{name: "unset, caller header passes", requestCollapse: "", targetCollapse: "", callerHeader: "any"},
		{name: "request set, caller header equal", requestCollapse: "a", callerHeader: "a"},
		{name: "request set, caller header different", requestCollapse: "a", callerHeader: "b", refused: true},
		{name: "request set, no caller header", requestCollapse: "a"},
		{name: "target set, caller header equal", requestCollapse: "a", targetCollapse: "b", callerHeader: "b"},
		{name: "target set, caller header of the request", requestCollapse: "a", targetCollapse: "b", callerHeader: "a", refused: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requireCollapseOutcome(t, apnsCollapseRequest(testCase), testCase.refused)
		})
	}
}

// TestCollapseIDTableFCM checks the collapse id rule of an FCM target: a caller android.collapse_key and a caller
// apns.headers.apns-collapse-id must equal a set resolved collapse id; when it is empty, both pass unchanged.
func TestCollapseIDTableFCM(t *testing.T) {
	testCases := []collapseFCMCase{
		{name: "unset, no caller fields"},
		{name: "unset, caller fields pass", callerKey: "any", callerAPNs: "other"},
		{name: "request set, collapse key equal", requestCollapse: "a", callerKey: "a"},
		{name: "request set, collapse key different", requestCollapse: "a", callerKey: "b", refused: true},
		{name: "request set, apns collapse id equal", requestCollapse: "a", callerAPNs: "a"},
		{name: "request set, apns collapse id different", requestCollapse: "a", callerAPNs: "b", refused: true},
		{name: "request set, no caller fields", requestCollapse: "a"},
		{name: "target set, collapse key of the request", requestCollapse: "a", targetCollapse: "b", callerKey: "a", refused: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requireCollapseOutcome(t, fcmCollapseRequest(testCase), testCase.refused)
		})
	}
}
