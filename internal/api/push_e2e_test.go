package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/push"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// newSendNode returns the executor and the send route of one node whose push configuration is cfg. A dry run sender
// loads no credential, so the node makes no network call.
func newSendNode(t *testing.T, cfg configtypes.PushNotifications) (*Executor, http.HandlerFunc) {
	t.Helper()

	n := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	sender, err := push.Setup(context.Background(), cfg)
	require.NoError(t, err)

	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	apiExecutor := NewExecutor(n, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", Push: sender})
	return apiExecutor, NewHandler(n, apiExecutor, Config{}).Routes()["/send_push_notification"]
}

// directSendRequest returns a request with two APNs targets in different environments and one FCM target. An expireAt
// in the past refuses every target before any call.
func directSendRequest(expireAt int64) *SendPushNotificationRequest {
	return &SendPushNotificationRequest{
		Uid: "e2e-uid",
		Recipient: &PushRecipient{
			Targets: []*PushTarget{
				{Provider: "apns", Token: "apns-prod", Environment: "production", Ref: "a"},
				{Provider: "apns", Token: "apns-dev", Environment: "development", Ref: "b"},
				{Provider: "fcm", Token: "fcm-1", Ref: "c"},
			},
		},
		Notification: &PushNotification{
			ExpireAt: expireAt,
			Apns:     &ApnsPushNotification{Payload: []byte(`{"aps":{"alert":"hi"}}`)},
			Fcm:      &FcmPushNotification{Message: []byte(`{"notification":{"title":"hi"}}`)},
		},
	}
}

// TestDirectSendEndToEndHTTP posts a mixed request to the send route, and then one whose notification has expired.
func TestDirectSendEndToEndHTTP(t *testing.T) {
	_, route := newSendNode(t, sendTestConfig(1<<20))

	body, err := json.Marshal(directSendRequest(0))
	require.NoError(t, err)
	recorder := serveSend(route, body)

	require.Equal(t, http.StatusOK, recorder.Code)
	var answer SendPushNotificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
	require.Nil(t, answer.GetError())
	require.Equal(t, "e2e-uid", answer.GetResult().GetUid())
	require.Len(t, answer.GetResult().GetResults(), 3)
	for index, result := range answer.GetResult().GetResults() {
		require.Equal(t, uint32(index), result.GetIndex())
		require.Equal(t, "dry_run", result.GetStatus(), "the dry run of the test config makes no provider call")
	}

	body, err = json.Marshal(directSendRequest(1))
	require.NoError(t, err)
	recorder = serveSend(route, body)

	require.Equal(t, http.StatusOK, recorder.Code)
	var expired SendPushNotificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &expired))
	require.Nil(t, expired.GetError())
	require.Len(t, expired.GetResult().GetResults(), 3)
	for _, result := range expired.GetResult().GetResults() {
		require.Equal(t, "rejected", result.GetStatus())
		require.Equal(t, "expired", result.GetCode())
	}
}

// TestDirectSendEndToEndGRPC sends the request of the HTTP check through the gRPC service of the same node, and checks
// that each result is the result the HTTP route gives.
func TestDirectSendEndToEndGRPC(t *testing.T) {
	executor, route := newSendNode(t, sendTestConfig(1<<20))
	service := newGRPCAPIService(nil, executor, GRPCAPIServiceConfig{})

	grpcResponse, err := service.SendPushNotification(context.Background(), directSendRequest(0))
	require.NoError(t, err)
	require.Nil(t, grpcResponse.GetError())

	body, err := json.Marshal(directSendRequest(0))
	require.NoError(t, err)
	recorder := serveSend(route, body)
	var httpAnswer SendPushNotificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &httpAnswer))

	grpcResults := grpcResponse.GetResult().GetResults()
	httpResults := httpAnswer.GetResult().GetResults()
	require.Len(t, grpcResults, len(httpResults))
	for index := range grpcResults {
		require.True(t, proto.Equal(grpcResults[index], httpResults[index]), "result %d differs between the protocols", index)
	}
}
