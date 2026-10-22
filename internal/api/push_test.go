package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"

	"github.com/stretchr/testify/require"
)

// TestPushMethodsNotAvailable checks that every push notification method
// answers 108 with the same message while the subsystem is not available.
func TestPushMethodsNotAvailable(t *testing.T) {
	h := &Executor{}
	ctx := context.Background()

	answers := []*Error{
		h.DeviceRegister(ctx, &DeviceRegisterRequest{}).Error,
		h.DeviceUpdate(ctx, &DeviceUpdateRequest{}).Error,
		h.DeviceRemove(ctx, &DeviceRemoveRequest{}).Error,
		h.DeviceList(ctx, &DeviceListRequest{}).Error,
		h.DeviceTopicList(ctx, &DeviceTopicListRequest{}).Error,
		h.DeviceTopicUpdate(ctx, &DeviceTopicUpdateRequest{}).Error,
		h.UserTopicList(ctx, &UserTopicListRequest{}).Error,
		h.UserTopicUpdate(ctx, &UserTopicUpdateRequest{}).Error,
		h.SendPushNotification(ctx, &SendPushNotificationRequest{}).Error,
		h.UpdatePushStatus(ctx, &UpdatePushStatusRequest{}).Error,
		h.CancelPush(ctx, &CancelPushRequest{}).Error,
	}

	for _, answer := range answers {
		require.NotNil(t, answer)
		require.Equal(t, uint32(108), answer.Code)
		require.Equal(t, "push notifications are not enabled", answer.Message)
		require.False(t, answer.Temporary)
	}
}

// TestPushRoutesRegistered checks that the HTTP routes of the push notification
// methods exist, so the methods answer 108 over HTTP and do not return 404.
func TestPushRoutesRegistered(t *testing.T) {
	routes := (&Handler{}).Routes()

	for _, path := range []string{
		"/device_register",
		"/device_update",
		"/device_remove",
		"/device_list",
		"/device_topic_update",
		"/device_topic_list",
		"/user_topic_update",
		"/user_topic_list",
		"/send_push_notification",
		"/update_push_status",
		"/cancel_push",
	} {
		require.NotNil(t, routes[path], path)
	}
}

// TestPushRouteAnswers108 checks that a push notification method called over
// HTTP answers 108 in the response body, not 404 or 405.
func TestPushRouteAnswers108(t *testing.T) {
	n := nodeWithMemoryEngine()
	defer func() { _ = n.Shutdown(context.Background()) }()

	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	apiExecutor := NewExecutor(n, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", UseOpenTelemetry: false})
	apiHandler := NewHandler(n, apiExecutor, Config{})

	mux := http.NewServeMux()
	for path, handler := range apiHandler.Routes() {
		mux.Handle("/api"+path, handler)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/send_push_notification", strings.NewReader(`{}`))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), `"code":108`)
	require.Contains(t, string(body), "push notifications are not enabled")
}
