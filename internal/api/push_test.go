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

// pushRoutePaths lists the HTTP paths of the push notification methods.
var pushRoutePaths = []string{
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
}

// newPushTestRoutes returns the API routes of a node that has no push
// notification subsystem, the state of a deployment with push off.
func newPushTestRoutes(t *testing.T) map[string]http.HandlerFunc {
	t.Helper()

	n := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	apiExecutor := NewExecutor(n, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test", UseOpenTelemetry: false})
	apiHandler := NewHandler(n, apiExecutor, Config{})
	return apiHandler.Routes()
}

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

// TestPushRouteAnswers108 checks that every push notification route exists and
// answers 108 in the response body over HTTP, not 404 or 405.
func TestPushRouteAnswers108(t *testing.T) {
	routes := newPushTestRoutes(t)

	for _, path := range pushRoutePaths {
		handler, ok := routes[path]
		require.True(t, ok, path)

		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodPost, "/api"+path, strings.NewReader(`{}`)))

		require.Equal(t, http.StatusOK, rec.Code, path)
		require.Contains(t, rec.Body.String(), `"code":108`, path)
		require.Contains(t, rec.Body.String(), "push notifications are not enabled", path)
	}
}

// countingReader serves an endless body and counts the bytes it hands out.
type countingReader struct {
	read int64
	max  int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.read >= c.max {
		return 0, io.EOF
	}
	n := int64(len(p))
	if c.read+n > c.max {
		n = c.max - c.read
	}
	for i := range p[:n] {
		p[i] = 'a'
	}
	c.read += n
	return int(n), nil
}

// TestPushRouteBodyLimit checks that an oversized body on a push route answers
// 107 and that the route does not read the body past the cap.
func TestPushRouteBodyLimit(t *testing.T) {
	routes := newPushTestRoutes(t)

	for _, path := range pushRoutePaths {
		body := &countingReader{max: 64 << 20}
		rec := httptest.NewRecorder()
		routes[path](rec, httptest.NewRequest(http.MethodPost, "/api"+path, body))

		require.Equal(t, http.StatusOK, rec.Code, path)
		require.Contains(t, rec.Body.String(), `"code":107`, path)
		require.LessOrEqual(t, body.read, int64(pushMaxBodyBytes+1), path)
	}
}
