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

// pushTestLargeBodySize is a body size above the default 1 MiB request cap of
// direct send, so the test shows that push off does not refuse by size first.
const pushTestLargeBodySize = 2 << 20

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

// TestPushMethodsNotAvailableAnswerFreshError checks that two answers do not
// share one error value, so a caller that changes its answer cannot change
// the answer of another call.
func TestPushMethodsNotAvailableAnswerFreshError(t *testing.T) {
	h := &Executor{}
	ctx := context.Background()

	first := h.SendPushNotification(ctx, &SendPushNotificationRequest{}).Error
	second := h.SendPushNotification(ctx, &SendPushNotificationRequest{}).Error

	require.NotSame(t, first, second)
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

// spaceReader serves an endless run of spaces and counts the bytes it hands out.
// Spaces after a JSON object are valid JSON, so the body stays decodable.
type spaceReader struct {
	read int64
	max  int64
}

func (s *spaceReader) Read(p []byte) (int, error) {
	if s.read >= s.max {
		return 0, io.EOF
	}
	n := int64(len(p))
	if s.read+n > s.max {
		n = s.max - s.read
	}
	for i := range p[:n] {
		p[i] = ' '
	}
	s.read += n
	return int(n), nil
}

// TestPushRouteAnswers108ForLargeBody checks that a body above the direct send
// cap still answers 108 on every push route while push is off: the size check
// belongs to the send path, which runs only when push is on.
func TestPushRouteAnswers108ForLargeBody(t *testing.T) {
	routes := newPushTestRoutes(t)

	for _, path := range pushRoutePaths {
		body := io.MultiReader(
			strings.NewReader(`{}`),
			&spaceReader{max: pushTestLargeBodySize},
		)
		rec := httptest.NewRecorder()
		routes[path](rec, httptest.NewRequest(http.MethodPost, "/api"+path, body))

		require.Equal(t, http.StatusOK, rec.Code, path)
		require.Contains(t, rec.Body.String(), `"code":108`, path)
	}
}
