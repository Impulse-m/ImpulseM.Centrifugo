package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// retryAfterKey is the metadata key of the seconds to wait before a retry, the key of a rate limited registry error.
const retryAfterKey = "x-centrifugo-retry-after"

// errorAnswer is the error of a send refusal: the Error JSON itself in the transport error mode, or the error of the
// method response otherwise.
type errorAnswer struct {
	Code      uint32            `json:"code"`
	Temporary bool              `json:"temporary"`
	Metadata  map[string]string `json:"metadata"`
	Error     *errorAnswer      `json:"error"`
}

// busyConfig returns a sender configuration in dry run with one call slot, so a second call is answered 111.
func busyConfig() configtypes.PushNotifications {
	cfg := sendTestConfig(1 << 20)
	cfg.MaxConcurrentRequests = 1
	return cfg
}

// holdSendSlot takes the only call slot of the route. It returns the function that releases the slot.
func holdSendSlot(t *testing.T, route http.HandlerFunc) func() {
	t.Helper()

	held := &blockingBody{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		route(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/send_push_notification", held))
	}()
	<-held.started

	return func() {
		close(held.release)
		<-done
	}
}

// TestSendRouteBusyCarriesRetryAfterInMethodResponse checks that a call over max_concurrent_requests is a temporary 111
// whose metadata says to wait one second, in the error mode of the method response.
func TestSendRouteBusyCarriesRetryAfterInMethodResponse(t *testing.T) {
	_, route := newSendNodeWithHandler(t, busyConfig(), Config{})
	release := holdSendSlot(t, route)
	defer release()

	recorder := serveSend(route, sendRequest(t, ""))

	var answer errorAnswer
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
	require.NotNil(t, answer.Error)
	require.Equal(t, uint32(111), answer.Error.Code)
	require.True(t, answer.Error.Temporary)
	require.Equal(t, "1", answer.Error.Metadata[retryAfterKey])
}

// TestSendRouteBusyCarriesRetryAfterInTransportErrorMode checks the same refusal in the transport error mode: the Error
// JSON carries the metadata, and the HTTP status is 429.
func TestSendRouteBusyCarriesRetryAfterInTransportErrorMode(t *testing.T) {
	_, route := newSendNodeWithHandler(t, busyConfig(), transportErrorConfig())
	release := holdSendSlot(t, route)
	defer release()

	recorder := serveSend(route, sendRequest(t, ""))

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	var answer errorAnswer
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
	require.Equal(t, uint32(111), answer.Code)
	require.True(t, answer.Temporary)
	require.Equal(t, "1", answer.Metadata[retryAfterKey])
}
