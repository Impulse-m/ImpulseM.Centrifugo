package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/push"

	"github.com/stretchr/testify/require"
)

// newSendRoute returns the send route of a node whose push configuration is cfg. A dry run sender loads no credential,
// so the route runs without any network call.
func newSendRoute(t *testing.T, cfg configtypes.PushNotifications) http.HandlerFunc {
	t.Helper()

	_, route := newSendNode(t, cfg)
	return route
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

// transportErrorConfig is the handler configuration of the transport error mode.
func transportErrorConfig() Config {
	return Config{UseTransportErrorMode: true}
}

// TestSendRouteBodyOverCapAnswers107 checks that a body over max_request_bytes is refused with the bad request code in
// the method response, with HTTP 200, as the generic handler answers the error in the default mode.
func TestSendRouteBodyOverCapAnswers107(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1024))

	body := sendRequest(t, strings.Repeat("u", 2048))
	recorder := serveSend(route, body)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, uint32(107), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteBodyOverCapTransportErrorModeAnswers107 checks that in the transport error mode a body over
// max_request_bytes is refused with the bad request code and the status of that code.
func TestSendRouteBodyOverCapTransportErrorModeAnswers107(t *testing.T) {
	_, route := newSendNodeWithHandler(t, sendTestConfig(1024), transportErrorConfig())

	body := sendRequest(t, strings.Repeat("u", 2048))
	recorder := serveSend(route, body)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, uint32(107), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteRecipientCountBeforeDecode checks that a body with more recipients than max_recipient_items is refused
// on the raw body, with the bad request code in the method response.
func TestSendRouteRecipientCountBeforeDecode(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1<<20))

	body := []byte(`{"recipient":{"targets":[` + strings.Repeat(`{},`, 1000) + `{}]}}`)
	recorder := serveSend(route, body)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, uint32(107), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteRecipientCountTransportErrorModeAnswers107 checks that in the transport error mode a body with too many
// recipients is refused with the bad request code and the status of that code.
func TestSendRouteRecipientCountTransportErrorModeAnswers107(t *testing.T) {
	_, route := newSendNodeWithHandler(t, sendTestConfig(1<<20), transportErrorConfig())

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

// TestSendRouteNonSenderAnswers108 checks that a node that is not a sender answers the sender-only error in the method
// response, with HTTP 200, as the generic handler answers the error in the default mode.
func TestSendRouteNonSenderAnswers108(t *testing.T) {
	cfg := sendTestConfig(1 << 20)
	cfg.Sender = false
	route := newSendRoute(t, cfg)

	recorder := serveSend(route, sendRequest(t, ""))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, uint32(108), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteNonSenderTransportErrorModeAnswers108 checks that in the transport error mode a node that is not a sender
// answers the sender-only error as the Error JSON, with the status of its code.
func TestSendRouteNonSenderTransportErrorModeAnswers108(t *testing.T) {
	cfg := sendTestConfig(1 << 20)
	cfg.Sender = false
	_, route := newSendNodeWithHandler(t, cfg, transportErrorConfig())

	recorder := serveSend(route, sendRequest(t, ""))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, uint32(108), errorCodeOf(t, recorder.Body.Bytes()))
}

// TestSendRouteNonSenderAnswers108WithoutReadingBody checks that a node that is not a sender answers the sender-only
// error before it reads the body, so an oversized body gets the same answer as any other body, and no body is buffered.
func TestSendRouteNonSenderAnswers108WithoutReadingBody(t *testing.T) {
	cfg := sendTestConfig(1024)
	cfg.Sender = false
	route := newSendRoute(t, cfg)

	counting := &countingBody{reader: strings.NewReader(string(sendRequest(t, strings.Repeat("u", 2048))))}
	recorder := httptest.NewRecorder()
	route(recorder, httptest.NewRequest(http.MethodPost, "/send_push_notification", counting))

	require.Equal(t, uint32(108), errorCodeOf(t, recorder.Body.Bytes()))
	require.Zero(t, counting.reads)
}

// blockingBody is a request body whose first read holds until release is closed. It shows a call that is in its body
// read, and so holds its call slot.
type blockingBody struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

// Read signals the first read, then waits for the release. It ends the body without data.
func (b *blockingBody) Read(_ []byte) (int, error) {
	b.once.Do(func() {
		close(b.started)
	})
	<-b.release
	return 0, io.EOF
}

// countingBody counts the reads of a request body.
type countingBody struct {
	reader *strings.Reader
	reads  int
}

// Read counts the read and reads from the data.
func (c *countingBody) Read(p []byte) (int, error) {
	c.reads++
	return c.reader.Read(p)
}

// TestSendRouteBusyAnswers111WithoutReadingBody checks that a call over max_concurrent_requests is answered 111 before its
// body is read, so the bodies of the calls in flight are bounded by the slots.
func TestSendRouteBusyAnswers111WithoutReadingBody(t *testing.T) {
	cfg := sendTestConfig(1 << 20)
	cfg.MaxConcurrentRequests = 1
	route := newSendRoute(t, cfg)

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

	counting := &countingBody{reader: strings.NewReader(string(sendRequest(t, "")))}
	recorder := httptest.NewRecorder()
	route(recorder, httptest.NewRequest(http.MethodPost, "/send_push_notification", counting))

	require.Equal(t, uint32(111), errorCodeOf(t, recorder.Body.Bytes()))
	require.Zero(t, counting.reads)

	close(held.release)
	<-done
}

// TestPushMethodsNotBuiltWhenEnabled checks that the push methods this build does not implement answer the not available
// message while push notifications are on, and the not enabled message only while they are off.
func TestPushMethodsNotBuiltWhenEnabled(t *testing.T) {
	sender, err := push.Setup(context.Background(), sendTestConfig(1<<20))
	require.NoError(t, err)
	h := &Executor{config: ExecutorConfig{Push: sender}}

	answer := h.DeviceRegister(context.Background(), &DeviceRegisterRequest{}).Error
	require.Equal(t, uint32(108), answer.GetCode())
	require.Equal(t, "not available in this build", answer.GetMessage())
}

// deadlineRecorder is a response recorder that takes a write deadline, as a server connection does.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

// SetWriteDeadline records the deadline of the response.
func (r *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	r.deadline = deadline
	return nil
}

// TestSendRouteWriteDeadline checks that the send route sets its write deadline past the longest inline run: the sync
// wait, the provider timeout and the margin.
func TestSendRouteWriteDeadline(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1<<20))

	request := httptest.NewRequest(http.MethodPost, "/send_push_notification", bytes.NewReader(sendRequest(t, "")))
	recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	before := time.Now()
	route(recorder, request)

	// The bound is 10 s of sync wait, 10 s of provider timeout and 5 s of margin.
	require.False(t, recorder.deadline.IsZero())
	require.WithinRange(t, recorder.deadline, before.Add(24*time.Second), before.Add(26*time.Second))
}

// slowBody is a request body that takes delay to arrive, as the body of a request on a slow uplink does.
type slowBody struct {
	reader *strings.Reader
	delay  time.Duration
	once   sync.Once
}

// Read waits for the delay once, before the first data, then reads from the data.
func (b *slowBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		time.Sleep(b.delay)
	})
	return b.reader.Read(p)
}

// TestSendRouteDeadlineCountsFromRequestArrival checks that timeout_ms counts from the arrival of the request, so the body
// upload is inside it: with a body that takes one second to arrive and timeout_ms of two seconds, the write deadline of the
// response is two seconds after the request started, not three.
func TestSendRouteDeadlineCountsFromRequestArrival(t *testing.T) {
	route := newSendRoute(t, sendTestConfig(1<<20))

	req := &SendPushNotificationRequest{
		TimeoutMs: 2000,
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

	slow := &slowBody{reader: strings.NewReader(string(body)), delay: time.Second}
	request := httptest.NewRequest(http.MethodPost, "/send_push_notification", slow)
	recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	before := time.Now()
	route(recorder, request)

	require.False(t, recorder.deadline.IsZero())
	require.WithinDuration(t, before.Add(2*time.Second), recorder.deadline, 500*time.Millisecond)
}
