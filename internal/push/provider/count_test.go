package provider

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// TestCountNotificationWebPushLabels checks that a Web Push result lands under the webpush provider and the web platform,
// with the bounded err_code of its result.
func TestCountNotificationWebPushLabels(t *testing.T) {
	initPushMetricsForTest(t)

	job := newTestJob(0, ProviderWebPush)
	countNotification(job, &apiproto.PushItemResult{Status: StatusNotSent, Code: CodeAddressBlocked})
	countNotification(job, &apiproto.PushItemResult{Status: StatusSent, HttpStatus: 201})

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.PushNotificationCount.WithLabelValues(
		ProviderWebPush, recipientToken, platformWeb, "false", errCodeAddressBlocked,
	)))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.PushNotificationCount.WithLabelValues(
		ProviderWebPush, recipientToken, platformWeb, "true", errCodeOK,
	)))
}

// Every result maps to one bounded err_code: a status class, a refusal or a not_sent target. A provider reason never
// reaches the label.
func TestErrCodeOf(t *testing.T) {
	testCases := []struct {
		name     string
		result   *apiproto.PushItemResult
		expected string
	}{
		{name: "sent", result: &apiproto.PushItemResult{Status: StatusSent, HttpStatus: 200}, expected: errCodeOK},
		{name: "not sent", result: &apiproto.PushItemResult{Status: StatusNotSent, Code: CodeConnectFailed}, expected: errCodeNotSent},
		{name: "unanswered", result: &apiproto.PushItemResult{Status: StatusUnanswered}, expected: errCodeUnanswered},
		{
			name:     "payload too large",
			result:   &apiproto.PushItemResult{Status: StatusRejected, Code: CodePayloadTooLarge},
			expected: errCodePayloadTooLarge,
		},
		{name: "expired", result: &apiproto.PushItemResult{Status: StatusRejected, Code: CodeExpired}, expected: errCodeExpired},
		{name: "http 400", result: &apiproto.PushItemResult{Status: StatusRejected, HttpStatus: 400}, expected: errCodeHTTP4xx},
		{name: "http 429", result: &apiproto.PushItemResult{Status: StatusRejected, HttpStatus: 429}, expected: errCodeHTTP4xx},
		{name: "http 503", result: &apiproto.PushItemResult{Status: StatusRejected, HttpStatus: 503}, expected: errCodeHTTP5xx},
		{name: "http 302", result: &apiproto.PushItemResult{Status: StatusRejected, HttpStatus: 302}, expected: errCodeHTTPOther},
		{name: "no status line", result: &apiproto.PushItemResult{Status: StatusRejected}, expected: errCodeHTTPOther},
		{
			name:     "invalid subscription",
			result:   &apiproto.PushItemResult{Status: StatusRejected, Code: CodeInvalidSubscription},
			expected: errCodeInvalidSubscription,
		},
		{
			name:     "endpoint not allowed",
			result:   &apiproto.PushItemResult{Status: StatusRejected, Code: CodeEndpointNotAllowed},
			expected: errCodeEndpointNotAllowed,
		},
		{
			name:     "invalid topic",
			result:   &apiproto.PushItemResult{Status: StatusRejected, Code: CodeInvalidTopic},
			expected: errCodeInvalidTopic,
		},
		{
			name:     "address blocked",
			result:   &apiproto.PushItemResult{Status: StatusNotSent, Code: CodeAddressBlocked},
			expected: errCodeAddressBlocked,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, errCodeOf(testCase.result))
		})
	}
}
