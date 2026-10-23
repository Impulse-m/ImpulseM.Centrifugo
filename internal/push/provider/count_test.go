package provider

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
)

// Every result maps to one bounded err_code: a status class, a refusal or a pending wait. A provider reason never
// reaches the label.
func TestErrCodeOf(t *testing.T) {
	testCases := []struct {
		name     string
		result   *apiproto.PushItemResult
		expected string
	}{
		{name: "sent", result: &apiproto.PushItemResult{Status: StatusSent, HttpStatus: 200}, expected: errCodeOK},
		{name: "pending", result: &apiproto.PushItemResult{Status: StatusPending}, expected: errCodeNotSent},
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
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, errCodeOf(testCase.result))
		})
	}
}
