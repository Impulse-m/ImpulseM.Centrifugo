package webpush

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/origin"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// defaultChecker returns the origin patterns of a server with no configured origins.
func defaultChecker(t *testing.T) *origin.PatternChecker {
	t.Helper()

	checker, err := origin.NewPatternChecker(EndpointOrigins(nil, nil))
	require.NoError(t, err)
	return checker
}

// jsonPayload returns a JSON object of exactly size bytes.
func jsonPayload(size int) string {
	return `{"a":"` + strings.Repeat("x", size-8) + `"}`
}

func TestEndpointOfBuildsTheOriginFromTheParsedURL(t *testing.T) {
	testCases := []struct {
		name       string
		endpoint   string
		origin     string
		requestURL string
	}{
		{
			name:       "default port is dropped and the host is lower-cased",
			endpoint:   "https://FCM.GoogleAPIs.com:443/send/abc?x=1",
			origin:     "https://fcm.googleapis.com",
			requestURL: "https://fcm.googleapis.com/send/abc?x=1",
		},
		{
			name:       "another port is kept",
			endpoint:   "https://fcm.googleapis.com:8443/x",
			origin:     "https://fcm.googleapis.com:8443",
			requestURL: "https://fcm.googleapis.com:8443/x",
		},
		{
			name:       "a fragment is not sent",
			endpoint:   "https://fcm.googleapis.com/x#fragment",
			origin:     "https://fcm.googleapis.com",
			requestURL: "https://fcm.googleapis.com/x",
		},
		{
			name:       "an empty path is the root",
			endpoint:   "https://fcm.googleapis.com",
			origin:     "https://fcm.googleapis.com",
			requestURL: "https://fcm.googleapis.com/",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requestURL, endpointOrigin, ok := endpointOf(testCase.endpoint)

			require.True(t, ok)
			require.Equal(t, testCase.origin, endpointOrigin)
			require.Equal(t, testCase.requestURL, requestURL)
		})
	}
}

func TestEndpointOfRefusesWhatIsNotAPublicHTTPSName(t *testing.T) {
	endpoints := []string{
		"http://fcm.googleapis.com/x",
		"https://user@fcm.googleapis.com/x",
		"https://user:secret@fcm.googleapis.com/x",
		"https://127.0.0.1/x",
		"https://10.0.0.1:8443/x",
		"https://[::1]/x",
		"https://fcm.googleapis.com./x",
		"https://fcm.googleapis.com:0/x",
		"https://fcm.googleapis.com:99999/x",
		"https://exämple.com/x",
		"https:///x",
		"/relative/path",
	}
	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			_, _, ok := endpointOf(endpoint)

			require.False(t, ok)
		})
	}
}

func TestSubscriptionValuesAreItemRefusals(t *testing.T) {
	endpoint := "https://fcm.googleapis.com/x"
	receiver, auth := validKeyStrings(t)

	point := make([]byte, 65)
	point[0] = 0x04
	offCurve := base64.RawURLEncoding.EncodeToString(point)
	shortPoint := base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	shortAuth := base64.RawURLEncoding.EncodeToString(make([]byte, 15))
	longAuth := base64.RawURLEncoding.EncodeToString(make([]byte, 17))

	testCases := []struct {
		name     string
		token    string
		expected string
	}{
		{name: "endpoint is not https", token: newTokenWith(t, "http://fcm.googleapis.com/x", receiver, auth), expected: provider.CodeInvalidSubscription},
		{name: "endpoint has user info", token: newTokenWith(t, "https://u@fcm.googleapis.com/x", receiver, auth), expected: provider.CodeInvalidSubscription},
		{name: "endpoint host is an IP literal", token: newTokenWith(t, "https://203.0.113.9/x", receiver, auth), expected: provider.CodeInvalidSubscription},
		{name: "endpoint host ends with a dot", token: newTokenWith(t, "https://fcm.googleapis.com./x", receiver, auth), expected: provider.CodeInvalidSubscription},
		{name: "endpoint host is not ASCII", token: newTokenWith(t, "https://exämple.com/x", receiver, auth), expected: provider.CodeInvalidSubscription},
		{name: "p256dh is not 65 bytes", token: newTokenWith(t, endpoint, shortPoint, auth), expected: provider.CodeInvalidSubscription},
		{name: "p256dh is not a point of P-256", token: newTokenWith(t, endpoint, offCurve, auth), expected: provider.CodeInvalidSubscription},
		{name: "p256dh is not base64url", token: newTokenWith(t, endpoint, "!!!", auth), expected: provider.CodeInvalidSubscription},
		{name: "auth is 15 bytes", token: newTokenWith(t, endpoint, receiver, shortAuth), expected: provider.CodeInvalidSubscription},
		{name: "auth is 17 bytes", token: newTokenWith(t, endpoint, receiver, longAuth), expected: provider.CodeInvalidSubscription},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			job := newTestJob(0, testCase.token, nil, `{"a":1}`)

			require.Equal(t, testCase.expected, Refusal(job, defaultChecker(t)))
		})
	}
}

func TestSubscriptionKeysAcceptPadding(t *testing.T) {
	point, auth := validKeyBytes(t)
	padded := base64.URLEncoding.EncodeToString(point)
	paddedAuth := base64.URLEncoding.EncodeToString(auth)
	require.True(t, strings.HasSuffix(padded, "="))
	require.True(t, strings.HasSuffix(paddedAuth, "="))

	job := newTestJob(0, newTokenWith(t, "https://fcm.googleapis.com/x", padded, paddedAuth), nil, `{"a":1}`)

	require.Empty(t, Refusal(job, defaultChecker(t)))
}

func TestEndpointOriginIsCheckedAgainstThePatterns(t *testing.T) {
	testCases := []struct {
		name     string
		endpoint string
		expected string
	}{
		{name: "a listed push service", endpoint: "https://web.push.apple.com/x", expected: ""},
		{name: "a host of another domain", endpoint: "https://evil.example/x", expected: provider.CodeEndpointNotAllowed},
		{name: "a listed domain as a label of another", endpoint: "https://fcm.googleapis.com.evil.example/x", expected: provider.CodeEndpointNotAllowed},
		{name: "a port the pattern does not name", endpoint: "https://fcm.googleapis.com:8443/x", expected: provider.CodeEndpointNotAllowed},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			job := newTestJob(0, newTokenFor(t, testCase.endpoint), nil, `{"a":1}`)

			require.Equal(t, testCase.expected, Refusal(job, defaultChecker(t)))
		})
	}
}

func TestEndpointOriginsEffectiveList(t *testing.T) {
	require.Equal(t, DefaultEndpointOrigins, EndpointOrigins(nil, nil))
	require.Equal(t, DefaultEndpointOrigins, EndpointOrigins([]string{}, nil))
	require.Equal(t, append(append([]string{}, DefaultEndpointOrigins...), "https://push.example.com"),
		EndpointOrigins(nil, []string{"https://push.example.com"}))
	require.Equal(t, []string{"https://a.example"}, EndpointOrigins([]string{"https://a.example"}, nil))
	require.Equal(t, []string{"https://a.example", "https://b.example"},
		EndpointOrigins([]string{"https://a.example"}, []string{"https://b.example"}))

	effective := EndpointOrigins(nil, nil)
	effective[0] = "https://changed.example"
	require.NotEqual(t, "https://changed.example", DefaultEndpointOrigins[0], "the result must not alias the built-in list")
}

func TestEndpointOriginsMatchGlobs(t *testing.T) {
	checker, err := origin.NewPatternChecker(EndpointOrigins(nil, nil))
	require.NoError(t, err)
	require.True(t, checker.MatchOrigin("https://a.b.googleapis.com"), "a glob star crosses dots")

	star, err := origin.NewPatternChecker(EndpointOrigins([]string{"*"}, nil))
	require.NoError(t, err)
	require.True(t, star.MatchOrigin("https://anything.example:9000"))

	empty, err := origin.NewPatternChecker(EndpointOrigins([]string{"https://none.example"}, nil))
	require.NoError(t, err)
	require.False(t, empty.MatchOrigin("https://fcm.googleapis.com"))
}

func TestCheckRequestRefusesMalformedSubscriptions(t *testing.T) {
	endpoint := "https://fcm.googleapis.com/x"
	testCases := []string{
		"",
		"{",
		"[]",
		"null",
		`"https://fcm.googleapis.com/x"`,
		`{"endpoint":"` + endpoint + `"}`,
		`{"endpoint":1,"keys":{"p256dh":"a","auth":"b"}}`,
		`{"endpoint":"` + endpoint + `","keys":"abc"}`,
		`{"endpoint":"` + endpoint + `","keys":{"p256dh":1,"auth":"b"}}`,
		`{"endpoint":"` + endpoint + `","keys":{"p256dh":"a"}}`,
	}
	for _, token := range testCases {
		job := newTestJob(3, token, nil, `{"a":1}`)

		err := CheckRequest(job)

		require.Error(t, err)
		require.Contains(t, err.Error(), "webpush target 3")
	}
}

func TestCheckRequestRefusesTheShapeOfThePayloadAndEnvironment(t *testing.T) {
	token := newTokenFor(t, "https://fcm.googleapis.com/x")

	require.Error(t, CheckRequest(newTestJob(0, token, nil, "")), "a target needs a payload")
	require.Error(t, CheckRequest(newTestJob(0, token, nil, "not json")), "a payload that is not a JSON object")

	environment := newTestJob(0, token, nil, `{"a":1}`)
	environment.Target.Environment = "production"
	require.Error(t, CheckRequest(environment), "environment must be empty")

	override := newTestJob(0, token, nil, "")
	override.Target.WebpushPayload = []byte(`{"a":1}`)
	require.NoError(t, CheckRequest(override), "the target payload replaces the request payload")
}

func TestCheckRequestLeavesAnOversizePayloadToTheItem(t *testing.T) {
	token := newTokenFor(t, "https://fcm.googleapis.com/x")

	// Over the scan threshold the content is not checked, so the item is answered payload_too_large.
	oversize := newTestJob(0, token, nil, strings.Repeat("x", provider.MaxAPNsVoIPPayloadBytes+1))
	require.NoError(t, CheckRequest(oversize))
	require.Equal(t, provider.CodePayloadTooLarge, Refusal(oversize, defaultChecker(t)))
}

func TestRefusalPayloadCeiling(t *testing.T) {
	token := newTokenFor(t, "https://fcm.googleapis.com/x")

	atLimit := newTestJob(0, token, nil, jsonPayload(provider.MaxWebPushPayloadBytes))
	require.Empty(t, Refusal(atLimit, defaultChecker(t)))

	overLimit := newTestJob(0, token, nil, jsonPayload(provider.MaxWebPushPayloadBytes+1))
	require.Equal(t, provider.CodePayloadTooLarge, Refusal(overLimit, defaultChecker(t)))
}

func TestRefusalIsEmptyForAValidTarget(t *testing.T) {
	job := newTestJob(0, newTokenFor(t, "https://fcm.googleapis.com/x"), nil, `{"a":1}`)

	require.Empty(t, Refusal(job, defaultChecker(t)))
}
