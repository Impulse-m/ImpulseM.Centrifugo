package webpush

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
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

// TestDefaultEndpointOriginsArePRODefaults checks the built-in origin patterns against the four patterns of the specification.
func TestDefaultEndpointOriginsArePRODefaults(t *testing.T) {
	expected := []string{
		"https://*.googleapis.com",
		"https://*.push.apple.com",
		"https://*.notify.windows.com",
		"https://*.push.services.mozilla.com",
	}
	require.Equal(t, expected, EndpointOrigins(nil, nil))
	require.Equal(t, expected, EndpointOrigins([]string{}, nil))
}

// TestDefaultCheckerAcceptsEachPushServiceDomain checks that a host of each push service domain of the built-in list is
// accepted by the default checker, and that an explicit :443 matches a pattern without a port.
func TestDefaultCheckerAcceptsEachPushServiceDomain(t *testing.T) {
	endpoints := []string{
		"https://fcm.googleapis.com/x",
		"https://web.push.apple.com/x",
		"https://wns.notify.windows.com/x",
		"https://updates.push.services.mozilla.com/x",
		"https://fcm.googleapis.com:443/x",
	}
	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			job := newTestJob(0, newTokenFor(t, endpoint), nil, `{"a":1}`)

			require.Empty(t, Refusal(job, defaultChecker(t)))
		})
	}
}

func TestEndpointOriginsEffectiveList(t *testing.T) {
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
		"42",
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

// TestCheckRequestRefusesTheTargetPayload checks the payload of one target: a target needs a payload, its own or the
// request's, and its own payload must be a JSON object. The request payload is checked once, by CheckNotification.
func TestCheckRequestRefusesTheTargetPayload(t *testing.T) {
	token := newTokenFor(t, "https://fcm.googleapis.com/x")

	require.Error(t, CheckRequest(newTestJob(0, token, nil, "")), "a target needs a payload")

	override := newTestJob(0, token, nil, "")
	override.Target.WebpushPayload = []byte(`{"a":1}`)
	require.NoError(t, CheckRequest(override), "the target payload replaces the request payload")

	notObject := newTestJob(0, token, nil, `{"a":1}`)
	notObject.Target.WebpushPayload = []byte(`[1]`)
	err := CheckRequest(notObject)
	require.Error(t, err, "a target payload that is not a JSON object")
	require.Contains(t, err.Error(), "webpush target 0 payload is not a JSON object")
}

// TestCheckNotificationRefusesAPayloadThatIsNotAnObject checks that the request payload is refused once for the request when
// it is not a JSON object and a target uses it, and that a payload over the scan bound is not scanned.
func TestCheckNotificationRefusesAPayloadThatIsNotAnObject(t *testing.T) {
	notification := &apiproto.PushNotification{
		Webpush: &apiproto.WebPushPushNotification{Payload: []byte("not json")},
	}
	require.Error(t, CheckNotification(notification, true))

	notification.Webpush.Payload = []byte(`{"a":1}`)
	require.NoError(t, CheckNotification(notification, true))

	notification.Webpush.Payload = []byte(strings.Repeat("x", provider.MaxScannedPayloadBytes+1))
	require.NoError(t, CheckNotification(notification, true), "a payload over the scan bound is not scanned")
}

// TestCheckNotificationSkipsAPayloadNoTargetUses checks that the request payload is not checked when no target uses it, so
// a payload that is not a JSON object is accepted, while the caller headers are still checked.
func TestCheckNotificationSkipsAPayloadNoTargetUses(t *testing.T) {
	notification := &apiproto.PushNotification{
		Webpush: &apiproto.WebPushPushNotification{Payload: []byte("not json")},
	}
	require.NoError(t, CheckNotification(notification, false), "a payload no target uses is not checked")

	notification.Webpush.Headers = map[string]string{"Ttl": "60"}
	require.Error(t, CheckNotification(notification, false), "the caller headers are checked even when no target uses the payload")
}

// TestCheckNotificationRefusesAnUnknownHeader checks that a caller header outside the three names is refused by
// CheckNotification, and that the error names no target.
func TestCheckNotificationRefusesAnUnknownHeader(t *testing.T) {
	notification := &apiproto.PushNotification{
		Webpush: &apiproto.WebPushPushNotification{
			Headers: map[string]string{"Ttl": "60"},
			Payload: []byte(`{"a":1}`),
		},
	}

	err := CheckNotification(notification, true)

	require.Error(t, err)
	require.Contains(t, err.Error(), "Ttl")
	require.NotContains(t, err.Error(), "target")
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
