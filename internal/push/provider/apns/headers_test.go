package apns

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPNsHeaderAllowlist(t *testing.T) {
	const validID = "3f2b8a64-1c2d-4e5f-8a9b-0c1d2e3f4a5b"

	testCases := []struct {
		name        string
		headers     map[string]string
		collapseID  string
		expectError bool
	}{
		{name: "topic override", headers: map[string]string{"apns-topic": "com.example.voip"}},
		{name: "authorization refused", headers: map[string]string{"authorization": "bearer x"}, expectError: true},
		{name: "unknown name refused", headers: map[string]string{"x-custom": "1"}, expectError: true},
		{name: "valid lowercase id", headers: map[string]string{"apns-id": validID}},
		{name: "uppercase id refused", headers: map[string]string{"apns-id": "3F2B8A64-1C2D-4E5F-8A9B-0C1D2E3F4A5B"}, expectError: true},
		{name: "urn form refused", headers: map[string]string{"apns-id": "urn:uuid:" + validID}, expectError: true},
		{name: "braced form refused", headers: map[string]string{"apns-id": "{" + validID + "}"}, expectError: true},
		{name: "priority 7 refused", headers: map[string]string{"apns-priority": "7"}, expectError: true},
		{name: "priority 10 accepted", headers: map[string]string{"apns-priority": "10"}},
		{name: "priority 5 accepted", headers: map[string]string{"apns-priority": "5"}},
		{
			name:        "collapse id differs from resolved",
			headers:     map[string]string{"apns-collapse-id": "b"},
			collapseID:  "a",
			expectError: true,
		},
		{
			name:       "collapse id equal to resolved",
			headers:    map[string]string{"apns-collapse-id": "a"},
			collapseID: "a",
		},
		{
			name:    "collapse id passes unchanged when resolved is empty",
			headers: map[string]string{"apns-collapse-id": "caller"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := CheckHeaders(testCase.headers, testCase.collapseID)
			if testCase.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRequestHeadersServerValues(t *testing.T) {
	headers := requestHeaders(nil, "collapse-1", 1700000000, "com.example.app")

	require.Equal(t, "alert", headers["apns-push-type"])
	require.Equal(t, "com.example.app", headers["apns-topic"])
	require.Equal(t, "collapse-1", headers["apns-collapse-id"])
	require.Equal(t, "1700000000", headers["apns-expiration"])
}

func TestRequestHeadersCallerValuesPassThrough(t *testing.T) {
	caller := map[string]string{
		"apns-push-type":  "voip",
		"apns-topic":      "com.example.app.voip",
		"apns-expiration": "0",
		"apns-thread-id":  "thread",
		"apns-priority":   "5",
	}

	headers := requestHeaders(caller, "", 1700000000, "com.example.app")

	require.Equal(t, "voip", headers["apns-push-type"])
	require.Equal(t, "com.example.app.voip", headers["apns-topic"])
	require.Equal(t, "0", headers["apns-expiration"], "a caller expiration overrides the derived one")
	require.Equal(t, "thread", headers["apns-thread-id"])
	require.Equal(t, "5", headers["apns-priority"])
	require.NotContains(t, headers, "apns-collapse-id")
}

func TestMaxPayloadBytesByPushType(t *testing.T) {
	require.Equal(t, 4096, maxPayloadBytes("alert"))
	require.Equal(t, 5120, maxPayloadBytes("voip"))
}
