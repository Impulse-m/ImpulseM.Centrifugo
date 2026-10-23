package fcm

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// detailsOfInvalidTTL is the raw details array of an INVALID_ARGUMENT answer about the time to live of the message.
const detailsOfInvalidTTL = `[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"INVALID_ARGUMENT"},` +
	`{"@type":"type.googleapis.com/google.rpc.BadRequest","fieldViolations":[{"field":"message.android.ttl","description":"Invalid ttl"}]}]`

// newDetailsFake returns an FCM fake that answers 400 with the raw details array in the error body.
func newDetailsFake(t *testing.T, details string) *fakeFCM {
	t.Helper()

	return newFakeFCM(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"error":{"code":400,"message":"bad request","status":"INVALID_ARGUMENT","details":` + details + `}}`))
	})
}

// TestFCMProviderDetailsPassThroughByteForByte checks that the raw details of an answer within the limit reach the result
// unchanged, and that the first field violation names the bad field.
func TestFCMProviderDetailsPassThroughByteForByte(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newDetailsFake(t, detailsOfInvalidTTL)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Equal(t, "message.android.ttl", result.GetProviderDetail())
	require.Equal(t, detailsOfInvalidTTL, string(result.GetProviderDetails()))
	require.False(t, result.GetProviderDetailsTruncated())
}

// TestFCMProviderDetailsCutAtElementBoundary checks that an oversized details array is cut at an element boundary: the
// result is a valid JSON array of whole elements, within the limit, and the cut is reported.
func TestFCMProviderDetailsCutAtElementBoundary(t *testing.T) {
	element := `{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"` + strings.Repeat("R", 180) + `"}`
	elements := make([]string, 40)
	for index := range elements {
		elements[index] = element
	}
	tokens := newFakeToken(t, false)
	fake := newDetailsFake(t, "["+strings.Join(elements, ",")+"]")
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.True(t, result.GetProviderDetailsTruncated())
	require.LessOrEqual(t, len(result.GetProviderDetails()), maxProviderDetailsBytes)

	var kept []json.RawMessage
	require.NoError(t, json.Unmarshal(result.GetProviderDetails(), &kept))
	require.NotEmpty(t, kept)
	require.Less(t, len(kept), len(elements))
	for _, whole := range kept {
		require.Equal(t, element, string(whole))
	}
}

// TestFCMProviderDetailsOverLimitByWhitespaceIsFlagged checks that an array over the limit only because of its whitespace
// keeps all its elements, comes back compacted, and is flagged: the bytes are then not the provider's, so the flag is set.
func TestFCMProviderDetailsOverLimitByWhitespaceIsFlagged(t *testing.T) {
	element := `{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"SMALL"}`
	elements := make([]string, 20)
	for index := range elements {
		elements[index] = element
	}
	details := "[" + strings.Repeat(" ", 2*maxProviderDetailsBytes) + strings.Join(elements, strings.Repeat(" ", 200)+",") + "]"
	require.Greater(t, len(details), maxProviderDetailsBytes)

	tokens := newFakeToken(t, false)
	fake := newDetailsFake(t, details)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.True(t, result.GetProviderDetailsTruncated())
	require.LessOrEqual(t, len(result.GetProviderDetails()), maxProviderDetailsBytes)
	var kept []json.RawMessage
	require.NoError(t, json.Unmarshal(result.GetProviderDetails(), &kept))
	require.Len(t, kept, len(elements))
}

// TestFCMProviderDetailsEmptyForSuccess checks that a success carries no raw details.
func TestFCMProviderDetailsEmptyForSuccess(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Empty(t, result.GetProviderDetails())
	require.False(t, result.GetProviderDetailsTruncated())
}
