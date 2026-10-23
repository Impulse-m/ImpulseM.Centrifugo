package push

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// routingSender is a provider fake. It records the job of each token, and answers the way its provider does: an APNs
// token named gone answers 410 Unregistered, and every other target answers 200 with a message id.
type routingSender struct {
	mu    sync.Mutex
	calls int
	jobs  map[string]*provider.Job
}

// newRoutingSender returns an empty routingSender.
func newRoutingSender() *routingSender {
	return &routingSender{
		jobs: make(map[string]*provider.Job),
	}
}

// Send records the job and answers it like its provider.
func (r *routingSender) Send(_ context.Context, job *provider.Job) *apiproto.PushItemResult {
	r.mu.Lock()
	r.calls++
	r.jobs[job.Target.GetToken()] = job
	r.mu.Unlock()

	if job.Target.GetToken() == "gone" {
		result := provider.NewResult(job, provider.StatusRejected)
		result.HttpStatus = 410
		result.ProviderCode = "Unregistered"
		return result
	}
	result := provider.NewResult(job, provider.StatusSent)
	result.HttpStatus = 200
	result.ProviderMessageId = "message-" + job.Target.GetToken()
	return result
}

// directRequest returns a request with two APNs targets in different environments and one FCM target. Its payload and
// message carry marker text, which the logging check looks for.
func directRequest(expireAt int64) *apiproto.SendPushNotificationRequest {
	return &apiproto.SendPushNotificationRequest{
		Uid: "e2e-uid",
		Recipient: &apiproto.PushRecipient{
			Targets: []*apiproto.PushTarget{
				{Provider: provider.ProviderAPNs, Token: "prod-token", Environment: provider.EnvironmentProduction, Ref: "ref-prod"},
				{Provider: provider.ProviderAPNs, Token: "gone", Environment: provider.EnvironmentDevelopment, Ref: "ref-gone"},
				{Provider: provider.ProviderFCM, Token: "fcm-token", Ref: "ref-fcm"},
			},
		},
		Notification: &apiproto.PushNotification{
			ExpireAt: expireAt,
			Apns:     &apiproto.ApnsPushNotification{Payload: []byte(`{"aps":{"alert":"payload-marker"}}`)},
			Fcm:      &apiproto.FcmPushNotification{Message: []byte(`{"notification":{"title":"message-marker"}}`)},
		},
	}
}

// TestDirectSendEndToEnd sends a mixed request through the sender to provider fakes, and checks the results and the
// request each provider received. An expired request makes no provider call.
func TestDirectSendEndToEnd(t *testing.T) {
	fake := newRoutingSender()
	s := newTestSender(t, fake, nil)

	resp := s.Send(context.Background(), directRequest(0))

	require.Nil(t, resp.GetError())
	require.Equal(t, "e2e-uid", resp.GetResult().GetUid())
	results := resp.GetResult().GetResults()
	require.Len(t, results, 3)
	for index, result := range results {
		require.Equal(t, uint32(index), result.GetIndex())
	}

	require.Equal(t, provider.StatusSent, results[0].GetStatus())
	require.Equal(t, "ref-prod", results[0].GetRef())
	require.Equal(t, uint32(200), results[0].GetHttpStatus())
	require.Equal(t, "message-prod-token", results[0].GetProviderMessageId())
	require.Equal(t, provider.StatusRejected, results[1].GetStatus())
	require.Equal(t, uint32(410), results[1].GetHttpStatus())
	require.Equal(t, "Unregistered", results[1].GetProviderCode())
	require.Equal(t, provider.StatusSent, results[2].GetStatus())
	require.Equal(t, "ref-fcm", results[2].GetRef())

	// Each target reached its provider once, with its own environment.
	require.Equal(t, 3, fake.calls)
	require.Equal(t, provider.EnvironmentProduction, fake.jobs["prod-token"].Target.GetEnvironment())
	require.Equal(t, provider.EnvironmentDevelopment, fake.jobs["gone"].Target.GetEnvironment())
	require.Equal(t, provider.ProviderFCM, fake.jobs["fcm-token"].Target.GetProvider())

	// An expired notification is refused for each target before any provider call.
	expired := s.Send(context.Background(), directRequest(1))
	require.Nil(t, expired.GetError())
	for _, result := range expired.GetResult().GetResults() {
		require.Equal(t, provider.StatusRejected, result.GetStatus())
		require.Equal(t, provider.CodeExpired, result.GetCode())
	}
	require.Equal(t, 3, fake.calls, "an expired request makes no provider call")
}

// TestNoBodyInLogs checks that the log of a send holds no token, ref, payload or message: the log of a send holds counts
// only. The check reads a real log line, so it first checks that the send wrote one.
func TestNoBodyInLogs(t *testing.T) {
	var logged bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&logged).Level(zerolog.DebugLevel)
	t.Cleanup(func() {
		log.Logger = previous
	})

	s := newTestSender(t, newRoutingSender(), nil)
	resp := s.Send(context.Background(), directRequest(0))
	require.Nil(t, resp.GetError())

	output := logged.String()
	require.NotEmpty(t, output, "a send writes its count line, so the check reads a log")
	for _, marker := range []string{
		"prod-token", "gone", "fcm-token", "ref-prod", "ref-gone", "ref-fcm", "payload-marker", "message-marker", "e2e-uid",
	} {
		require.NotContains(t, output, marker)
	}
}

// keysOf returns the keys of object.
func keysOf(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	return keys
}

// TestPROOnlyFieldsAnswerPROShape sends a request that uses the PRO fields only, apns_tokens and a PRO notification, and
// checks that the answer carries the PRO uid and the results of this mode, and no other field.
func TestPROOnlyFieldsAnswerPROShape(t *testing.T) {
	s := newTestSender(t, newRoutingSender(), nil)
	req := &apiproto.SendPushNotificationRequest{
		Recipient: &apiproto.PushRecipient{
			ApnsTokens: []string{"prod-token"},
		},
		Notification: &apiproto.PushNotification{
			Apns: &apiproto.ApnsPushNotification{Payload: testPayload()},
		},
	}

	resp := s.Send(context.Background(), req)

	require.Nil(t, resp.GetError())
	require.NotEmpty(t, resp.GetResult().GetUid())
	require.Len(t, resp.GetResult().GetResults(), 1)

	body, err := protojson.Marshal(resp)
	require.NoError(t, err)
	var answer map[string]any
	require.NoError(t, json.Unmarshal(body, &answer))
	require.ElementsMatch(t, []string{"result"}, keysOf(answer))

	result, ok := answer["result"].(map[string]any)
	require.True(t, ok)
	require.ElementsMatch(t, []string{"uid", "results"}, keysOf(result))

	allowed := map[string]bool{
		"index":             true,
		"ref":               true,
		"status":            true,
		"code":              true,
		"providerCode":      true,
		"providerDetail":    true,
		"httpStatus":        true,
		"providerMessageId": true,
	}
	items, ok := result["results"].([]any)
	require.True(t, ok)
	for _, item := range items {
		object, ok := item.(map[string]any)
		require.True(t, ok)
		for _, key := range keysOf(object) {
			require.True(t, allowed[key], "field %s is not a field of a result item", key)
		}
	}
}
