package fcm

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// fakeFCM is a plain httptest FCM API. The handler answers each send request.
type fakeFCM struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests [][]byte
	headers  []http.Header
}

func newFakeFCM(t *testing.T, handler http.HandlerFunc) *fakeFCM {
	t.Helper()

	fake := &fakeFCM{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)

		fake.mu.Lock()
		fake.requests = append(fake.requests, body)
		fake.headers = append(fake.headers, request.Header.Clone())
		fake.mu.Unlock()

		handler(writer, request)
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeFCM) recorded() ([][]byte, []http.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.requests...), append([]http.Header(nil), f.headers...)
}

// fakeToken is a token endpoint of the service account. It counts its requests and answers with access token
// fcm-access, or hangs until the request context ends when hang is set.
type fakeToken struct {
	server   *httptest.Server
	requests atomic.Int64
}

func newFakeToken(t *testing.T, hang bool) *fakeToken {
	t.Helper()

	fake := &fakeToken{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		fake.requests.Add(1)
		// The body is read first: the server notices a client that gave up only after the body is consumed.
		_, _ = io.Copy(io.Discard, request.Body)
		if hang {
			<-request.Context().Done()
			return
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`{"access_token":"fcm-access","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

// testKeyOnce and testKeyPEM hold the RSA key of the service account fixture, generated once per package.
var (
	testKeyOnce sync.Once
	testKeyPEM  []byte
	testKeyErr  error
)

// serviceAccountKeyPEM returns the PKCS#8 PEM of an RSA key. A service account key is RSA: the JWT grant signs with
// RS256.
func serviceAccountKeyPEM() ([]byte, error) {
	testKeyOnce.Do(func() {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			testKeyErr = err
			return
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			testKeyErr = err
			return
		}
		testKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	})
	return testKeyPEM, testKeyErr
}

// newTestAccount returns the parsed service account of a generated key, with token_uri at the fake token endpoint.
func newTestAccount(t *testing.T, tokenURL string) *provider.FCMServiceAccount {
	t.Helper()

	keyPEM, err := serviceAccountKeyPEM()
	require.NoError(t, err)

	data, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "kid",
		"private_key":    string(keyPEM),
		"client_email":   "sender@test-project.iam.gserviceaccount.com",
		"token_uri":      tokenURL,
	})
	require.NoError(t, err)

	account, err := provider.LoadFCMServiceAccount(data)
	require.NoError(t, err)
	return account
}

// newTestClient returns the client of a generated account, and fails the test when the client cannot be built.
func newTestClient(t *testing.T, tokenURL string, timeout time.Duration, options ...Option) *Client {
	t.Helper()

	client, err := NewClient(context.Background(), newTestAccount(t, tokenURL), timeout, options...)
	require.NoError(t, err)
	return client
}

// newFailingToken is a token endpoint that refuses every request with status, as a revoked service account does.
func newFailingToken(t *testing.T, status int) *fakeToken {
	t.Helper()

	fake := &fakeToken{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		fake.requests.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		writer.Header().Set("content-type", "application/json")
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(`{"error":"invalid_client"}`))
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

// newTestJob returns a job of one FCM target with the message.
func newTestJob(index int, message string) *provider.Job {
	return &provider.Job{
		Index: index,
		Target: &apiproto.PushTarget{
			Provider: provider.ProviderFCM,
			Token:    "device-token",
			Ref:      "ref",
		},
		Notification: &apiproto.PushNotification{
			Fcm: &apiproto.FcmPushNotification{
				Message: []byte(message),
			},
		},
	}
}

func okHandler(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("content-type", "application/json")
	_, _ = writer.Write([]byte(`{"name":"projects/test-project/messages/123"}`))
}

func TestFCMBodyIsPassThrough(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	job := newTestJob(0, `{"notification":{"title":"hello"},"data":{"k":"v"}}`)
	job.CollapseID = "collapse-1"
	result := client.Send(context.Background(), job)

	require.Equal(t, provider.StatusSent, result.GetStatus())
	requests, _ := fake.recorded()
	body := requests[0]
	require.Equal(t, `{"title":"hello"}`, gjson.GetBytes(body, "message.notification").Raw)
	require.Equal(t, `{"k":"v"}`, gjson.GetBytes(body, "message.data").Raw)
	require.Equal(t, "device-token", gjson.GetBytes(body, "message.token").String())
	require.NotEqual(t, "fcm-access", gjson.GetBytes(body, "message.token").String(), "the OAuth token never goes in the body")
	require.Equal(t, "collapse-1", gjson.GetBytes(body, "message.android.collapse_key").String())
	require.Equal(t, "collapse-1", gjson.GetBytes(body, "message.apns.headers.apns-collapse-id").String())
	require.False(t, gjson.GetBytes(body, "message.android.ttl").Exists())
	require.False(t, gjson.GetBytes(body, "message.apns.headers.apns-expiration").Exists())
}

func TestFCMApnsExpirationIsString(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	job := newTestJob(0, `{"data":{"k":"v"}}`)
	expireAt := time.Now().Unix() + 3600
	job.Notification.ExpireAt = expireAt
	client.Send(context.Background(), job)

	requests, _ := fake.recorded()
	// The expiration is a JSON string holding the decimal Unix seconds.
	expiration := gjson.GetBytes(requests[0], "message.apns.headers.apns-expiration")
	require.Equal(t, gjson.String, expiration.Type)
	parsed, err := strconv.ParseInt(expiration.String(), 10, 64)
	require.NoError(t, err)
	require.Equal(t, expireAt, parsed)

	ttl := gjson.GetBytes(requests[0], "message.android.ttl")
	require.Equal(t, gjson.String, ttl.Type)
	require.True(t, strings.HasSuffix(ttl.String(), "s"))
}

func TestFCMMessageTokenRefused(t *testing.T) {
	err := CheckMessage([]byte(`{"token":"caller","data":{}}`), "")

	require.ErrorContains(t, err, "message.token is set by the server")
}

func TestFCMMessageMustBeObject(t *testing.T) {
	require.ErrorContains(t, CheckMessage([]byte(`[{"data":{}}]`), ""), "not a JSON object")
	require.ErrorContains(t, CheckMessage([]byte(`"text"`), ""), "not a JSON object")
	require.NoError(t, CheckMessage([]byte(`{"data":{}}`), ""))
}

func TestFCMOversizedAnswerFieldsAreTruncated(t *testing.T) {
	tokens := newFakeToken(t, false)
	longStatus := strings.Repeat("S", 60<<10)
	longName := strings.Repeat("n", 60<<10)
	fake := newFakeFCM(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`{"name":"` + longName + `"}`))
	})
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))
	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Len(t, result.GetProviderMessageId(), provider.MaxFieldBytes)

	fake = newFakeFCM(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"error":{"status":"` + longStatus + `"}}`))
	})
	client = newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result = client.Send(context.Background(), newTestJob(0, `{"data":{}}`))
	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Len(t, result.GetProviderCode(), provider.MaxFieldBytes)
}

func TestFCMCollapseKeyConflict(t *testing.T) {
	require.Error(t, CheckMessage([]byte(`{"android":{"collapse_key":"b"}}`), "a"))
	require.Error(t, CheckMessage([]byte(`{"apns":{"headers":{"apns-collapse-id":"b"}}}`), "a"))
	require.NoError(t, CheckMessage([]byte(`{"android":{"collapse_key":"a"}}`), "a"))
}

func TestCollapseHeadersPassThroughWhenEmpty(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	message := `{"android":{"collapse_key":"caller-a"},"apns":{"headers":{"apns-collapse-id":"caller-b"}}}`
	result := client.Send(context.Background(), newTestJob(0, message))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	requests, _ := fake.recorded()
	require.Equal(t, "caller-a", gjson.GetBytes(requests[0], "message.android.collapse_key").String())
	require.Equal(t, "caller-b", gjson.GetBytes(requests[0], "message.apns.headers.apns-collapse-id").String())
	require.NoError(t, CheckMessage([]byte(message), ""))
}

func TestFCMErrorDetails(t *testing.T) {
	testCases := []struct {
		name           string
		status         int
		body           string
		expectedCode   string
		expectedDetail string
	}{
		{
			name:           "unregistered",
			status:         http.StatusNotFound,
			body:           `{"error":{"status":"NOT_FOUND","details":[{"@type":"x","errorCode":"UNREGISTERED"}]}}`,
			expectedCode:   "NOT_FOUND",
			expectedDetail: "UNREGISTERED",
		},
		{
			name:           "invalid argument field",
			status:         http.StatusBadRequest,
			body:           `{"error":{"status":"INVALID_ARGUMENT","details":[{"@type":"BadRequest","fieldViolations":[{"field":"message.token"}]}]}}`,
			expectedCode:   "INVALID_ARGUMENT",
			expectedDetail: "message.token",
		},
		{
			name:           "invalid argument with an error code keeps the field",
			status:         http.StatusBadRequest,
			body:           `{"error":{"status":"INVALID_ARGUMENT","details":[{"@type":"FcmError","errorCode":"INVALID_ARGUMENT"},{"@type":"BadRequest","fieldViolations":[{"field":"message.token"}]}]}}`,
			expectedCode:   "INVALID_ARGUMENT",
			expectedDetail: "message.token",
		},
		{
			name:           "quota",
			status:         http.StatusTooManyRequests,
			body:           `{"error":{"status":"RESOURCE_EXHAUSTED","details":[{"errorCode":"QUOTA_EXCEEDED"}]}}`,
			expectedCode:   "RESOURCE_EXHAUSTED",
			expectedDetail: "QUOTA_EXCEEDED",
		},
		{
			name:         "internal without details",
			status:       http.StatusInternalServerError,
			body:         `{"error":{"status":"INTERNAL"}}`,
			expectedCode: "INTERNAL",
		},
		{
			name:         "html body keeps the status only",
			status:       http.StatusServiceUnavailable,
			body:         `<html>down</html>`,
			expectedCode: "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tokens := newFakeToken(t, false)
			fake := newFakeFCM(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(testCase.status)
				_, _ = writer.Write([]byte(testCase.body))
			})
			client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

			result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

			require.Equal(t, provider.StatusRejected, result.GetStatus())
			require.Equal(t, uint32(testCase.status), result.GetHttpStatus())
			require.Equal(t, testCase.expectedCode, result.GetProviderCode())
			require.Equal(t, testCase.expectedDetail, result.GetProviderDetail())
		})
	}
}

func TestFCMSentCarriesMessageName(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Equal(t, uint32(http.StatusOK), result.GetHttpStatus())
	require.Equal(t, "projects/test-project/messages/123", result.GetProviderMessageId())
}

func TestFCMTokenRefreshedFromServiceAccount(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	client.Send(context.Background(), newTestJob(0, `{"data":{}}`))
	client.Send(context.Background(), newTestJob(1, `{"data":{}}`))

	_, headers := fake.recorded()
	require.Equal(t, "Bearer fcm-access", headers[0].Get("authorization"))
	require.Equal(t, int64(1), tokens.requests.Load(), "a cached token is not fetched again")
}

func TestFCMTokenWaitBounded(t *testing.T) {
	tokens := newFakeToken(t, true)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, 200*time.Millisecond, withFCMEndpoint(fake.server.URL))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := client.Send(ctx, newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Empty(t, result.GetCode(), "the call deadline ended the wait: the set names the code")
	require.Less(t, time.Since(started), time.Second)
	requests, _ := fake.recorded()
	require.Empty(t, requests, "a token wait is not a provider call")
}

func TestFCMTokenFetchOneAtATime(t *testing.T) {
	tokens := newFakeToken(t, true)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	var wg sync.WaitGroup
	for index := 0; index < 20; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			client.Send(ctx, newTestJob(index, `{"data":{}}`))
		}(index)
	}
	wg.Wait()

	require.Equal(t, int64(1), tokens.requests.Load())
}

func TestFCMPayloadLimit(t *testing.T) {
	const limit = 4096
	tokens := newFakeToken(t, false)

	// The padding is inside a string, so each extra byte adds one byte to the injected message.
	base := func(padding int) string {
		return `{"data":{"x":"` + strings.Repeat("a", padding) + `"}}`
	}
	injectedLength := func(padding int) int {
		injected, err := injectMessage([]byte(base(padding)), "device-token", "", 0, time.Now())
		require.NoError(t, err)
		return len(injected)
	}
	fit := limit - injectedLength(0)

	t.Run("at the limit is sent", func(t *testing.T) {
		fake := newFakeFCM(t, okHandler)
		client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

		result := client.Send(context.Background(), newTestJob(0, base(fit)))

		require.Equal(t, provider.StatusSent, result.GetStatus())
		require.Equal(t, limit, injectedLength(fit))
	})

	t.Run("over the limit is refused without a request", func(t *testing.T) {
		fake := newFakeFCM(t, okHandler)
		client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

		result := client.Send(context.Background(), newTestJob(0, base(fit+1)))

		require.Equal(t, provider.StatusRejected, result.GetStatus())
		require.Equal(t, provider.CodePayloadTooLarge, result.GetCode())
		requests, _ := fake.recorded()
		require.Empty(t, requests)
	})
}

func TestFCMKeepaliveSet(t *testing.T) {
	tokens := newFakeToken(t, false)
	client := newTestClient(t, tokens.server.URL, time.Second)

	transport := client.httpClient.Transport.(*http.Transport)
	require.NotNil(t, transport.HTTP2)
	require.Equal(t, 15*time.Second, transport.HTTP2.SendPingTimeout)
	require.Equal(t, 15*time.Second, transport.HTTP2.PingTimeout)
	require.Equal(t, 90*time.Second, transport.IdleConnTimeout)
}

func TestFCMMessageMustBeValidJSON(t *testing.T) {
	require.ErrorContains(t, CheckMessage([]byte(`{"notification":{"title":"x"}`), ""), "not a JSON object")
	require.ErrorContains(t, CheckMessage([]byte(`{"data":{}} trailing`), ""), "not a JSON object")
	require.NoError(t, CheckMessage([]byte(`{"data":{}}`+"\n"), ""))
}

func TestFCMMalformedMessageMakesNoRequest(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	for _, message := range []string{`{"notification":{"title":"x"}`, `{"data":{}} trailing`} {
		result := client.Send(context.Background(), newTestJob(0, message))

		require.Equal(t, provider.StatusRejected, result.GetStatus())
	}
	requests, _ := fake.recorded()
	require.Empty(t, requests)
}

func TestFCMExpiredRefusedBeforeAnyCall(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	job := newTestJob(0, `{"data":{}}`)
	job.Notification.ExpireAt = time.Now().Unix() - 1
	result := client.Send(context.Background(), job)

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Equal(t, provider.CodeExpired, result.GetCode())
	requests, _ := fake.recorded()
	require.Empty(t, requests)
	require.Zero(t, tokens.requests.Load(), "an expired target does not even fetch a token")
}

func TestFCMTTLNeverNegative(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	_, err := injectMessage([]byte(`{"data":{}}`), "device-token", "", now.Unix(), now)
	require.ErrorIs(t, err, errExpired)
	_, err = injectMessage([]byte(`{"data":{}}`), "device-token", "", now.Unix()-5, now)
	require.ErrorIs(t, err, errExpired)

	injected, err := injectMessage([]byte(`{"data":{}}`), "device-token", "", now.Unix()+60, now)
	require.NoError(t, err)
	require.Equal(t, "60s", gjson.GetBytes(injected, "android.ttl").String())
}

func TestNewClientRejectsNonPositiveTimeout(t *testing.T) {
	tokens := newFakeToken(t, false)
	account := newTestAccount(t, tokens.server.URL)

	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := NewClient(context.Background(), account, timeout)

		require.ErrorContains(t, err, "must be positive")
	}
}

func TestFCMTokenFetchFailureIsNotSentWithTokenFetchCode(t *testing.T) {
	tokens := newFailingToken(t, http.StatusUnauthorized)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	// Nothing left the node, so the item is not_sent, not rejected: a caller can resend it safely.
	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeTokenFetch, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
	require.Empty(t, result.GetProviderCode())
	requests, _ := fake.recorded()
	require.Empty(t, requests, "a token failure makes no provider call")
}

func TestFCMTokenFailureIsNotHeld(t *testing.T) {
	tokens := newFailingToken(t, http.StatusServiceUnavailable)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))

	require.Equal(t, provider.StatusNotSent, client.Send(context.Background(), newTestJob(0, `{"data":{}}`)).GetStatus())
	require.Equal(t, provider.StatusNotSent, client.Send(context.Background(), newTestJob(1, `{"data":{}}`)).GetStatus())

	require.Equal(t, int64(2), tokens.requests.Load(), "a failed fetch is not held: the next call starts a new fetch")
	requests, _ := fake.recorded()
	require.Empty(t, requests)
}

func TestFCMPendingWhenTokenReadyAfterDeadline(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := newTestClient(t, tokens.server.URL, time.Second, withFCMEndpoint(fake.server.URL))
	require.Equal(t, provider.StatusSent, client.Send(context.Background(), newTestJob(0, `{"data":{}}`)).GetStatus())

	// The token is cached, so the wait returns it at once, but the call is already past its deadline.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := client.Send(ctx, newTestJob(1, `{"data":{}}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	requests, _ := fake.recorded()
	require.Len(t, requests, 1, "a call past its deadline makes no provider call")
}
