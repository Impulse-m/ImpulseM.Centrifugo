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
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

	job := newTestJob(0, `{"notification":{"title":"hello"},"data":{"k":"v"}}`)
	job.CollapseID = "collapse-1"
	result := client.Send(context.Background(), job)

	require.Equal(t, provider.StatusSent, result.GetStatus())
	requests, _ := fake.recorded()
	body := requests[0]
	require.Equal(t, `{"title":"hello"}`, gjson.GetBytes(body, "message.notification").Raw)
	require.Equal(t, `{"k":"v"}`, gjson.GetBytes(body, "message.data").Raw)
	require.Equal(t, "fcm-access", gjson.GetBytes(body, "message.token").String())
	require.Equal(t, "collapse-1", gjson.GetBytes(body, "message.android.collapse_key").String())
	require.Equal(t, "collapse-1", gjson.GetBytes(body, "message.apns.headers.apns-collapse-id").String())
	require.False(t, gjson.GetBytes(body, "message.android.ttl").Exists())
	require.False(t, gjson.GetBytes(body, "message.apns.headers.apns-expiration").Exists())
}

func TestFCMApnsExpirationIsString(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

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

func TestFCMCollapseKeyConflict(t *testing.T) {
	require.Error(t, CheckMessage([]byte(`{"android":{"collapse_key":"b"}}`), "a"))
	require.Error(t, CheckMessage([]byte(`{"apns":{"headers":{"apns-collapse-id":"b"}}}`), "a"))
	require.NoError(t, CheckMessage([]byte(`{"android":{"collapse_key":"a"}}`), "a"))
}

func TestCollapseHeadersPassThroughWhenEmpty(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

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
			client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

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
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

	result := client.Send(context.Background(), newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Equal(t, uint32(http.StatusOK), result.GetHttpStatus())
	require.Equal(t, "projects/test-project/messages/123", result.GetProviderMessageId())
}

func TestFCMTokenRefreshedFromServiceAccount(t *testing.T) {
	tokens := newFakeToken(t, false)
	fake := newFakeFCM(t, okHandler)
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

	client.Send(context.Background(), newTestJob(0, `{"data":{}}`))
	client.Send(context.Background(), newTestJob(1, `{"data":{}}`))

	_, headers := fake.recorded()
	require.Equal(t, "Bearer fcm-access", headers[0].Get("authorization"))
	require.Equal(t, int64(1), tokens.requests.Load(), "a cached token is not fetched again")
}

func TestFCMTokenWaitBounded(t *testing.T) {
	tokens := newFakeToken(t, true)
	fake := newFakeFCM(t, okHandler)
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), 200*time.Millisecond, withFCMEndpoint(fake.server.URL))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := client.Send(ctx, newTestJob(0, `{"data":{}}`))

	require.Equal(t, provider.StatusPending, result.GetStatus())
	require.Less(t, time.Since(started), time.Second)
	requests, _ := fake.recorded()
	require.Empty(t, requests, "a token wait is not a provider call")
}

func TestFCMTokenFetchOneAtATime(t *testing.T) {
	tokens := newFakeToken(t, true)
	fake := newFakeFCM(t, okHandler)
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

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
		injected, err := injectMessage([]byte(base(padding)), "fcm-access", "", 0, time.Now())
		require.NoError(t, err)
		return len(injected)
	}
	fit := limit - injectedLength(0)

	t.Run("at the limit is sent", func(t *testing.T) {
		fake := newFakeFCM(t, okHandler)
		client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

		result := client.Send(context.Background(), newTestJob(0, base(fit)))

		require.Equal(t, provider.StatusSent, result.GetStatus())
		require.Equal(t, limit, injectedLength(fit))
	})

	t.Run("over the limit is refused without a request", func(t *testing.T) {
		fake := newFakeFCM(t, okHandler)
		client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second, withFCMEndpoint(fake.server.URL))

		result := client.Send(context.Background(), newTestJob(0, base(fit+1)))

		require.Equal(t, provider.StatusRejected, result.GetStatus())
		require.Equal(t, provider.CodePayloadTooLarge, result.GetCode())
		requests, _ := fake.recorded()
		require.Empty(t, requests)
	})
}

func TestFCMKeepaliveSet(t *testing.T) {
	tokens := newFakeToken(t, false)
	client := NewClient(context.Background(), newTestAccount(t, tokens.server.URL), time.Second)

	transport := client.httpClient.Transport.(*http.Transport)
	require.NotNil(t, transport.HTTP2)
	require.Equal(t, 15*time.Second, transport.HTTP2.SendPingTimeout)
	require.Equal(t, 15*time.Second, transport.HTTP2.PingTimeout)
	require.Equal(t, 90*time.Second, transport.IdleConnTimeout)
}
