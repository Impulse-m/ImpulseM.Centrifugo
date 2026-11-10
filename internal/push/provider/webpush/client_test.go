package webpush

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/origin"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// testOrigins allows the origin of the test servers, which are reached under the name localhost.
func testOrigins(t *testing.T) *origin.PatternChecker {
	t.Helper()

	checker, err := origin.NewPatternChecker([]string{"https://localhost:*"})
	require.NoError(t, err)
	return checker
}

// newTestSigningKey returns a new P-256 key for the VAPID signature.
func newTestSigningKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

// publicKeyOf returns the base64url form of the uncompressed public point of key, as the configuration holds it.
func publicKeyOf(t *testing.T, key *ecdsa.PrivateKey) string {
	t.Helper()

	private, err := key.ECDH()
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(private.PublicKey().Bytes())
}

// validKeyBytes returns a new subscription key pair's public point and a new auth secret.
func validKeyBytes(t *testing.T) ([]byte, []byte) {
	t.Helper()

	receiver, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	auth := make([]byte, authLength)
	_, err = io.ReadFull(rand.Reader, auth)
	require.NoError(t, err)
	return receiver.PublicKey().Bytes(), auth
}

// validKeyStrings returns the base64url strings of validKeyBytes, without padding.
func validKeyStrings(t *testing.T) (string, string) {
	t.Helper()

	point, auth := validKeyBytes(t)
	return base64.RawURLEncoding.EncodeToString(point), base64.RawURLEncoding.EncodeToString(auth)
}

// newTokenWith returns the PushSubscription JSON of a token with the endpoint and the two key strings.
func newTokenWith(t *testing.T, endpoint string, p256dh string, auth string) string {
	t.Helper()

	token, err := json.Marshal(map[string]any{
		"endpoint": endpoint,
		"keys":     map[string]string{"p256dh": p256dh, "auth": auth},
	})
	require.NoError(t, err)
	return string(token)
}

// newSubscription returns the token of a new subscription to endpoint, with the receiver private key and the auth secret of
// the subscription, so a test can open what is sent to it.
func newSubscription(t *testing.T, endpoint string) (string, *ecdh.PrivateKey, []byte) {
	t.Helper()

	receiver, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	auth := make([]byte, authLength)
	_, err = io.ReadFull(rand.Reader, auth)
	require.NoError(t, err)

	token := newTokenWith(
		t,
		endpoint,
		base64.RawURLEncoding.EncodeToString(receiver.PublicKey().Bytes()),
		base64.RawURLEncoding.EncodeToString(auth),
	)
	return token, receiver, auth
}

// newTokenFor returns the token of a new subscription to endpoint.
func newTokenFor(t *testing.T, endpoint string) string {
	t.Helper()

	token, _, _ := newSubscription(t, endpoint)
	return token
}

// newTestJob returns a job of one webpush target with the caller headers and the request payload.
func newTestJob(index int, token string, headers map[string]string, payload string) *provider.Job {
	return &provider.Job{
		Index: index,
		Target: &apiproto.PushTarget{
			Provider: provider.ProviderWebPush,
			Token:    token,
			Ref:      "ref-" + strconv.Itoa(index),
		},
		Notification: &apiproto.PushNotification{
			Webpush: &apiproto.WebPushPushNotification{
				Headers: headers,
				Payload: []byte(payload),
			},
		},
	}
}

// countingRandom returns a source of the ephemeral key and the salt that counts each call.
func countingRandom(count *atomic.Int64) randomSource {
	return func() (*ecdh.PrivateKey, []byte, error) {
		count.Add(1)
		return systemRandom()
	}
}

// recordedRequest is one request seen by the fake push service.
type recordedRequest struct {
	path    string
	headers http.Header
	body    []byte
}

// fakePush is an httptest TLS push service. Its handler is set by the test, and it records each request before the handler
// runs.
type fakePush struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []recordedRequest
}

// newFakePush starts a fake push service with handler.
func newFakePush(t *testing.T, handler http.HandlerFunc) *fakePush {
	t.Helper()

	fake := &fakePush{}
	fake.server = httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)

		fake.mu.Lock()
		fake.requests = append(fake.requests, recordedRequest{
			path:    request.URL.Path,
			headers: request.Header.Clone(),
			body:    body,
		})
		fake.mu.Unlock()

		handler(writer, request)
	}))
	fake.server.StartTLS()
	t.Cleanup(fake.server.Close)
	return fake
}

// endpoint returns the endpoint of the fake at path. The host is localhost, a domain name that the origin patterns allow.
func (f *fakePush) endpoint(path string) string {
	parsed, _ := url.Parse(f.server.URL)
	return "https://localhost:" + parsed.Port() + path
}

// tlsConfig trusts the certificate of the fake. The client dials localhost, so the name that the certificate is checked
// against is its IP address.
func (f *fakePush) tlsConfig() *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(f.server.Certificate())
	return &tls.Config{
		RootCAs:    pool,
		ServerName: "127.0.0.1",
	}
}

// recorded returns the requests the fake has seen.
func (f *fakePush) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

// newTestClient returns a client that reaches the fake, with the address guard off for the loopback test server.
func newTestClient(t *testing.T, fake *fakePush, options ...Option) *Client {
	t.Helper()

	return newTestClientWithOrigins(t, fake, testOrigins(t), options...)
}

// newTestClientWithOrigins returns a client that reaches the fake and allows the origins of origins, with the address guard
// off for the loopback test server.
func newTestClientWithOrigins(t *testing.T, fake *fakePush, origins *origin.PatternChecker, options ...Option) *Client {
	t.Helper()

	key := newTestSigningKey(t)
	all := append([]Option{withTLSConfig(fake.tlsConfig()), withoutAddressGuard()}, options...)
	client, err := NewClient(key, publicKeyOf(t, key), "mailto:push@example.com", origins, all...)
	require.NoError(t, err)
	return client
}

// failedWrite is a transport that fails the write of every request the way a reset connection does: it reports the write
// error to the trace of the request, then returns the error.
type failedWrite struct{}

// RoundTrip reports the write error to the write hook of the request, and returns it.
func (failedWrite) RoundTrip(request *http.Request) (*http.Response, error) {
	writeErr := errors.New("write: connection reset by peer")
	trace := httptrace.ContextClientTrace(request.Context())
	if trace != nil && trace.WroteRequest != nil {
		trace.WroteRequest(httptrace.WroteRequestInfo{Err: writeErr})
	}
	return nil, writeErr
}

// TestSendSentEncryptsForTheSubscription checks the request of a sent target: the headers, the VAPID authorization and the
// body, which a receiver with the subscription keys opens to the payload.
func TestSendSentEncryptsForTheSubscription(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", "https://push.example/msg/1")
		writer.WriteHeader(http.StatusCreated)
	})
	token, receiver, auth := newSubscription(t, fake.endpoint("/send/abc"))
	client := newTestClient(t, fake)
	job := newTestJob(0, token, map[string]string{"TTL": "60", "Urgency": "high", "Topic": "chat-1"}, `{"title":"hi"}`)

	result := client.Send(context.Background(), job)

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Equal(t, uint32(http.StatusCreated), result.GetHttpStatus())
	require.Equal(t, "https://push.example/msg/1", result.GetProviderMessageId())
	require.Empty(t, result.GetCode())

	requests := fake.recorded()
	require.Len(t, requests, 1)
	request := requests[0]
	require.Equal(t, "/send/abc", request.path)
	require.Equal(t, "aes128gcm", request.headers.Get("Content-Encoding"))
	require.Equal(t, "application/octet-stream", request.headers.Get("Content-Type"))
	require.Equal(t, "60", request.headers.Get("TTL"))
	require.Equal(t, "high", request.headers.Get("Urgency"))
	require.Equal(t, "chat-1", request.headers.Get("Topic"))

	authorization := request.headers.Get("Authorization")
	require.True(t, strings.HasPrefix(authorization, "vapid t="))
	require.True(t, strings.HasSuffix(authorization, ", k="+client.publicKey))

	require.Equal(t, `{"title":"hi"}`, string(decryptForTest(t, request.body, receiver, auth)))
}

// TestSendDefaultTTLIsFourWeeks checks that a target with no TTL, no expire_at and no Topic sends the default TTL alone.
func TestSendDefaultTTLIsFourWeeks(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	client := newTestClient(t, fake)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	request := fake.recorded()[0]
	require.Equal(t, "2419200", request.headers.Get("TTL"))
	require.Empty(t, request.headers.Get("Topic"))
	require.Empty(t, request.headers.Get("Urgency"))
}

// TestSendAnswersMapToItemResults checks the result of each status: a non-2xx answer is rejected with its status, its
// reason and its message, and the body is read only for those fields.
func TestSendAnswersMapToItemResults(t *testing.T) {
	testCases := []struct {
		name   string
		status int
		body   string
		code   string
		detail string
	}{
		{name: "404 subscription gone", status: http.StatusNotFound, body: `{"reason":"subscription-gone","message":"no such subscription"}`, code: "subscription-gone", detail: "no such subscription"},
		{name: "410 gone with error", status: http.StatusGone, body: `{"error":"gone"}`, code: "gone"},
		{name: "413 too large", status: http.StatusRequestEntityTooLarge, body: `{"message":"too big"}`, detail: "too big"},
		{name: "429 with a body that is not JSON", status: http.StatusTooManyRequests, body: `not json`},
		{name: "500 with no body", status: http.StatusInternalServerError},
		{name: "reason wins over error", status: http.StatusBadRequest, body: `{"reason":"BadRequest","error":"other"}`, code: "BadRequest"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(testCase.status)
				_, _ = writer.Write([]byte(testCase.body))
			})
			client := newTestClient(t, fake)

			result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

			require.Equal(t, provider.StatusRejected, result.GetStatus())
			require.Equal(t, uint32(testCase.status), result.GetHttpStatus())
			require.Equal(t, testCase.code, result.GetProviderCode())
			require.Equal(t, testCase.detail, result.GetProviderDetail())
			require.Empty(t, result.GetProviderMessageId())
		})
	}
}

// TestSendDoesNotFollowRedirect checks that a 3xx is the answer: the client makes one request and reports the status.
func TestSendDoesNotFollowRedirect(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/second" {
			writer.WriteHeader(http.StatusCreated)
			return
		}
		writer.Header().Set("Location", "/second")
		writer.WriteHeader(http.StatusFound)
	})
	client := newTestClient(t, fake)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/first")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Equal(t, uint32(http.StatusFound), result.GetHttpStatus())
	require.Len(t, fake.recorded(), 1, "a redirect is not followed")
}

// TestSendLocationIsKeptOnlyWhenPrintable checks that the Location of a 2xx answer becomes the message id only when it is
// printable and within the field bound.
func TestSendLocationIsKeptOnlyWhenPrintable(t *testing.T) {
	testCases := []struct {
		name     string
		location string
		expected string
	}{
		{name: "printable", location: "https://push.example/msg/2", expected: "https://push.example/msg/2"},
		{name: "a tab is a control character", location: "https://push.example/a\tb", expected: ""},
		{name: "longer than the field bound", location: strings.Repeat("a", provider.MaxFieldBytes+1), expected: ""},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Location", testCase.location)
				writer.WriteHeader(http.StatusCreated)
			})
			client := newTestClient(t, fake)

			result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

			require.Equal(t, provider.StatusSent, result.GetStatus())
			require.Equal(t, testCase.expected, result.GetProviderMessageId())
		})
	}
}

// TestSendWriteFailureIsNotSentWithConnectFailed checks that a request whose write failed was not written: the target is
// not_sent with connect_failed, so a resend is not a duplicate.
func TestSendWriteFailureIsNotSentWithConnectFailed(t *testing.T) {
	key := newTestSigningKey(t)
	client, err := NewClient(key, publicKeyOf(t, key), "mailto:push@example.com", testOrigins(t), withRoundTripper(failedWrite{}))
	require.NoError(t, err)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, "https://localhost:9/x"), nil, `{"a":1}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeConnectFailed, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// TestSendConnectFailureIsNotSentWithConnectFailed checks that a refused connection is not_sent with connect_failed, not
// address_blocked: the guard did not refuse the address, the connection was refused.
func TestSendConnectFailureIsNotSentWithConnectFailed(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	token := newTokenFor(t, fake.endpoint("/x"))
	client := newTestClient(t, fake)
	fake.server.Close()

	result := client.Send(context.Background(), newTestJob(0, token, nil, `{"a":1}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeConnectFailed, result.GetCode())
}

// TestSendRefusedAddressIsNotSentWithAddressBlocked checks that a connection the guard refuses is not_sent with
// address_blocked, and that no request reaches the fake. The endpoint is named localhost, so the dial reaches loopback.
func TestSendRefusedAddressIsNotSentWithAddressBlocked(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	key := newTestSigningKey(t)
	client, err := NewClient(key, publicKeyOf(t, key), "mailto:push@example.com", testOrigins(t), withTLSConfig(fake.tlsConfig()))
	require.NoError(t, err)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeAddressBlocked, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
	require.Empty(t, fake.recorded())
}

// TestSendWrittenThenHeldIsUnanswered checks that a request the server read, and then held, is unanswered.
func TestSendWrittenThenHeldIsUnanswered(t *testing.T) {
	held := make(chan struct{}, 1)
	fake := newFakePush(t, func(writer http.ResponseWriter, request *http.Request) {
		held <- struct{}{}
		<-request.Context().Done()
	})
	client := newTestClient(t, fake)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token := newTokenFor(t, fake.endpoint("/x"))
	results := make(chan *apiproto.PushItemResult, 1)
	go func() {
		results <- client.Send(ctx, newTestJob(0, token, nil, `{"a":1}`))
	}()

	// The waits of the test end at a deadline of their own, so a broken send fails the test and cannot hang the run.
	wait, stopWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWait()
	select {
	case <-held:
	case <-wait.Done():
		t.Fatal("the server did not read the request within 5 seconds")
	}
	cancel()
	var result *apiproto.PushItemResult
	select {
	case result = <-results:
	case <-wait.Done():
		t.Fatal("the send did not return within 5 seconds")
	}

	require.Len(t, fake.recorded(), 1, "the server must have read the request before it held the answer")
	require.Equal(t, provider.StatusUnanswered, result.GetStatus())
	require.Empty(t, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
}

// TestSendRefusalsMakeNoRequest checks that each item refusal is answered with its code and no request is made. Each endpoint
// is built from the fake, so a request sent by mistake would be recorded. The origin case allows other origins only, so the
// origin of the fake is not allowed there.
func TestSendRefusalsMakeNoRequest(t *testing.T) {
	testCases := []struct {
		name    string
		origins []string
		mutate  func(job *provider.Job)
		code    string
	}{
		{name: "origin not allowed", origins: []string{"https://push.example.com"}, code: provider.CodeEndpointNotAllowed},
		{
			name: "invalid resolved topic",
			mutate: func(job *provider.Job) {
				job.CollapseID = "not a topic"
			},
			code: provider.CodeInvalidTopic,
		},
		{
			name: "expired",
			mutate: func(job *provider.Job) {
				job.Notification.ExpireAt = time.Now().Unix() - 1
			},
			code: provider.CodeExpired,
		},
		{
			name: "payload too large",
			mutate: func(job *provider.Job) {
				job.Notification.Webpush.Payload = []byte(jsonPayload(provider.MaxWebPushPayloadBytes + 1))
			},
			code: provider.CodePayloadTooLarge,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(http.StatusCreated)
			})
			origins := testOrigins(t)
			if testCase.origins != nil {
				checker, err := origin.NewPatternChecker(testCase.origins)
				require.NoError(t, err)
				origins = checker
			}
			client := newTestClientWithOrigins(t, fake, origins)
			job := newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`)
			if testCase.mutate != nil {
				testCase.mutate(job)
			}

			result := client.Send(context.Background(), job)

			require.Equal(t, provider.StatusRejected, result.GetStatus())
			require.Equal(t, testCase.code, result.GetCode())
			require.Empty(t, fake.recorded())
		})
	}
}

// TestSendEncryptsOnlyAfterTheSlotIsHeld checks that a target which waits for the only in-flight slot holds no encrypted
// body: the key and the salt are drawn once, for the target that holds the slot.
func TestSendEncryptsOnlyAfterTheSlotIsHeld(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHeld := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	entered := make(chan struct{}, 2)
	fake := newFakePush(t, func(writer http.ResponseWriter, request *http.Request) {
		entered <- struct{}{}
		// The handler ends when the test releases it, or when its request ends, so it cannot outlive the test.
		select {
		case <-release:
		case <-request.Context().Done():
		}
		writer.WriteHeader(http.StatusCreated)
	})
	// Registered after the fake server, so the release runs before the server's Close waits for the held handler.
	t.Cleanup(releaseHeld)
	var randoms atomic.Int64
	client := newTestClient(t, fake, withRandom(countingRandom(&randoms)))
	set := provider.NewSet()
	set.Add(provider.ProviderWebPush, client, 1)
	token := newTokenFor(t, fake.endpoint("/x"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		set.Dispatch(context.Background(), time.Minute, newTestJob(0, token, nil, `{"a":1}`))
	}()
	// The wait ends at a deadline, so a first target that never reaches the server fails the test and cannot hang the run.
	wait, stopWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWait()
	select {
	case <-entered:
	case <-wait.Done():
		t.Fatal("the first target did not reach the server within 5 seconds")
	}

	slot, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := set.DispatchWindow(slot, context.Background(), time.Minute, newTestJob(1, token, nil, `{"a":1}`))

	require.Equal(t, provider.StatusNotSent, result.GetStatus())
	require.Equal(t, provider.CodeNoSlot, result.GetCode())
	require.Equal(t, int64(1), randoms.Load(), "the waiting target draws no key and encrypts nothing")

	releaseHeld()
	wg.Wait()
}

// TestSendIgnoresProxyEnvironment checks that a proxy named in the environment has no effect: the request reaches the
// push service directly, so the guard sees its address.
func TestSendIgnoresProxyEnvironment(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("https_proxy", "http://127.0.0.1:1")
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	client := newTestClient(t, fake)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Len(t, fake.recorded(), 1)
}

// TestNewClientRequiresOriginPatterns checks that a client without origin patterns is not built.
func TestNewClientRequiresOriginPatterns(t *testing.T) {
	key := newTestSigningKey(t)

	_, err := NewClient(key, publicKeyOf(t, key), "mailto:push@example.com", nil)

	require.Error(t, err)
}

// TestPaddedPublicKeyGivesAnUnpaddedKValue checks that a configured public key with "=" padding gives the k value of the
// Authorization header in unpadded base64url.
func TestPaddedPublicKeyGivesAnUnpaddedKValue(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	key := newTestSigningKey(t)
	private, err := key.ECDH()
	require.NoError(t, err)
	padded := base64.URLEncoding.EncodeToString(private.PublicKey().Bytes())
	require.True(t, strings.HasSuffix(padded, "="))
	client, err := NewClient(key, padded, "mailto:push@example.com", testOrigins(t), withTLSConfig(fake.tlsConfig()), withoutAddressGuard())
	require.NoError(t, err)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	authorization := fake.recorded()[0].headers.Get("Authorization")
	require.True(t, strings.HasSuffix(authorization, ", k="+publicKeyOf(t, key)))
}

// TestRejectedBodyFieldsAreReadOneByOne checks that a field of another JSON type does not drop the other fields of the body:
// the reason is the error string, and the detail is the message.
func TestRejectedBodyFieldsAreReadOneByOne(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"reason": 1, "error": "BadRequest", "message": "x"}`))
	})
	client := newTestClient(t, fake)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Equal(t, uint32(http.StatusBadRequest), result.GetHttpStatus())
	require.Equal(t, "BadRequest", result.GetProviderCode())
	require.Equal(t, "x", result.GetProviderDetail())
}

// TestSuccessWithBodyIsSent checks that a 2xx answer is sent, and that its body does not change the result.
func TestSuccessWithBodyIsSent(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte(`{"some":"body"}`))
	})
	client := newTestClient(t, fake)

	result := client.Send(context.Background(), newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Equal(t, uint32(http.StatusCreated), result.GetHttpStatus())
	require.Empty(t, result.GetProviderCode())
	require.Empty(t, result.GetProviderDetail())
}

// TestSendWithoutAPriorRefusal checks that Send checks and sends a job that Refusal never saw.
func TestSendWithoutAPriorRefusal(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	client := newTestClient(t, fake)
	job := newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`)
	require.Nil(t, job.Prepared)

	result := client.Send(context.Background(), job)

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Len(t, fake.recorded(), 1)
}

// TestSendUsesWhatRefusalPrepared checks that Refusal keeps the checked target of a job that passes, and that Send sends it.
func TestSendUsesWhatRefusalPrepared(t *testing.T) {
	fake := newFakePush(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	client := newTestClient(t, fake)
	job := newTestJob(0, newTokenFor(t, fake.endpoint("/x")), nil, `{"a":1}`)

	require.Empty(t, Refusal(job, testOrigins(t)))
	require.NotNil(t, job.Prepared)
	// The token is no longer a subscription, so Send can only send the target that Refusal prepared.
	job.Target.Token = "not a subscription"

	result := client.Send(context.Background(), job)

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Len(t, fake.recorded(), 1)
}
