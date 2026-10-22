package apns

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

const testBundleID = "com.example.app"

// recordedRequest is one request seen by the fake APNs server.
type recordedRequest struct {
	path          string
	authorization string
	headers       http.Header
	body          []byte
	protoMajor    int
}

// fakeAPNs is an httptest APNs server with HTTP/2 enabled. Its handler is set by the test.
type fakeAPNs struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []recordedRequest
}

// newFakeAPNs starts the fake with handler. The handler runs after the request is recorded.
func newFakeAPNs(t *testing.T, handler http.HandlerFunc) *fakeAPNs {
	t.Helper()

	fake := &fakeAPNs{}
	fake.server = httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)

		fake.mu.Lock()
		fake.requests = append(fake.requests, recordedRequest{
			path:          request.URL.Path,
			authorization: request.Header.Get("authorization"),
			headers:       request.Header.Clone(),
			body:          body,
			protoMajor:    request.ProtoMajor,
		})
		fake.mu.Unlock()

		handler(writer, request)
	}))
	fake.server.EnableHTTP2 = true
	fake.server.StartTLS()
	t.Cleanup(fake.server.Close)
	return fake
}

// tlsConfig trusts the fake certificate.
func (f *fakeAPNs) tlsConfig() *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(f.server.Certificate())
	return &tls.Config{RootCAs: pool}
}

func (f *fakeAPNs) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

// newTestClient returns a client of the fake, with the bearer on clock.
func newTestClient(t *testing.T, fake *fakeAPNs, credential *Credential) *Client {
	t.Helper()

	client, err := NewClient(credential, "production", testBundleID, withHost(fake.server.URL), withTLSConfig(fake.tlsConfig()))
	require.NoError(t, err)
	return client
}

// newTestJob returns a job of one APNs target with the caller headers and the payload.
func newTestJob(index int, token string, headers map[string]string, payload string) *provider.Job {
	return &provider.Job{
		Index: index,
		Target: &apiproto.PushTarget{
			Provider:    provider.ProviderAPNs,
			Token:       token,
			Environment: "production",
			Ref:         "ref-" + strconv.Itoa(index),
		},
		Notification: &apiproto.PushNotification{
			Apns: &apiproto.ApnsPushNotification{
				Headers: headers,
				Payload: []byte(payload),
			},
		},
	}
}

func TestAPNsHostFollowsEnvironment(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})

	production, err := NewClient(credential, "production", testBundleID)
	require.NoError(t, err)
	development, err := NewClient(credential, "development", testBundleID)
	require.NoError(t, err)
	_, err = NewClient(credential, "sandbox", testBundleID)

	require.Equal(t, hostProduction, production.host)
	require.Equal(t, hostDevelopment, development.host)
	require.Error(t, err)
}

func TestAPNsEnvironmentsReachTheirOwnFake(t *testing.T) {
	clock := &fakeClock{now: testWindow}
	credential, _ := newTestCredential(t, newTestKey(t), clock)

	production := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	development := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	productionClient := newTestClient(t, production, credential)
	developmentClient, err := NewClient(credential, "development", testBundleID, withHost(development.server.URL), withTLSConfig(development.tlsConfig()))
	require.NoError(t, err)

	result := productionClient.Send(context.Background(), newTestJob(0, "tok-prod", nil, "{}"))
	require.Equal(t, provider.StatusSent, result.GetStatus())
	result = developmentClient.Send(context.Background(), newTestJob(1, "tok-dev", nil, "{}"))
	require.Equal(t, provider.StatusSent, result.GetStatus())

	require.Len(t, production.recorded(), 1)
	require.Equal(t, "/3/device/tok-prod", production.recorded()[0].path)
	require.Len(t, development.recorded(), 1)
	require.Equal(t, "/3/device/tok-dev", development.recorded()[0].path)
}

func TestAPNsKeepaliveSet(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})

	client, err := NewClient(credential, "production", testBundleID)
	require.NoError(t, err)

	transport := client.httpClient.Transport.(*http.Transport)
	require.NotNil(t, transport.HTTP2)
	require.Equal(t, 15*time.Second, transport.HTTP2.SendPingTimeout)
	require.Equal(t, 15*time.Second, transport.HTTP2.PingTimeout)
	require.Equal(t, 90*time.Second, transport.IdleConnTimeout)
	require.True(t, transport.ForceAttemptHTTP2)
}

func TestAPNsNegotiatesHTTP2(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Equal(t, 2, fake.recorded()[0].protoMajor)
}

func TestAPNsOneBearerPerCredential(t *testing.T) {
	clock := &fakeClock{now: testWindow}
	credential, counter := newTestCredential(t, newTestKey(t), clock)
	ok := func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}
	production := newFakeAPNs(t, ok)
	development := newFakeAPNs(t, ok)

	developmentClient, err := NewClient(credential, "development", testBundleID, withHost(development.server.URL), withTLSConfig(development.tlsConfig()))
	require.NoError(t, err)

	newTestClient(t, production, credential).Send(context.Background(), newTestJob(0, "a", nil, "{}"))
	developmentClient.Send(context.Background(), newTestJob(1, "b", nil, "{}"))

	require.Equal(t, int64(1), counter.signs.Load())
	require.Equal(t, production.recorded()[0].authorization, development.recorded()[0].authorization)
}

func TestAPNsServerHeaders(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	job := newTestJob(0, "device-token", nil, `{"aps":{"alert":"hi"}}`)
	job.CollapseID = "collapse-1"
	job.Notification.ExpireAt = 1700000000

	result := newTestClient(t, fake, credential).Send(context.Background(), job)

	require.Equal(t, provider.StatusSent, result.GetStatus())
	request := fake.recorded()[0]
	require.Equal(t, "/3/device/device-token", request.path)
	require.True(t, strings.HasPrefix(request.authorization, "bearer "))
	require.Equal(t, "application/json; charset=utf-8", request.headers.Get("content-type"))
	require.Equal(t, "alert", request.headers.Get("apns-push-type"))
	require.Equal(t, testBundleID, request.headers.Get("apns-topic"))
	require.Equal(t, "collapse-1", request.headers.Get("apns-collapse-id"))
	require.Equal(t, "1700000000", request.headers.Get("apns-expiration"))
	require.Equal(t, `{"aps":{"alert":"hi"}}`, string(request.body))
}

func TestAPNsTargetPayloadOverridesRequestPayload(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	client := newTestClient(t, fake, credential)

	overridden := newTestJob(0, "a", nil, `{"request":true}`)
	overridden.Target.ApnsPayload = []byte(`{"target":true}`)
	plain := newTestJob(1, "b", nil, `{"request":true}`)

	client.Send(context.Background(), overridden)
	client.Send(context.Background(), plain)

	requests := fake.recorded()
	require.Equal(t, `{"target":true}`, string(requests[0].body))
	require.Equal(t, `{"request":true}`, string(requests[1].body))
}

func TestAPNsStatusAndReasonPassThrough(t *testing.T) {
	testCases := []struct {
		name           string
		status         int
		body           string
		expectedStatus string
		expectedReason string
		expectedDetail string
	}{
		{
			name:           "410 unregistered with timestamp",
			status:         http.StatusGone,
			body:           `{"reason":"Unregistered","timestamp":1700000001}`,
			expectedStatus: provider.StatusRejected,
			expectedReason: "Unregistered",
			expectedDetail: "1700000001",
		},
		{
			name:           "400 bad device token",
			status:         http.StatusBadRequest,
			body:           `{"reason":"BadDeviceToken"}`,
			expectedStatus: provider.StatusRejected,
			expectedReason: "BadDeviceToken",
		},
		{
			name:           "429 too many requests",
			status:         http.StatusTooManyRequests,
			body:           `{"reason":"TooManyRequests"}`,
			expectedStatus: provider.StatusRejected,
			expectedReason: "TooManyRequests",
		},
		{
			name:           "429 too many provider token updates is not repeated",
			status:         http.StatusTooManyRequests,
			body:           `{"reason":"TooManyProviderTokenUpdates"}`,
			expectedStatus: provider.StatusRejected,
			expectedReason: "TooManyProviderTokenUpdates",
		},
		{
			name:           "500 html body keeps no reason",
			status:         http.StatusInternalServerError,
			body:           `<html>error</html>`,
			expectedStatus: provider.StatusRejected,
		},
		{
			name:           "200 sent",
			status:         http.StatusOK,
			body:           "",
			expectedStatus: provider.StatusSent,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
			fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("apns-id", "3f2b8a64-1c2d-4e5f-8a9b-0c1d2e3f4a5b")
				writer.WriteHeader(testCase.status)
				_, _ = writer.Write([]byte(testCase.body))
			})

			result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

			require.Equal(t, testCase.expectedStatus, result.GetStatus())
			require.Equal(t, uint32(testCase.status), result.GetHttpStatus())
			require.Equal(t, testCase.expectedReason, result.GetProviderCode())
			require.Equal(t, testCase.expectedDetail, result.GetProviderDetail())
			require.Equal(t, "3f2b8a64-1c2d-4e5f-8a9b-0c1d2e3f4a5b", result.GetProviderMessageId())
		})
	}
}

func TestAPNsBodyErrorKeepsStatus(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		// The declared length is longer than the body, so the body read fails.
		writer.Header().Set("Content-Length", "100")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"rea`))
	})

	result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Equal(t, uint32(http.StatusOK), result.GetHttpStatus())
	require.Empty(t, result.GetProviderCode())
}

func TestAPNsPayloadLimits(t *testing.T) {
	testCases := []struct {
		name          string
		pushType      string
		size          int
		expectedState string
	}{
		{name: "alert 4096 sent", pushType: "alert", size: 4096, expectedState: provider.StatusSent},
		{name: "alert 4097 refused", pushType: "alert", size: 4097, expectedState: provider.StatusRejected},
		{name: "voip 5120 sent", pushType: "voip", size: 5120, expectedState: provider.StatusSent},
		{name: "voip 5121 refused", pushType: "voip", size: 5121, expectedState: provider.StatusRejected},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
			fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(http.StatusOK)
			})
			headers := map[string]string{"apns-push-type": testCase.pushType}
			payload := strings.Repeat("x", testCase.size)

			result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", headers, payload))

			require.Equal(t, testCase.expectedState, result.GetStatus())
			if testCase.expectedState == provider.StatusRejected {
				require.Equal(t, provider.CodePayloadTooLarge, result.GetCode())
				require.Empty(t, fake.recorded(), "a refused payload makes no request")
			}
		})
	}
}

// providerTokenFake answers 403 with reason for the first request, and 200 when the bearer carries the iat of the
// window the test expects. The clock moves to the next window while the first request is answered when moveClock is
// set.
func providerTokenFake(t *testing.T, reason string, expectedIAT int64, clock *fakeClock, moveTo time.Time) *fakeAPNs {
	t.Helper()

	var answered atomic.Int64
	return newFakeAPNs(t, func(writer http.ResponseWriter, request *http.Request) {
		if answered.Add(1) == 1 {
			if !moveTo.IsZero() {
				clock.Set(moveTo)
			}
			writer.WriteHeader(http.StatusForbidden)
			_, _ = writer.Write([]byte(`{"reason":"` + reason + `"}`))
			return
		}

		if issuedAtOf(t, strings.TrimPrefix(request.Header.Get("authorization"), "bearer ")) == expectedIAT {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusForbidden)
		_, _ = writer.Write([]byte(`{"reason":"` + reason + `"}`))
	})
}

func TestAPNsProviderTokenRepeatsAfterWindowChange(t *testing.T) {
	boundary := testWindow.Unix()
	clock := &fakeClock{now: time.Unix(boundary+100, 0)}
	credential, counter := newTestCredential(t, newTestKey(t), clock)
	nextWindow := time.Unix(boundary+2400+100, 0)

	fake := providerTokenFake(t, "ExpiredProviderToken", boundary+2400, clock, nextWindow)
	result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusSent, result.GetStatus())
	require.Len(t, fake.recorded(), 2)
	require.Equal(t, int64(2), counter.signs.Load())
}

func TestAPNsProviderTokenInsideWindowIsRaw(t *testing.T) {
	boundary := testWindow.Unix()
	clock := &fakeClock{now: time.Unix(boundary+100, 0)}
	credential, counter := newTestCredential(t, newTestKey(t), clock)

	fake := providerTokenFake(t, "InvalidProviderToken", boundary, clock, time.Time{})
	result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Equal(t, uint32(http.StatusForbidden), result.GetHttpStatus())
	require.Equal(t, "InvalidProviderToken", result.GetProviderCode())
	require.Len(t, fake.recorded(), 1, "no repeat and no new token inside the window")
	require.Equal(t, int64(1), counter.signs.Load())
}

// alwaysExpiredFake answers every request with 403 ExpiredProviderToken.
func alwaysExpiredFake(t *testing.T) *fakeAPNs {
	t.Helper()

	return newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
		_, _ = writer.Write([]byte(`{"reason":"ExpiredProviderToken"}`))
	})
}

func TestAPNsBurstSignsAtMostOncePerWindow(t *testing.T) {
	boundary := testWindow.Unix()
	clock := &fakeClock{now: time.Unix(boundary+100, 0)}
	credential, counter := newTestCredential(t, newTestKey(t), clock)

	fake := alwaysExpiredFake(t)
	client := newTestClient(t, fake, credential)
	for index := 0; index < 50; index++ {
		result := client.Send(context.Background(), newTestJob(index, "tok", nil, "{}"))
		require.Equal(t, provider.StatusRejected, result.GetStatus())
		require.Equal(t, uint32(http.StatusForbidden), result.GetHttpStatus())
	}
	require.Len(t, fake.recorded(), 50, "no item repeats inside the window")
	require.Equal(t, int64(1), counter.signs.Load(), "one signing for the window")

	clock.Set(time.Unix(boundary+2400+100, 0))
	for index := 0; index < 50; index++ {
		client.Send(context.Background(), newTestJob(index, "tok", nil, "{}"))
	}
	require.Equal(t, int64(2), counter.signs.Load(), "one more signing for the next window")
}

func TestAPNsConcurrentRefreshRace(t *testing.T) {
	boundary := testWindow.Unix()
	clock := &fakeClock{now: time.Unix(boundary+100, 0)}
	credential, counter := newTestCredential(t, newTestKey(t), clock)

	fake := alwaysExpiredFake(t)
	client := newTestClient(t, fake, credential)

	var wg sync.WaitGroup
	for index := 0; index < 100; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			client.Send(context.Background(), newTestJob(index, "tok", nil, "{}"))
		}(index)
	}
	wg.Wait()

	require.Len(t, fake.recorded(), 100)
	require.Equal(t, int64(1), counter.signs.Load())

	bearers := make(map[string]struct{})
	for _, request := range fake.recorded() {
		bearers[request.authorization] = struct{}{}
	}
	require.Len(t, bearers, 1, "all parallel items use the bearer of the window")
}

func TestAPNsOversizedAnswerFieldsAreTruncated(t *testing.T) {
	clock := &fakeClock{now: testWindow}
	credential, _ := newTestCredential(t, newTestKey(t), clock)

	longReason := strings.Repeat("R", 60<<10)
	longID := strings.Repeat("a", 60<<10)
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(headerID, longID)
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"reason":"` + longReason + `"}`))
	})

	result := newTestClient(t, fake, credential).Send(context.Background(), newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusRejected, result.GetStatus())
	require.Len(t, result.GetProviderCode(), provider.MaxFieldBytes)
	require.Len(t, result.GetProviderMessageId(), provider.MaxFieldBytes)
}

func TestAPNsMaxInflightHonoured(t *testing.T) {
	const targets = 10

	var inflight, maxInflight atomic.Int64
	entered := make(chan struct{}, targets)
	gate := make(chan struct{})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {
		current := inflight.Add(1)
		for {
			seen := maxInflight.Load()
			if current <= seen || maxInflight.CompareAndSwap(seen, current) {
				break
			}
		}
		entered <- struct{}{}
		<-gate
		inflight.Add(-1)
		writer.WriteHeader(http.StatusOK)
	})

	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	set := provider.NewSet()
	set.Add(provider.ProviderAPNs, newTestClient(t, fake, credential), 2)

	var wg sync.WaitGroup
	for index := 0; index < targets; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			set.Dispatch(context.Background(), 10*time.Second, newTestJob(index, "tok", nil, "{}"))
		}(index)
	}

	// Two requests reach the fake; the rest wait for a slot. Releasing the gate lets the rest run in turn.
	<-entered
	<-entered
	close(gate)
	wg.Wait()

	require.Equal(t, int64(2), maxInflight.Load())
	require.Len(t, fake.recorded(), targets)
}

func TestAPNsTimeoutIsUnanswered(t *testing.T) {
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := newTestClient(t, fake, credential).Send(ctx, newTestJob(0, "tok", nil, "{}"))

	require.Equal(t, provider.StatusUnanswered, result.GetStatus())
	require.Zero(t, result.GetHttpStatus())
	require.Empty(t, result.GetProviderCode())
}

func TestAPNsErrorsNeverCarryToken(t *testing.T) {
	const token = "SECRET-DEVICE-TOKEN-VALUE"
	credential, _ := newTestCredential(t, newTestKey(t), &fakeClock{now: testWindow})
	fake := newFakeAPNs(t, func(writer http.ResponseWriter, _ *http.Request) {})
	client := newTestClient(t, fake, credential)
	fake.server.Close()

	result := client.Send(context.Background(), newTestJob(0, token, nil, "{}"))

	require.Equal(t, provider.StatusUnanswered, result.GetStatus())
	require.NotContains(t, result.String(), token)
	require.NotContains(t, result.GetProviderDetail(), token)
}
