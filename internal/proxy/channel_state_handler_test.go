package proxy

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const channelStateTestBody = `{"events":[{"time_ms":1697206286533,"channel":"chat:index","type":"occupied"}]}`

func channelStateTestEvents() []*proxyproto.ChannelEvent {
	return []*proxyproto.ChannelEvent{
		{TimeMs: 1697206286533, Channel: "chat:index", Type: "occupied"},
	}
}

// channelStateGRPCTestServer answers NotifyChannelState with the response returned by respond.
type channelStateGRPCTestServer struct {
	proxyproto.UnimplementedCentrifugoProxyServer
	respond func(*proxyproto.NotifyChannelStateRequest) *proxyproto.NotifyChannelStateResponse
}

func (s channelStateGRPCTestServer) NotifyChannelState(_ context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	return s.respond(req), nil
}

// TestChannelStateHandlerHTTP checks the HTTP request body and the handling of the responses.
func TestChannelStateHandlerHTTP(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		response string
		wantErr  bool
		wantCode uint32
	}{
		{
			name:     "result",
			status:   http.StatusOK,
			response: `{"result": {}}`,
		},
		{
			name:     "error in response",
			status:   http.StatusOK,
			response: `{"error": {"code": 100, "message": "x"}}`,
			wantErr:  true,
			wantCode: 100,
		},
		{
			name:     "unexpected status",
			status:   http.StatusInternalServerError,
			response: `{}`,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCase := tools.NewCommonHTTPProxyTestCase(context.Background())
			defer testCase.Teardown()

			requestBodies := make(chan string, 1)
			testCase.Mux.HandleFunc("/channel_state", func(w http.ResponseWriter, req *http.Request) {
				body, _ := io.ReadAll(req.Body)
				requestBodies <- string(body)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.response))
			})

			proxy, err := NewHTTPChannelStateProxy(getTestHttpProxy(testCase, "/channel_state"))
			require.NoError(t, err)
			handle := NewChannelStateHandler(proxy).Handle()

			errorsBefore := testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("http", "state", "default"))
			err = handle(context.Background(), channelStateTestEvents())
			errorsAfter := testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("http", "state", "default"))

			require.JSONEq(t, channelStateTestBody, <-requestBodies)
			if !tt.wantErr {
				require.NoError(t, err)
				require.Equal(t, errorsBefore, errorsAfter)
				return
			}
			require.Error(t, err)
			require.Equal(t, errorsBefore+1, errorsAfter)
			if tt.wantCode != 0 {
				var protoErr *centrifuge.Error
				require.ErrorAs(t, err, &protoErr)
				require.Equal(t, tt.wantCode, protoErr.Code)
			}
		})
	}
}

// TestChannelStateHandlerHTTPTimeout checks that the timeout of the handler bounds a call to a slow backend.
func TestChannelStateHandlerHTTPTimeout(t *testing.T) {
	testCase := tools.NewCommonHTTPProxyTestCase(context.Background())
	defer testCase.Teardown()

	testCase.Mux.HandleFunc("/channel_state", func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
	})

	timeout := 100 * time.Millisecond
	proxyConfig := getTestHttpProxy(testCase, "/channel_state")
	proxyConfig.Timeout = configtypes.Duration(timeout)
	proxy, err := NewHTTPChannelStateProxy(proxyConfig)
	require.NoError(t, err)
	handle := NewChannelStateHandler(proxy).Handle()

	started := time.Now()
	err = handle(context.Background(), channelStateTestEvents())
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
}

// TestChannelStateHandlerGRPC checks the gRPC call and the handling of the responses.
func TestChannelStateHandlerGRPC(t *testing.T) {
	tests := []struct {
		name     string
		response *proxyproto.NotifyChannelStateResponse
		wantErr  bool
		wantCode uint32
	}{
		{
			name:     "result",
			response: &proxyproto.NotifyChannelStateResponse{Result: &proxyproto.NotifyChannelStateResult{}},
		},
		{
			name: "error in response",
			response: &proxyproto.NotifyChannelStateResponse{
				Error: &proxyproto.Error{Code: 100, Message: "x"},
			},
			wantErr:  true,
			wantCode: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			received := make(chan []*proxyproto.ChannelEvent, 1)
			server := channelStateGRPCTestServer{
				respond: func(req *proxyproto.NotifyChannelStateRequest) *proxyproto.NotifyChannelStateResponse {
					received <- req.Events
					return tt.response
				},
			}
			testCase := tools.NewCommonGRPCProxyTestCase(context.Background(), server)
			defer testCase.Teardown()

			proxy, err := NewGRPCChannelStateProxy("default", getTestGrpcProxy(testCase))
			require.NoError(t, err)
			handle := NewChannelStateHandler(proxy).Handle()

			errorsBefore := testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("grpc", "state", "default"))
			err = handle(context.Background(), channelStateTestEvents())
			errorsAfter := testutil.ToFloat64(metrics.ProxyCallErrorCount.WithLabelValues("grpc", "state", "default"))

			events := <-received
			require.Len(t, events, 1)
			require.Equal(t, "chat:index", events[0].Channel)
			require.Equal(t, "occupied", events[0].Type)
			if !tt.wantErr {
				require.NoError(t, err)
				require.Equal(t, errorsBefore, errorsAfter)
				return
			}
			require.Error(t, err)
			require.Equal(t, errorsBefore+1, errorsAfter)
			var protoErr *centrifuge.Error
			require.ErrorAs(t, err, &protoErr)
			require.Equal(t, tt.wantCode, protoErr.Code)
		})
	}
}

// channelStateRejectingProxy rejects the batches whose first event has one of the channels in rejects.
type channelStateRejectingProxy struct {
	rejects map[string]bool
}

func (p *channelStateRejectingProxy) ProxyNotifyChannelState(_ context.Context, req *proxyproto.NotifyChannelStateRequest) (*proxyproto.NotifyChannelStateResponse, error) {
	if p.rejects[req.Events[0].Channel] {
		return &proxyproto.NotifyChannelStateResponse{
			Error: &proxyproto.Error{Code: 100, Message: "rejected"},
		}, nil
	}
	return &proxyproto.NotifyChannelStateResponse{}, nil
}

func (p *channelStateRejectingProxy) Protocol() string {
	return "test"
}

func (p *channelStateRejectingProxy) Timeout() time.Duration {
	return time.Second
}

// TestChannelStateHandlerOldestFailingBatch checks the gauge of the batch that the proxy rejects for the longest
// time: it is the time the oldest rejected batch was first sent, and it returns to 0 once every batch is delivered.
func TestChannelStateHandlerOldestFailingBatch(t *testing.T) {
	proxy := &channelStateRejectingProxy{rejects: map[string]bool{}}
	handle := NewChannelStateHandler(proxy).Handle()
	gauge := func() float64 {
		return testutil.ToFloat64(metrics.ChannelStateOldestFailingBatchTimestamp)
	}
	batch := func(channel string) []*proxyproto.ChannelEvent {
		return []*proxyproto.ChannelEvent{{TimeMs: 1697206286533, Channel: channel, Type: "occupied"}}
	}
	const tolerance = 1e-3

	proxy.rejects["a"] = true
	before := float64(time.Now().UnixNano()) / float64(time.Second)
	require.Error(t, handle(context.Background(), batch("a")))
	after := float64(time.Now().UnixNano()) / float64(time.Second)
	firstFailure := gauge()
	require.GreaterOrEqual(t, firstFailure, before-tolerance)
	require.LessOrEqual(t, firstFailure, after+tolerance)

	// A second rejected batch does not move the gauge: the oldest one still counts.
	proxy.rejects["b"] = true
	require.Error(t, handle(context.Background(), batch("b")))
	require.Equal(t, firstFailure, gauge())

	// A batch rejected again keeps its first send time.
	require.Error(t, handle(context.Background(), batch("a")))
	require.Equal(t, firstFailure, gauge())

	// Delivering the oldest batch moves the gauge to the next rejected one, which was sent later.
	delete(proxy.rejects, "a")
	require.NoError(t, handle(context.Background(), batch("a")))
	require.NotZero(t, gauge())
	require.GreaterOrEqual(t, gauge(), firstFailure)

	// Delivering the last rejected batch clears the gauge.
	delete(proxy.rejects, "b")
	require.NoError(t, handle(context.Background(), batch("b")))
	require.Zero(t, gauge())
}
