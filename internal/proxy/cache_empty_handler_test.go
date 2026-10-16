package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// newCacheEmptyTestContainer returns the config container with the options set on the without_namespace
// channels, or on the chat namespace when namespaced is true. Named proxies are declared for the names
// given, so the options can refer to them.
func newCacheEmptyTestContainer(t *testing.T, opts configtypes.ChannelOptions, namespaced bool, proxyNames ...string) *config.Container {
	cfg := config.DefaultConfig()
	cfg.Channel.Proxy.CacheEmpty = configtypes.Proxy{
		Endpoint: "http://localhost:8000/cache_empty",
		Timeout:  configtypes.Duration(time.Second),
	}
	for _, name := range proxyNames {
		cfg.Proxies = append(cfg.Proxies, configtypes.NamedProxy{
			Name: name,
			Proxy: configtypes.Proxy{
				Endpoint: "http://localhost:8000/" + name,
				Timeout:  configtypes.Duration(time.Second),
			},
		})
	}
	if namespaced {
		cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
			{Name: "chat", ChannelOptions: opts},
		}
	} else {
		cfg.Channel.WithoutNamespace = opts
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	return cfgContainer
}

// TestCacheEmptyHandlerHTTP checks the HTTP request body and the populated flag of the reply.
func TestCacheEmptyHandlerHTTP(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		response      string
		wantPopulated bool
		wantErr       bool
	}{
		{
			name:          "populated",
			status:        http.StatusOK,
			response:      `{"result": {"populated": true}}`,
			wantPopulated: true,
		},
		{
			name:     "not populated",
			status:   http.StatusOK,
			response: `{"result": {"populated": false}}`,
		},
		{
			name:     "empty body",
			status:   http.StatusOK,
			response: ``,
			wantErr:  true,
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
			testCase.Mux.HandleFunc("/cache_empty", func(w http.ResponseWriter, req *http.Request) {
				body, _ := io.ReadAll(req.Body)
				requestBodies <- string(body)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.response))
			})

			proxy, err := NewHTTPCacheEmptyProxy(getTestHttpProxy(testCase, "/cache_empty"))
			require.NoError(t, err)
			handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{"default": proxy})

			opts := configtypes.ChannelOptions{
				CacheEmptyProxyEnabled: true,
				CacheEmptyProxyName:    "default",
				ForceRecoveryMode:      "cache",
			}
			cfgContainer := newCacheEmptyTestContainer(t, opts, false)
			reply, err := handler.Handle(cfgContainer)(centrifuge.CacheEmptyEvent{Channel: "c"})

			require.JSONEq(t, `{"channel":"c"}`, <-requestBodies)
			if tt.wantErr {
				require.Equal(t, centrifuge.ErrorInternal, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPopulated, reply.Populated)
		})
	}
}

// cacheEmptyGRPCTestServer answers NotifyCacheEmpty with the result of respond.
type cacheEmptyGRPCTestServer struct {
	proxyproto.UnimplementedCentrifugoProxyServer
	respond func(*proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error)
}

func (s cacheEmptyGRPCTestServer) NotifyCacheEmpty(_ context.Context, req *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	return s.respond(req)
}

// TestCacheEmptyHandlerGRPC checks the gRPC call and the handling of its results.
func TestCacheEmptyHandlerGRPC(t *testing.T) {
	tests := []struct {
		name          string
		respond       func(*proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error)
		wantPopulated bool
		wantErr       bool
	}{
		{
			name: "populated",
			respond: func(*proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
				return &proxyproto.NotifyCacheEmptyResponse{Result: &proxyproto.NotifyCacheEmptyResult{Populated: true}}, nil
			},
			wantPopulated: true,
		},
		{
			name: "not populated",
			respond: func(*proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
				return &proxyproto.NotifyCacheEmptyResponse{Result: &proxyproto.NotifyCacheEmptyResult{}}, nil
			},
		},
		{
			name: "backend error",
			respond: func(*proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
				return nil, errors.New("backend failed")
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			received := make(chan string, 1)
			server := cacheEmptyGRPCTestServer{
				respond: func(req *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
					received <- req.Channel
					return tt.respond(req)
				},
			}
			testCase := tools.NewCommonGRPCProxyTestCase(context.Background(), server)
			defer testCase.Teardown()

			proxy, err := NewGRPCCacheEmptyProxy("default", getTestGrpcProxy(testCase))
			require.NoError(t, err)
			handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{"default": proxy})

			opts := configtypes.ChannelOptions{
				CacheEmptyProxyEnabled: true,
				CacheEmptyProxyName:    "default",
				ForceRecoveryMode:      "cache",
			}
			cfgContainer := newCacheEmptyTestContainer(t, opts, false)
			reply, err := handler.Handle(cfgContainer)(centrifuge.CacheEmptyEvent{Channel: "c"})

			require.Equal(t, "c", <-received)
			if tt.wantErr {
				require.Equal(t, centrifuge.ErrorInternal, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPopulated, reply.Populated)
		})
	}
}

// TestCacheEmptyHandlerDisabled checks that a channel whose namespace has cache_empty_proxy_enabled off
// gets an empty reply without calling the backend.
func TestCacheEmptyHandlerDisabled(t *testing.T) {
	testCase := tools.NewCommonHTTPProxyTestCase(context.Background())
	defer testCase.Teardown()

	calls := make(chan struct{}, 10)
	testCase.Mux.HandleFunc("/cache_empty", func(w http.ResponseWriter, req *http.Request) {
		calls <- struct{}{}
		_, _ = w.Write([]byte(`{"result": {"populated": true}}`))
	})

	proxy, err := NewHTTPCacheEmptyProxy(getTestHttpProxy(testCase, "/cache_empty"))
	require.NoError(t, err)
	handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{"default": proxy})

	opts := configtypes.ChannelOptions{
		CacheEmptyProxyEnabled: false,
		CacheEmptyProxyName:    "default",
	}
	cfgContainer := newCacheEmptyTestContainer(t, opts, true)
	reply, err := handler.Handle(cfgContainer)(centrifuge.CacheEmptyEvent{Channel: "chat:index"})

	require.NoError(t, err)
	require.Equal(t, centrifuge.CacheEmptyReply{}, reply)
	require.Empty(t, calls)
}

// cacheEmptyNilProxy answers every call with neither a result nor an error.
type cacheEmptyNilProxy struct{}

func (p *cacheEmptyNilProxy) ProxyNotifyCacheEmpty(_ context.Context, _ *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	return nil, nil
}

func (p *cacheEmptyNilProxy) Protocol() string {
	return "test"
}

func (p *cacheEmptyNilProxy) Timeout() time.Duration {
	return time.Second
}

// TestCacheEmptyHandlerNilResponse checks that a proxy which returns no response rejects the subscription
// with the internal error, without a panic.
func TestCacheEmptyHandlerNilResponse(t *testing.T) {
	handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{"default": &cacheEmptyNilProxy{}})

	opts := configtypes.ChannelOptions{
		CacheEmptyProxyEnabled: true,
		CacheEmptyProxyName:    "default",
		ForceRecoveryMode:      "cache",
	}
	cfgContainer := newCacheEmptyTestContainer(t, opts, true)
	reply, err := handler.Handle(cfgContainer)(centrifuge.CacheEmptyEvent{Channel: "chat:index"})

	require.ErrorIs(t, err, centrifuge.ErrorInternal)
	require.Equal(t, centrifuge.CacheEmptyReply{}, reply)
}

// TestCacheEmptyHandlerNamedProxy checks that the proxy is picked by cache_empty_proxy_name.
func TestCacheEmptyHandlerNamedProxy(t *testing.T) {
	testCase := tools.NewCommonHTTPProxyTestCase(context.Background())
	defer testCase.Teardown()

	defaultCalls := make(chan struct{}, 10)
	testCase.Mux.HandleFunc("/default", func(w http.ResponseWriter, req *http.Request) {
		defaultCalls <- struct{}{}
		_, _ = w.Write([]byte(`{"result": {"populated": false}}`))
	})
	testCase.Mux.HandleFunc("/other", func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(`{"result": {"populated": true}}`))
	})

	defaultProxy, err := NewHTTPCacheEmptyProxy(getTestHttpProxy(testCase, "/default"))
	require.NoError(t, err)
	otherProxy, err := NewHTTPCacheEmptyProxy(getTestHttpProxy(testCase, "/other"))
	require.NoError(t, err)
	handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{
		"default": defaultProxy,
		"other":   otherProxy,
	})

	opts := configtypes.ChannelOptions{
		CacheEmptyProxyEnabled: true,
		CacheEmptyProxyName:    "other",
		ForceRecoveryMode:      "cache",
	}
	cfgContainer := newCacheEmptyTestContainer(t, opts, true, "other")
	reply, err := handler.Handle(cfgContainer)(centrifuge.CacheEmptyEvent{Channel: "chat:index"})

	require.NoError(t, err)
	require.True(t, reply.Populated)
	require.Empty(t, defaultCalls)
}

// TestCacheEmptyHandlerOwnsProxies checks that a proxy added to the caller's map after construction is not used:
// the subscription is rejected with the internal error, without a panic.
func TestCacheEmptyHandlerOwnsProxies(t *testing.T) {
	proxies := map[string]CacheEmptyProxy{"default": &cacheEmptyNilProxy{}}
	handler := NewCacheEmptyHandler(proxies)
	proxies["late"] = &cacheEmptyNilProxy{}

	opts := configtypes.ChannelOptions{
		CacheEmptyProxyEnabled: true,
		CacheEmptyProxyName:    "late",
		ForceRecoveryMode:      "cache",
	}
	cfgContainer := newCacheEmptyTestContainer(t, opts, true, "late")
	reply, err := handler.Handle(cfgContainer)(centrifuge.CacheEmptyEvent{Channel: "chat:index"})

	require.ErrorIs(t, err, centrifuge.ErrorInternal)
	require.Equal(t, centrifuge.CacheEmptyReply{}, reply)
}

// blockingCacheEmptyProxy counts its calls, reports each call on entered and holds it until release is closed.
type blockingCacheEmptyProxy struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (p *blockingCacheEmptyProxy) ProxyNotifyCacheEmpty(_ context.Context, _ *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	p.calls.Add(1)
	p.entered <- struct{}{}
	<-p.release
	return &proxyproto.NotifyCacheEmptyResponse{
		Result: &proxyproto.NotifyCacheEmptyResult{Populated: true},
	}, nil
}

func (p *blockingCacheEmptyProxy) Protocol() string {
	return "test"
}

func (p *blockingCacheEmptyProxy) Timeout() time.Duration {
	return time.Second
}

// TestCacheEmptyHandlerCallsEverySubscriber checks that subscribers which find the same channel empty while a call
// is in flight each call the backend: a subscriber's answer is decided after its own arrival, so it never joins
// a call started before it. The backend holds both calls until both have arrived.
func TestCacheEmptyHandlerCallsEverySubscriber(t *testing.T) {
	const subscribers = 2

	proxy := &blockingCacheEmptyProxy{
		entered: make(chan struct{}, subscribers),
		release: make(chan struct{}),
	}
	handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{"default": proxy})
	opts := configtypes.ChannelOptions{
		CacheEmptyProxyEnabled: true,
		CacheEmptyProxyName:    "default",
		ForceRecoveryMode:      "cache",
	}
	cfgContainer := newCacheEmptyTestContainer(t, opts, false)
	cacheEmpty := handler.Handle(cfgContainer)

	replies := make(chan centrifuge.CacheEmptyReply, subscribers)
	errs := make(chan error, subscribers)
	for i := 0; i < subscribers; i++ {
		go func() {
			reply, err := cacheEmpty(centrifuge.CacheEmptyEvent{Channel: "c"})
			replies <- reply
			errs <- err
		}()
	}
	for i := 0; i < subscribers; i++ {
		select {
		case <-proxy.entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("fewer than %d subscribers reached the backend", subscribers)
		}
	}
	close(proxy.release)
	for i := 0; i < subscribers; i++ {
		require.NoError(t, <-errs)
		require.True(t, (<-replies).Populated)
	}
	require.Equal(t, int64(subscribers), proxy.calls.Load())
}
