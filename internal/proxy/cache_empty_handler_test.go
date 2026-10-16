package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
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

// blockingCacheEmptyProxy counts its calls and holds each one until release is closed.
type blockingCacheEmptyProxy struct {
	calls   atomic.Int64
	release chan struct{}
}

func (p *blockingCacheEmptyProxy) ProxyNotifyCacheEmpty(_ context.Context, _ *proxyproto.NotifyCacheEmptyRequest) (*proxyproto.NotifyCacheEmptyResponse, error) {
	p.calls.Add(1)
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

// singleflightDoFrame is the stack frame of a goroutine that is inside singleflight.Group.Do, either as the
// caller which runs the function or as a caller which waits for it.
const singleflightDoFrame = "singleflight.(*Group).Do("

// waitForSingleflightCallers returns once want goroutines are inside singleflight.Group.Do. A caller inside Do
// while the call is in flight joins that call, so the test releases the backend only after every subscriber
// has joined it. It fails the test when the callers do not get there in time.
func waitForSingleflightCallers(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	const maxStackBufSize = 1 << 26
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		// A full buffer may hold a truncated dump, so it is doubled and the dump is taken again.
		if n == len(buf) && len(buf) < maxStackBufSize {
			buf = make([]byte, 2*len(buf))
			continue
		}
		if strings.Count(string(buf[:n]), singleflightDoFrame) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fewer than %d subscribers reached the in-flight call", want)
		}
		runtime.Gosched()
	}
}

// TestCacheEmptyHandlerCoalescesInflightCalls checks that subscribers which find the same channel empty while
// a call is in flight share one backend call. The release of the backend waits until every subscriber is inside
// the shared call, see waitForSingleflightCallers, so all of them join the same call. Without coalescing no
// subscriber waits in singleflight, and the wait fails. Without a shared result the count assertion fails.
func TestCacheEmptyHandlerCoalescesInflightCalls(t *testing.T) {
	const subscribers = 16

	proxy := &blockingCacheEmptyProxy{release: make(chan struct{})}
	handler := NewCacheEmptyHandler(map[string]CacheEmptyProxy{"default": proxy})
	opts := configtypes.ChannelOptions{
		CacheEmptyProxyEnabled: true,
		CacheEmptyProxyName:    "default",
		ForceRecoveryMode:      "cache",
	}
	cfgContainer := newCacheEmptyTestContainer(t, opts, false)
	cacheEmpty := handler.Handle(cfgContainer)

	start := make(chan struct{})
	replies := make(chan centrifuge.CacheEmptyReply, subscribers)
	errs := make(chan error, subscribers)
	var done sync.WaitGroup
	for i := 0; i < subscribers; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			<-start
			reply, err := cacheEmpty(centrifuge.CacheEmptyEvent{Channel: "c"})
			replies <- reply
			errs <- err
		}()
	}
	close(start)
	waitForSingleflightCallers(t, subscribers)
	close(proxy.release)
	done.Wait()
	close(replies)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for reply := range replies {
		require.True(t, reply.Populated)
	}
	require.Equal(t, int64(1), proxy.calls.Load())

	// A call made after the shared one has finished goes to the backend again: nothing is cached.
	reply, err := cacheEmpty(centrifuge.CacheEmptyEvent{Channel: "c"})
	require.NoError(t, err)
	require.True(t, reply.Populated)
	require.Equal(t, int64(2), proxy.calls.Load())
}
