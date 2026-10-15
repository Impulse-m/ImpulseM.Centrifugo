package proxy

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
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
