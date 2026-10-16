//go:build integration

package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

const channelStateIntegrationRedisAddress = "localhost:6379"

// channelStateBackendEvent is one event of a channel state request, as the backend decodes it.
type channelStateBackendEvent struct {
	Channel string `json:"channel"`
	Type    string `json:"type"`
}

// channelStateBackendRequest is the body of a channel state request.
type channelStateBackendRequest struct {
	Events []channelStateBackendEvent `json:"events"`
}

// channelStateBackend is an HTTP channel state proxy which records the events it receives.
type channelStateBackend struct {
	mu     sync.Mutex
	events []channelStateBackendEvent
	server *httptest.Server
}

// newChannelStateBackend starts the backend, which is closed when the test ends.
func newChannelStateBackend(t testing.TB) *channelStateBackend {
	t.Helper()
	backend := &channelStateBackend{}
	backend.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req channelStateBackendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		backend.mu.Lock()
		backend.events = append(backend.events, req.Events...)
		backend.mu.Unlock()
		_, _ = w.Write([]byte(`{"result":{}}`))
	}))
	t.Cleanup(backend.server.Close)
	return backend
}

// received returns the events received so far.
func (b *channelStateBackend) received() []channelStateBackendEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]channelStateBackendEvent(nil), b.events...)
}

// hasEvent reports whether an event of the type was received for the channel.
func (b *channelStateBackend) hasEvent(channel string, eventType string) bool {
	for _, event := range b.received() {
		if event.Channel == channel && event.Type == eventType {
			return true
		}
	}
	return false
}

// newChannelStateIntegrationConfig returns a config with channel state on the example namespace, and the Redis presence
// manager in the placement of presenceManager: presence_manager when true, the engine otherwise. The prefix is unique
// per call, and the vacated delay is short.
func newChannelStateIntegrationConfig(endpoint string, presenceManager bool) config.Config {
	cfg := config.DefaultConfig()
	cfg.Channel.Proxy.State = configtypes.Proxy{
		Endpoint: endpoint,
		Timeout:  configtypes.Duration(time.Second),
	}
	cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
		{Name: "example", ChannelOptions: configtypes.ChannelOptions{
			Presence:          true,
			StateProxyEnabled: true,
		}},
		{Name: "sample", ChannelOptions: configtypes.ChannelOptions{
			Presence: true,
		}},
	}
	prefix := "channel_state_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	placement := configtypes.RedisChannelState{
		VacatedEventDelay: configtypes.Duration(200 * time.Millisecond),
		NumPartitions:     8,
	}
	if presenceManager {
		cfg.PresenceManager.Enabled = true
		cfg.PresenceManager.Type = "redis"
		cfg.PresenceManager.Redis.Prefix = prefix
		cfg.PresenceManager.Redis.Redis.Address = []string{channelStateIntegrationRedisAddress}
		cfg.PresenceManager.Redis.ChannelState = placement
		return cfg
	}
	cfg.Engine.Type = "redis"
	cfg.Engine.Redis.Prefix = prefix
	cfg.Engine.Redis.Redis.Address = []string{channelStateIntegrationRedisAddress}
	cfg.Engine.Redis.ChannelState = placement
	return cfg
}

// TestChannelStateRedisDelivery checks the wiring end to end, in both placements of channel_state: a channel of a
// namespace with channel state reaches the backend as occupied and, after the delay, as vacated; a channel of a
// namespace without channel state reaches it never.
func TestChannelStateRedisDelivery(t *testing.T) {
	testCases := []struct {
		name            string
		presenceManager bool
	}{
		{name: "presence manager", presenceManager: true},
		{name: "engine", presenceManager: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newChannelStateBackend(t)
			cfg := newChannelStateIntegrationConfig(backend.server.URL+"/state", tc.presenceManager)
			cfgContainer, err := config.NewContainer(cfg)
			require.NoError(t, err)
			proxyMap, _, err := buildProxyMap(cfg)
			require.NoError(t, err)
			require.NotNil(t, proxyMap.ChannelStateProxy)

			node, err := centrifuge.New(centrifuge.Config{})
			require.NoError(t, err)

			var presenceManager centrifuge.PresenceManager
			if tc.presenceManager {
				presenceManager, _, err = createRedisPresenceManager(node, cfgContainer, proxyMap)
			} else {
				_, presenceManager, _, err = createRedisEngine(node, cfgContainer, proxyMap)
			}
			require.NoError(t, err)
			t.Cleanup(func() {
				if closer, ok := presenceManager.(centrifuge.Closer); ok {
					_ = closer.Close(context.Background())
				}
			})

			info := &centrifuge.ClientInfo{ClientID: "client-1", UserID: "user-1"}
			require.NoError(t, presenceManager.AddPresence("example:index", "client-1", info))
			require.Eventually(t, func() bool {
				return backend.hasEvent("example:index", "occupied")
			}, 10*time.Second, 20*time.Millisecond)

			require.NoError(t, presenceManager.RemovePresence("example:index", "client-1", "user-1"))
			require.Eventually(t, func() bool {
				return backend.hasEvent("example:index", "vacated")
			}, 10*time.Second, 20*time.Millisecond)

			require.NoError(t, presenceManager.AddPresence("sample:index", "client-2", &centrifuge.ClientInfo{
				ClientID: "client-2",
				UserID:   "user-2",
			}))
			require.NoError(t, presenceManager.AddPresence("index", "client-3", &centrifuge.ClientInfo{
				ClientID: "client-3",
				UserID:   "user-3",
			}))
			require.Never(t, func() bool {
				return backend.hasEvent("sample:index", "occupied") || backend.hasEvent("index", "occupied")
			}, time.Second, 50*time.Millisecond)
		})
	}
}
