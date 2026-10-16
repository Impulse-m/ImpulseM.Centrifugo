package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/client"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// channelStateTestProxy is a channel state proxy which records the batches it receives and answers with err.
type channelStateTestProxy struct {
	mu      sync.Mutex
	batches [][]*proxyproto.ChannelEvent
	err     error
}

func (p *channelStateTestProxy) ProxyNotifyChannelState(
	_ context.Context,
	req *proxyproto.NotifyChannelStateRequest,
) (*proxyproto.NotifyChannelStateResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches = append(p.batches, req.Events)
	if p.err != nil {
		return nil, p.err
	}
	return &proxyproto.NotifyChannelStateResponse{}, nil
}

func (p *channelStateTestProxy) Protocol() string {
	return "test"
}

func (p *channelStateTestProxy) Timeout() time.Duration {
	return time.Second
}

// newChannelStateTestConfig returns the config for TestChannelStateEngineConfig: the example namespace enables channel
// state, the other one does not.
func newChannelStateTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Engine.Type = "redis"
	cfg.Channel.Proxy.State = newChannelEventsTestProxy("http://localhost:8000/state")
	cfg.Channel.Namespaces = []configtypes.ChannelNamespace{
		{Name: "example", ChannelOptions: configtypes.ChannelOptions{
			Presence:          true,
			StateProxyEnabled: true,
		}},
		{Name: "sample", ChannelOptions: configtypes.ChannelOptions{
			Presence: true,
		}},
	}
	return cfg
}

// TestChannelStateBatch checks the conversion of the events of Core to the events of the proxy.
func TestChannelStateBatch(t *testing.T) {
	when := time.UnixMilli(1697206286533)
	batch := channelStateBatch([]centrifuge.ChannelStateEvent{
		{Channel: "example:index", Type: centrifuge.ChannelStateOccupied, Time: when},
		{Channel: "example:index", Type: centrifuge.ChannelStateVacated, Time: when.Add(time.Second)},
	})
	require.Len(t, batch, 2)
	require.Equal(t, int64(1697206286533), batch[0].TimeMs)
	require.Equal(t, "example:index", batch[0].Channel)
	require.Equal(t, "occupied", batch[0].Type)
	require.Equal(t, int64(1697206287533), batch[1].TimeMs)
	require.Equal(t, "vacated", batch[1].Type)
	require.Empty(t, channelStateBatch(nil))
}

// TestChannelStateLayout checks the channel groups which channel state enables.
func TestChannelStateLayout(t *testing.T) {
	cfg := newChannelStateTestConfig()
	require.Equal(t, []string{"example"}, channelStateLayout(cfg))

	cfg.Channel.WithoutNamespace.StateProxyEnabled = true
	require.Equal(t, []string{"", "example"}, channelStateLayout(cfg))

	require.Empty(t, channelStateLayout(config.DefaultConfig()))
}

// TestCheckChannelStateReload checks that a reload keeps the channel groups with channel state of the startup
// config, and accepts the changes which do not move a channel between the presence keys.
func TestCheckChannelStateReload(t *testing.T) {
	startup := channelStateLayout(newChannelStateTestConfig())

	testCases := []struct {
		name    string
		modify  func(cfg *config.Config)
		wantErr bool
	}{
		{
			name:   "unchanged",
			modify: func(cfg *config.Config) {},
		},
		{
			name: "endpoint of the state proxy changed",
			modify: func(cfg *config.Config) {
				cfg.Channel.Proxy.State = newChannelEventsTestProxy("http://localhost:9000/state")
			},
		},
		{
			name: "namespace disabled",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces[0].StateProxyEnabled = false
			},
			wantErr: true,
		},
		{
			name: "namespace enabled",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces[1].StateProxyEnabled = true
			},
			wantErr: true,
		},
		{
			name: "namespace removed",
			modify: func(cfg *config.Config) {
				cfg.Channel.Namespaces = cfg.Channel.Namespaces[1:]
			},
			wantErr: true,
		},
		{
			name: "without namespace enabled",
			modify: func(cfg *config.Config) {
				cfg.Channel.WithoutNamespace.StateProxyEnabled = true
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newChannelStateTestConfig()
			tc.modify(&cfg)
			err := checkChannelStateReload(startup, cfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestChannelStateEngineConfig checks the config of the Redis presence manager: nil without a state proxy, and
// otherwise the namespaces which are enabled, the partitions and the vacated delay of the placement.
func TestChannelStateEngineConfig(t *testing.T) {
	cfg := newChannelStateTestConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	placement := configtypes.RedisChannelState{
		VacatedEventDelay: configtypes.Duration(5 * time.Second),
		NumPartitions:     8,
	}

	t.Run("no state proxy", func(t *testing.T) {
		require.Nil(t, channelStateEngineConfig(cfgContainer, &client.ProxyMap{}, placement))
	})

	t.Run("enabled channels", func(t *testing.T) {
		proxyMap := &client.ProxyMap{ChannelStateProxy: &channelStateTestProxy{}}
		chConfig := channelStateEngineConfig(cfgContainer, proxyMap, placement)
		require.NotNil(t, chConfig)
		require.True(t, chConfig.Enabled("example:index"))
		require.False(t, chConfig.Enabled("sample:index"))
		require.False(t, chConfig.Enabled("index"))
		require.False(t, chConfig.Enabled("missing:index"))
		require.Equal(t, 8, chConfig.NumPartitions)
		require.Equal(t, 5*time.Second, chConfig.VacatedEventDelay)
	})

	t.Run("zero vacated delay means no delay", func(t *testing.T) {
		proxyMap := &client.ProxyMap{ChannelStateProxy: &channelStateTestProxy{}}
		zero := placement
		zero.VacatedEventDelay = 0
		chConfig := channelStateEngineConfig(cfgContainer, proxyMap, zero)
		require.NotNil(t, chConfig)
		require.Negative(t, chConfig.VacatedEventDelay)
	})

	t.Run("handler delivers the batch", func(t *testing.T) {
		state := &channelStateTestProxy{}
		proxyMap := &client.ProxyMap{ChannelStateProxy: state}
		chConfig := channelStateEngineConfig(cfgContainer, proxyMap, placement)
		require.NotNil(t, chConfig)
		when := time.UnixMilli(1697206286533)
		err := chConfig.Handler(context.Background(), []centrifuge.ChannelStateEvent{
			{Channel: "example:index", Type: centrifuge.ChannelStateOccupied, Time: when},
		})
		require.NoError(t, err)
		require.Len(t, state.batches, 1)
		require.Equal(t, "example:index", state.batches[0][0].Channel)
	})

	t.Run("handler returns the proxy error", func(t *testing.T) {
		state := &channelStateTestProxy{err: errors.New("backend down")}
		proxyMap := &client.ProxyMap{ChannelStateProxy: state}
		chConfig := channelStateEngineConfig(cfgContainer, proxyMap, placement)
		require.NotNil(t, chConfig)
		err := chConfig.Handler(context.Background(), []centrifuge.ChannelStateEvent{
			{Channel: "example:index", Type: centrifuge.ChannelStateVacated, Time: time.Now()},
		})
		require.Error(t, err)
	})
}

// Compile-time check that the test proxy is a channel state proxy.
var _ proxy.ChannelStateProxy = (*channelStateTestProxy)(nil)
