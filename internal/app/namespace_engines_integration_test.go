//go:build integration

package app

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/confighelpers"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// countingBrokerEventHandler counts the publications which a broker receives.
type countingBrokerEventHandler struct {
	publications atomic.Int64
}

var _ centrifuge.BrokerEventHandler = (*countingBrokerEventHandler)(nil)

func (h *countingBrokerEventHandler) HandlePublication(_ string, _ *centrifuge.Publication, _ centrifuge.StreamPosition, _ bool, _ *centrifuge.Publication) error {
	h.publications.Add(1)
	return nil
}

func (h *countingBrokerEventHandler) HandleJoin(_ string, _ *centrifuge.ClientInfo) error {
	return nil
}

func (h *countingBrokerEventHandler) HandleLeave(_ string, _ *centrifuge.ClientInfo) error {
	return nil
}

func TestNamespaceEnginesRedisIntegration(t *testing.T) {
	ctx := context.Background()
	goroutinesBefore := runtime.NumGoroutine()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	defaultPrefix := "t-" + suffix + "-default"
	isoPrefix := "t-" + suffix + "-iso"
	redisAddress := []string{"redis://127.0.0.1:6379"}

	cfg := config.DefaultConfig()
	cfg.Engine.Type = "redis"
	cfg.Engine.Redis.Prefix = defaultPrefix
	cfg.Engine.Redis.Redis.Address = redisAddress
	var iso configtypes.NamedBroker
	iso.Name = "iso"
	iso.Enabled = true
	iso.Type = "redis"
	iso.Redis.Prefix = isoPrefix
	iso.Redis.Redis.Address = redisAddress
	cfg.Brokers = configtypes.NamedBrokers{iso}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "iso", ChannelOptions: configtypes.ChannelOptions{BrokerName: "iso"}},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	engines := newNamespaceEngines(cfgContainer)
	nodeCfg := centrifuge.Config{}
	engines.nodeConfig(&nodeCfg)
	node, err := centrifuge.New(nodeCfg)
	require.NoError(t, err)
	defaultBroker, defaultPresenceManager, _, err := createRedisEngine(node, cfgContainer)
	require.NoError(t, err)
	node.SetBroker(defaultBroker)
	node.SetPresenceManager(defaultPresenceManager)
	require.NoError(t, engines.create(node, cfgContainer))
	require.NoError(t, node.Run())
	require.NoError(t, engines.run(node))
	goroutinesRunning := runtime.NumGoroutine()
	t.Cleanup(func() {
		_ = node.Shutdown(ctx)
		engines.close()
	})

	// A second node with a broker on the iso prefix stands for a subscriber of iso:x: it receives each
	// publication which goes through the iso prefix, once per subscription.
	listenerNode, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	listenerShards, _, err := confighelpers.CentrifugeRedisShards(listenerNode, redisAddressConfig(redisAddress))
	require.NoError(t, err)
	listenerBroker, err := centrifuge.NewRedisBroker(listenerNode, centrifuge.RedisBrokerConfig{
		Shards: listenerShards,
		Prefix: isoPrefix,
	})
	require.NoError(t, err)
	handler := &countingBrokerEventHandler{}
	require.NoError(t, listenerBroker.RegisterBrokerEventHandler(handler))
	require.NoError(t, listenerBroker.Subscribe("iso:x"))
	t.Cleanup(func() {
		_ = listenerBroker.Close(ctx)
		_ = listenerNode.Shutdown(ctx)
	})

	_, err = node.Publish("iso:x", []byte(`{"n":1}`), centrifuge.WithHistory(10, time.Minute))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return handler.publications.Load() == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Never(t, func() bool {
		return handler.publications.Load() != 1
	}, 300*time.Millisecond, 20*time.Millisecond)

	// The keys are checked before any History call: reading history of a channel can create its meta key.
	redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	require.NoError(t, err)
	t.Cleanup(redisClient.Close)
	isoKeys, err := redisClient.Do(ctx, redisClient.B().Keys().Pattern(isoPrefix+"*").Build()).AsStrSlice()
	require.NoError(t, err)
	require.NotEmpty(t, isoKeys)
	defaultKeys, err := redisClient.Do(ctx, redisClient.B().Keys().Pattern(defaultPrefix+"*").Build()).AsStrSlice()
	require.NoError(t, err)
	require.Empty(t, defaultKeys)

	// History is kept by the iso broker and not by the default one.
	historyOptions := centrifuge.HistoryOptions{Filter: centrifuge.HistoryFilter{Limit: -1}}
	isoPubs, _, err := engines.brokers["iso"].History("iso:x", historyOptions)
	require.NoError(t, err)
	require.Len(t, isoPubs, 1)
	defaultPubs, _, err := defaultBroker.History("iso:x", historyOptions)
	require.NoError(t, err)
	require.Empty(t, defaultPubs)

	t.Logf("goroutines before %d, with engines running %d", goroutinesBefore, goroutinesRunning)
}

// redisAddressConfig returns the Redis configuration of the addresses, with the defaults of the config.
func redisAddressConfig(address []string) configtypes.Redis {
	redisCfg := config.DefaultConfig().Engine.Redis.Redis
	redisCfg.Address = address
	return redisCfg
}
