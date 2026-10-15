package app

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func memoryNamedBroker(name string) configtypes.NamedBroker {
	var b configtypes.NamedBroker
	b.Name = name
	b.Enabled = true
	b.Type = "memory"
	return b
}

func memoryNamedPresenceManager(name string) configtypes.NamedPresenceManager {
	var pm configtypes.NamedPresenceManager
	pm.Name = name
	pm.Enabled = true
	pm.Type = "memory"
	return pm
}

// newNamespaceEnginesForTest returns the engines of cfg, with the node config hooks applied as in
// production, and the node created with them. The node is not run.
func newNamespaceEnginesForTest(t *testing.T, cfg config.Config) (*namespaceEngines, *config.Container, *centrifuge.Node) {
	t.Helper()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	engines := newNamespaceEngines(cfgContainer)

	nodeCfg := centrifuge.Config{}
	engines.nodeConfig(&nodeCfg)
	node, err := centrifuge.New(nodeCfg)
	require.NoError(t, err)
	return engines, cfgContainer, node
}

func TestNamespaceEnginesNilWhenUnused(t *testing.T) {
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	require.Nil(t, newNamespaceEngines(cfgContainer))

	// Disabled entries and no channel option selecting one: nothing to route, nothing to create.
	cfg := config.DefaultConfig()
	disabled := memoryNamedBroker("alpha")
	disabled.Enabled = false
	cfg.Brokers = configtypes.NamedBrokers{disabled}
	cfgContainer, err = config.NewContainer(cfg)
	require.NoError(t, err)
	require.Nil(t, newNamespaceEngines(cfgContainer))
}

func TestNamespaceEnginesEnabledEntriesWithoutRouting(t *testing.T) {
	// Enabled entries are created even when no namespace selects them yet, so their connections are checked
	// at startup and a reload may route a namespace to them. The node still gets no routing hooks.
	cfg := config.DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("alpha")}
	engines, cfgContainer, node := newNamespaceEnginesForTest(t, cfg)
	require.NotNil(t, engines)
	require.False(t, engines.routeBrokers)
	require.False(t, engines.routePresence)

	nodeCfg := centrifuge.Config{}
	engines.nodeConfig(&nodeCfg)
	require.Nil(t, nodeCfg.GetBroker)
	require.Nil(t, nodeCfg.GetPresenceManager)

	require.NoError(t, engines.create(node, cfgContainer))
	require.Contains(t, engines.brokers, "alpha")
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
}

func TestNamespaceEnginesNodeHooksOnlyForRoutedKind(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("alpha")}
	cfg.PresenceManagers = configtypes.NamedPresenceManagers{memoryNamedPresenceManager("presence")}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "one", ChannelOptions: configtypes.ChannelOptions{BrokerName: "alpha"}},
	}
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	engines := newNamespaceEngines(cfgContainer)
	require.NotNil(t, engines)

	nodeCfg := centrifuge.Config{}
	engines.nodeConfig(&nodeCfg)
	require.NotNil(t, nodeCfg.GetBroker)
	require.Nil(t, nodeCfg.GetPresenceManager)
}

func TestNamespaceEnginesNilReceiver(t *testing.T) {
	var engines *namespaceEngines

	nodeCfg := centrifuge.Config{}
	engines.nodeConfig(&nodeCfg)
	require.Nil(t, nodeCfg.GetBroker)
	require.Nil(t, nodeCfg.GetPresenceManager)

	broker, ok := engines.broker("one:x")
	require.Nil(t, broker)
	require.False(t, ok)
	presenceManager, ok := engines.presenceManager("one:x")
	require.Nil(t, presenceManager)
	require.False(t, ok)

	require.NoError(t, engines.create(nil, nil))
	require.NoError(t, engines.run(nil))
	engines.close()
}

func TestNamespaceEnginesRouting(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("alpha"), memoryNamedBroker("beta")}
	cfg.PresenceManagers = configtypes.NamedPresenceManagers{memoryNamedPresenceManager("presence")}
	cfg.Channel.WithoutNamespace.BrokerName = "alpha"
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "one", ChannelOptions: configtypes.ChannelOptions{BrokerName: "alpha"}},
		{Name: "two", ChannelOptions: configtypes.ChannelOptions{BrokerName: "beta", PresenceManagerName: "presence"}},
		{Name: "three"},
	}
	engines, cfgContainer, node := newNamespaceEnginesForTest(t, cfg)
	require.NoError(t, engines.create(node, cfgContainer))
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	brokerA := engines.brokers["alpha"]
	brokerB := engines.brokers["beta"]
	presenceP := engines.presenceManagers["presence"]
	require.NotNil(t, brokerA)
	require.NotNil(t, brokerB)
	require.NotNil(t, presenceP)

	broker, ok := engines.broker("one:x")
	require.True(t, ok)
	require.Same(t, brokerA, broker)

	broker, ok = engines.broker("two:x")
	require.True(t, ok)
	require.Same(t, brokerB, broker)

	broker, ok = engines.broker("three:x")
	require.False(t, ok)
	require.Nil(t, broker)

	broker, ok = engines.broker("unknown:x")
	require.False(t, ok)
	require.Nil(t, broker)

	// Channel without namespace uses the top-level channel options.
	broker, ok = engines.broker("top")
	require.True(t, ok)
	require.Same(t, brokerA, broker)

	presenceManager, ok := engines.presenceManager("two:x")
	require.True(t, ok)
	require.Same(t, presenceP, presenceManager)

	presenceManager, ok = engines.presenceManager("one:x")
	require.False(t, ok)
	require.Nil(t, presenceManager)

	presenceManager, ok = engines.presenceManager("three:x")
	require.False(t, ok)
	require.Nil(t, presenceManager)
}

func TestNamespaceEnginesDisabledEntryNotCreated(t *testing.T) {
	cfg := config.DefaultConfig()
	disabledBroker := memoryNamedBroker("off")
	disabledBroker.Enabled = false
	disabledPresence := memoryNamedPresenceManager("off")
	disabledPresence.Enabled = false
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("on"), disabledBroker}
	cfg.PresenceManagers = configtypes.NamedPresenceManagers{disabledPresence}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "one", ChannelOptions: configtypes.ChannelOptions{BrokerName: "on"}},
	}
	engines, cfgContainer, node := newNamespaceEnginesForTest(t, cfg)
	require.NoError(t, engines.create(node, cfgContainer))
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	require.Len(t, engines.brokers, 1)
	require.Contains(t, engines.brokers, "on")
	require.NotContains(t, engines.brokers, "off")
	require.Empty(t, engines.presenceManagers)
}

func TestNamespaceEnginesPublishReachesNamedBroker(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("alpha")}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "iso", ChannelOptions: configtypes.ChannelOptions{BrokerName: "alpha"}},
	}
	engines, cfgContainer, node := newNamespaceEnginesForTest(t, cfg)

	defaultBroker, defaultPresence, err := createMemoryEngine(node)
	require.NoError(t, err)
	node.SetBroker(defaultBroker)
	node.SetPresenceManager(defaultPresence)

	require.NoError(t, engines.create(node, cfgContainer))
	require.NoError(t, node.Run())
	require.NoError(t, engines.run(node))
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		engines.close()
	})

	named := engines.brokers["alpha"]
	historyOptions := centrifuge.HistoryOptions{Filter: centrifuge.HistoryFilter{Limit: -1}}

	_, err = node.Publish("iso:x", []byte(`{}`), centrifuge.WithHistory(10, 60*time.Second))
	require.NoError(t, err)
	pubs, _, err := named.History("iso:x", historyOptions)
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	pubs, _, err = defaultBroker.History("iso:x", historyOptions)
	require.NoError(t, err)
	require.Empty(t, pubs)

	_, err = node.Publish("other:x", []byte(`{}`), centrifuge.WithHistory(10, 60*time.Second))
	require.NoError(t, err)
	pubs, _, err = defaultBroker.History("other:x", historyOptions)
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	pubs, _, err = named.History("other:x", historyOptions)
	require.NoError(t, err)
	require.Empty(t, pubs)
}

// closeRecordingBroker is a broker which only records its Close call.
type closeRecordingBroker struct {
	centrifuge.Broker
	closed chan struct{}
}

func (b *closeRecordingBroker) Close(_ context.Context) error {
	close(b.closed)
	return nil
}

func TestNamespaceEnginesCloseOnShutdown(t *testing.T) {
	broker := &closeRecordingBroker{closed: make(chan struct{})}
	engines := &namespaceEngines{
		brokers: map[string]centrifuge.Broker{"alpha": broker},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- engines.service().Run(ctx)
	}()

	select {
	case <-broker.closed:
		t.Fatal("broker closed before the service context is done")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("service did not return after the context is done")
	}
	select {
	case <-broker.closed:
	default:
		t.Fatal("broker not closed when the service stopped")
	}
}

func TestNamespaceEnginesBrokerNoAllocs(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("alpha")}
	cfg.PresenceManagers = configtypes.NamedPresenceManagers{memoryNamedPresenceManager("presence")}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "routed", ChannelOptions: configtypes.ChannelOptions{BrokerName: "alpha", PresenceManagerName: "presence"}},
		{Name: "plain"},
	}
	engines, cfgContainer, node := newNamespaceEnginesForTest(t, cfg)
	require.NoError(t, engines.create(node, cfgContainer))
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	for _, ch := range []string{"routed:x", "plain:x", "unknown:x", "top"} {
		allocs := testing.AllocsPerRun(1000, func() {
			_, _ = engines.broker(ch)
			_, _ = engines.presenceManager(ch)
		})
		require.Zero(t, allocs, "channel %s", ch)
	}
}

func BenchmarkNamespaceEnginesBroker(b *testing.B) {
	cfg := config.DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{memoryNamedBroker("alpha")}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "routed", ChannelOptions: configtypes.ChannelOptions{BrokerName: "alpha"}},
		{Name: "plain"},
	}
	cfgContainer, err := config.NewContainer(cfg)
	if err != nil {
		b.Fatal(err)
	}
	engines := newNamespaceEngines(cfgContainer)
	nodeCfg := centrifuge.Config{}
	engines.nodeConfig(&nodeCfg)
	node, err := centrifuge.New(nodeCfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := engines.create(node, cfgContainer); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	channels := []string{"routed:x", "plain:x"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engines.broker(channels[i%len(channels)])
	}
}
