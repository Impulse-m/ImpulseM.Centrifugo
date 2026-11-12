package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// serverTagsNamespace returns a namespace with the options given, which allows server tags.
func serverTagsNamespace(name string, opts configtypes.ChannelOptions) configtypes.ChannelNamespace {
	opts.AllowServerTags = true
	return configtypes.ChannelNamespace{Name: name, ChannelOptions: opts}
}

// TestValidateServerTagsAllowedOnMemoryAndRedis checks a namespace which allows server tags validates on the memory
// broker and on the redis engine.
func TestValidateServerTagsAllowedOnMemoryAndRedis(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Engine.Type = "memory"
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{serverTagsNamespace("st", configtypes.ChannelOptions{})}
	require.NoError(t, cfg.Validate())

	cfg.Engine.Type = "redis"
	require.NoError(t, validateServerTags(cfg))
}

// TestValidateServerTagsWithoutNamespaceAllowed checks the option works on channel.without_namespace too.
func TestValidateServerTagsWithoutNamespaceAllowed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.WithoutNamespace.AllowServerTags = true
	require.NoError(t, validateServerTags(cfg))
}

// TestValidateServerTagsRefusesOtherBrokers checks allow_server_tags is refused on NATS, the postgres stream broker and
// the experimental Redis and NATS broker, which do not carry the server tags of a publication.
func TestValidateServerTagsRefusesOtherBrokers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		brokerType string
	}{
		{"nats", "nats"},
		{"postgres", "postgres"},
		{"redisnats", "redisnats"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Broker = configtypes.Broker{Enabled: true, Type: tc.brokerType}
			cfg.Channel.Namespaces = configtypes.ChannelNamespaces{serverTagsNamespace("st", configtypes.ChannelOptions{})}
			require.ErrorContains(t, validateServerTags(cfg),
				"namespace st: allow_server_tags requires the memory or redis broker, the broker of the namespace is "+tc.brokerType)
		})
	}
}

// TestValidateServerTagsUsesEngineWhenBrokerDisabled checks the broker of a namespace is the engine when the standalone
// broker is not enabled, so a disabled broker type does not refuse the option.
func TestValidateServerTagsUsesEngineWhenBrokerDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Engine.Type = "redis"
	cfg.Broker = configtypes.Broker{Enabled: false, Type: "nats"}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{serverTagsNamespace("st", configtypes.ChannelOptions{})}
	require.NoError(t, validateServerTags(cfg))
}

// TestValidateServerTagsNamedBroker checks the broker of a namespace which selects a named broker is the type of that
// broker, not of the default one.
func TestValidateServerTagsNamedBroker(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{
		{Name: "fast", Broker: configtypes.Broker{Enabled: true, Type: "nats"}},
	}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		serverTagsNamespace("st", configtypes.ChannelOptions{BrokerName: "fast"}),
	}
	require.ErrorContains(t, validateServerTags(cfg),
		"namespace st: allow_server_tags requires the memory or redis broker, the broker of the namespace is nats")

	cfg.Brokers[0].Type = "redis"
	require.NoError(t, validateServerTags(cfg))
}

// TestValidateServerTagsRefusesSharedPollAndStreamProxy checks allow_server_tags is refused with shared poll and with
// the subscribe stream proxy, whose subscriptions do not apply a server tags filter.
func TestValidateServerTagsRefusesSharedPollAndStreamProxy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		serverTagsNamespace("poll", configtypes.ChannelOptions{SubscriptionType: "shared_poll"}),
	}
	require.ErrorContains(t, validateServerTags(cfg),
		"namespace poll: allow_server_tags is not supported with subscription_type shared_poll")

	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		serverTagsNamespace("proxy", configtypes.ChannelOptions{SubscribeStreamProxyEnabled: true}),
	}
	require.ErrorContains(t, validateServerTags(cfg),
		"namespace proxy: allow_server_tags cannot be used with subscribe_stream_proxy_enabled")
}

// TestValidateServerTagsRefusesNonStreamSubscriptionTypes checks allow_server_tags is refused on every subscription type
// other than stream, and is allowed with the default and the stream type.
func TestValidateServerTagsRefusesNonStreamSubscriptionTypes(t *testing.T) {
	for _, subscriptionType := range []string{"map", "map_clients", "map_users", "shared_poll"} {
		t.Run(subscriptionType, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
				serverTagsNamespace("other", configtypes.ChannelOptions{SubscriptionType: subscriptionType}),
			}
			require.ErrorContains(t, validateServerTags(cfg),
				"namespace other: allow_server_tags is not supported with subscription_type "+subscriptionType)
		})
	}

	for _, subscriptionType := range []string{"", "stream"} {
		t.Run("allowed "+subscriptionType, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
				serverTagsNamespace("ok", configtypes.ChannelOptions{SubscriptionType: subscriptionType}),
			}
			require.NoError(t, validateServerTags(cfg))
		})
	}
}

// TestValidateServerTagsNegativeCapsRefused checks a negative cap is refused in a namespace and in channel.without_namespace,
// whether or not the namespace allows server tags.
func TestValidateServerTagsNegativeCapsRefused(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "neg", ChannelOptions: configtypes.ChannelOptions{PublishTagsMaxCount: -1}},
	}
	require.ErrorContains(t, validateServerTags(cfg), "namespace neg: publish_tags_max_count must not be negative")

	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "neg", ChannelOptions: configtypes.ChannelOptions{PublishTagsMaxBytes: -1}},
	}
	require.ErrorContains(t, validateServerTags(cfg), "namespace neg: publish_tags_max_bytes must not be negative")

	cfg.Channel.Namespaces = nil
	cfg.Channel.WithoutNamespace.PublishTagsMaxBytes = -1
	require.ErrorContains(t, validateServerTags(cfg),
		"in channel.without_namespace: publish_tags_max_bytes must not be negative")
}

// TestValidateServerTagsPositiveCapsAllowed checks the recommended caps validate with server tags.
func TestValidateServerTagsPositiveCapsAllowed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		serverTagsNamespace("capped", configtypes.ChannelOptions{PublishTagsMaxCount: 256, PublishTagsMaxBytes: 16384}),
	}
	require.NoError(t, validateServerTags(cfg))
}

// TestValidateServerTagsOffByDefault checks a namespace which does not allow server tags is not checked by the rules of
// the option, so an existing config with shared poll or another broker stays valid.
func TestValidateServerTagsOffByDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Broker = configtypes.Broker{Enabled: true, Type: "nats"}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "poll", ChannelOptions: configtypes.ChannelOptions{SubscriptionType: "shared_poll"}},
	}
	require.NoError(t, validateServerTags(cfg))
}
