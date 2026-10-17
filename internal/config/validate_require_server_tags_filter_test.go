package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func requiredNamespace(name string) configtypes.ChannelNamespace {
	return configtypes.ChannelNamespace{
		Name: name,
		ChannelOptions: configtypes.ChannelOptions{
			RequireServerTagsFilter: true,
		},
	}
}

func TestRequireServerTagsFilterValidates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{requiredNamespace("req")}
	require.NoError(t, cfg.Validate())
	container, err := NewContainer(cfg)
	require.NoError(t, err)
	require.True(t, container.ConfigRef().RequiresServerTagsFilter())

	chOpts, ok, err := container.ChannelOptionsRef("req:room")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, chOpts.RequireServerTagsFilter)

	// A channel of a namespace which is not configured has no options, so the option is off for it.
	other, ok, err := container.ChannelOptionsRef("plain:room")
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, other.RequireServerTagsFilter)
}

func TestRequireServerTagsFilterWithoutNamespaceValidates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.WithoutNamespace.RequireServerTagsFilter = true
	require.NoError(t, cfg.Validate())
	container, err := NewContainer(cfg)
	require.NoError(t, err)
	require.True(t, container.ConfigRef().RequiresServerTagsFilter())
	chOpts, _, err := container.ChannelOptionsRef("room")
	require.NoError(t, err)
	require.True(t, chOpts.RequireServerTagsFilter)
}

func TestRequireServerTagsFilterOffByDefault(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate())
	container, err := NewContainer(cfg)
	require.NoError(t, err)
	require.False(t, container.ConfigRef().RequiresServerTagsFilter())
}

func TestValidateRequireServerTagsFilterRules(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(cfg *Config)
		wantErr string
	}{
		{
			name: "shared poll without namespace",
			prepare: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.RequireServerTagsFilter = true
				cfg.Channel.WithoutNamespace.SubscriptionType = "shared_poll"
			},
			wantErr: "in channel.without_namespace: require_server_tags_filter is not supported with subscription_type shared_poll",
		},
		{
			name: "shared poll in namespace",
			prepare: func(cfg *Config) {
				ns := requiredNamespace("feed")
				ns.SubscriptionType = "shared_poll"
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{ns}
			},
			wantErr: "namespace feed: require_server_tags_filter is not supported with subscription_type shared_poll",
		},
		{
			name: "subscribe stream proxy",
			prepare: func(cfg *Config) {
				ns := requiredNamespace("stream")
				ns.SubscribeStreamProxyEnabled = true
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{ns}
			},
			wantErr: "namespace stream: require_server_tags_filter cannot be used with subscribe_stream_proxy_enabled",
		},
		{
			name: "personal channel namespace without template",
			prepare: func(cfg *Config) {
				cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
				cfg.Client.SubscribeToUserPersonalChannel.PersonalChannelNamespace = "req"
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{requiredNamespace("req")}
			},
			wantErr: "namespace req: require_server_tags_filter cannot be used on the personal channel namespace",
		},
		{
			name: "personal channel without namespace",
			prepare: func(cfg *Config) {
				cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
				cfg.Channel.WithoutNamespace.RequireServerTagsFilter = true
			},
			wantErr: "in channel.without_namespace: require_server_tags_filter cannot be used on the personal channel namespace",
		},
		{
			name: "insecure token verification",
			prepare: func(cfg *Config) {
				cfg.Client.InsecureSkipTokenSignatureVerify = true
				cfg.Channel.Namespaces = configtypes.ChannelNamespaces{requiredNamespace("req")}
			},
			wantErr: "namespace req: require_server_tags_filter cannot be used with client.insecure_skip_token_signature_verify",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.prepare(&cfg)
			require.ErrorContains(t, validateRequireServerTagsFilter(cfg), tt.wantErr)
			require.ErrorContains(t, cfg.Validate(), tt.wantErr)
		})
	}
}

func TestValidateRequireServerTagsFilterAllowed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	cfg.Client.SubscribeToUserPersonalChannel.PersonalChannelNamespace = "tenant"
	personal := requiredNamespace("tenant")
	personal.ServerTagsFilterTemplate = sessionTemplate()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{personal, requiredNamespace("req")}
	require.NoError(t, validateRequireServerTagsFilter(cfg))
	require.NoError(t, cfg.Validate())
}

func TestValidateRequireServerTagsFilterRulesOnlyWhenSet(t *testing.T) {
	// A config which does not set the option is not refused by the rules of the option.
	cfg := DefaultConfig()
	cfg.Client.InsecureSkipTokenSignatureVerify = true
	cfg.Client.SubscribeToUserPersonalChannel.Enabled = true
	cfg.Client.SubscribeToUserPersonalChannel.PersonalChannelNamespace = ""
	cfg.Channel.WithoutNamespace.SubscriptionType = "shared_poll"
	require.NoError(t, validateRequireServerTagsFilter(cfg))
}

func TestRequireServerTagsFilterEnvVars(t *testing.T) {
	t.Setenv("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_REQUIRE_SERVER_TAGS_FILTER", "true")
	conf, meta := getConfig(t, "testdata/config.json")
	require.True(t, conf.Channel.WithoutNamespace.RequireServerTagsFilter)
	require.Len(t, meta.UnknownEnvs, 0)
}

func TestRequireServerTagsFilterNamespacesEnvJSON(t *testing.T) {
	t.Setenv("CENTRIFUGO_CHANNEL_NAMESPACES", `[{"name":"req","require_server_tags_filter":true}]`)
	conf, meta := getConfig(t, "testdata/config.json")
	require.Len(t, conf.Channel.Namespaces, 1)
	require.True(t, conf.Channel.Namespaces[0].RequireServerTagsFilter)
	require.Len(t, meta.UnknownEnvs, 0)
}
