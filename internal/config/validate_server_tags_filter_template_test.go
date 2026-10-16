package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func sessionTemplate() configtypes.ServerTagsFilterTemplate {
	return configtypes.ServerTagsFilterTemplate{
		Filter: configtypes.TagsFilterNode{
			"key": "session",
			"cmp": "neq",
			"val": "{{claims.sid}}",
		},
	}
}

func tenantTemplate() configtypes.ServerTagsFilterTemplate {
	return configtypes.ServerTagsFilterTemplate{
		Filter: configtypes.TagsFilterNode{
			"op": "and",
			"nodes": []any{
				map[string]any{"key": "tenant", "cmp": "eq", "val": "{{claims.org.tenant}}"},
				map[string]any{"key": "audience", "cmp": "in", "vals": []any{"all", "user:{{user}}"}},
			},
		},
		OnMissingValue: "reject",
	}
}

func namespaceWithTemplate(name string, template configtypes.ServerTagsFilterTemplate) configtypes.ChannelNamespace {
	return configtypes.ChannelNamespace{
		Name: name,
		ChannelOptions: configtypes.ChannelOptions{
			ServerTagsFilterTemplate: template,
		},
	}
}

func TestServerTagsFilterTemplateDefaults(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate())
	container, err := NewContainer(cfg)
	require.NoError(t, err)
	prepared := container.ConfigRef()
	require.False(t, prepared.HasServerTagsFilterTemplates())
	require.Empty(t, prepared.ServerTagsFilterClaims())
	chOpts, ok, err := container.ChannelOptionsRef("room")
	require.NoError(t, err)
	require.True(t, ok)
	require.Nil(t, chOpts.Compiled.ServerTagsFilterTemplate)
}

func TestServerTagsFilterTemplateCompiled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.WithoutNamespace.ServerTagsFilterTemplate = configtypes.ServerTagsFilterTemplate{
		Filter: configtypes.TagsFilterNode{"key": "owner", "cmp": "eq", "val": "{{user}}"},
	}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		namespaceWithTemplate("chat", sessionTemplate()),
		namespaceWithTemplate("tenant", tenantTemplate()),
		{Name: "plain"},
	}
	require.NoError(t, cfg.Validate())
	container, err := NewContainer(cfg)
	require.NoError(t, err)

	prepared := container.ConfigRef()
	require.True(t, prepared.HasServerTagsFilterTemplates())
	require.Equal(t, []string{"org.tenant", "sid"}, prepared.ServerTagsFilterClaims())

	chat, ok, err := container.ChannelOptionsRef("chat:room")
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, chat.Compiled.ServerTagsFilterTemplate)

	tenant, ok, err := container.ChannelOptionsRef("tenant:room")
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, tenant.Compiled.ServerTagsFilterTemplate)

	plain, ok, err := container.ChannelOptionsRef("plain:room")
	require.NoError(t, err)
	require.True(t, ok)
	require.Nil(t, plain.Compiled.ServerTagsFilterTemplate)

	withoutNamespace, ok, err := container.ChannelOptionsRef("room")
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, withoutNamespace.Compiled.ServerTagsFilterTemplate)
}

func TestServerTagsFilterTemplateDoesNotChangeCallerConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{namespaceWithTemplate("chat", sessionTemplate())}
	_, err := NewContainer(cfg)
	require.NoError(t, err)
	// The caller's config is not changed by the preparation.
	require.Nil(t, cfg.Channel.Namespaces[0].Compiled.ServerTagsFilterTemplate)
	require.False(t, cfg.HasServerTagsFilterTemplates())
}

func TestValidateServerTagsFilterTemplate(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		template  configtypes.ServerTagsFilterTemplate
		wantErr   string
	}{
		{
			name:      "unknown filter field names the namespace",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{"key": "k", "cmpp": "eq", "val": "x"},
			},
			wantErr: `namespace "chat"`,
		},
		{
			name:      "unknown variable",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{"key": "k", "cmp": "eq", "val": "{{session}}"},
			},
			wantErr: `unknown variable "session"`,
		},
		{
			name:      "variable in key",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{"key": "{{user}}", "cmp": "eq", "val": "x"},
			},
			wantErr: `namespace "chat"`,
		},
		{
			name:      "on missing value without filter",
			namespace: "chat",
			template:  configtypes.ServerTagsFilterTemplate{OnMissingValue: "skip"},
			wantErr:   `namespace "chat": on_missing_value requires filter`,
		},
		{
			name:      "unknown on missing value",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter:         configtypes.TagsFilterNode{"key": "k", "cmp": "eq", "val": "x"},
				OnMissingValue: "ignore",
			},
			wantErr: `unknown on_missing_value "ignore"`,
		},
		{
			name:      "without namespace",
			namespace: "",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{"key": "k", "cmp": "eq", "val": "{{claims.}}"},
			},
			wantErr: `without_namespace`,
		},
		{
			name:      "unknown cmp names the namespace",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{"key": "k", "cmp": "equals", "val": "{{user}}"},
			},
			wantErr: `namespace "chat": filter:`,
		},
		{
			name:      "in without vals",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{"key": "k", "cmp": "in", "val": "{{user}}"},
			},
			wantErr: `namespace "chat": filter:`,
		},
		{
			name:      "not with two children",
			namespace: "chat",
			template: configtypes.ServerTagsFilterTemplate{
				Filter: configtypes.TagsFilterNode{
					"op": "not",
					"nodes": []any{
						map[string]any{"key": "a", "cmp": "eq", "val": "x"},
						map[string]any{"key": "b", "cmp": "eq", "val": "{{user}}"},
					},
				},
			},
			wantErr: `namespace "chat": filter:`,
		},
		{
			name:      "valid",
			namespace: "chat",
			template:  sessionTemplate(),
		},
		{
			name:      "valid nested tree",
			namespace: "tenant",
			template:  tenantTemplate(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServerTagsFilterTemplate(tt.namespace, configtypes.ChannelOptions{ServerTagsFilterTemplate: tt.template})
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestValidateServerTagsFilterTemplateRefusesPresence(t *testing.T) {
	tests := []struct {
		name    string
		options configtypes.ChannelOptions
	}{
		{name: "presence", options: configtypes.ChannelOptions{Presence: true}},
		{name: "join_leave", options: configtypes.ChannelOptions{JoinLeave: true}},
		{name: "force_push_join_leave", options: configtypes.ChannelOptions{ForcePushJoinLeave: true}},
		{name: "map clients presence prefix", options: configtypes.ChannelOptions{MapClientsPresenceChannelPrefix: "clients:"}},
		{name: "map users presence prefix", options: configtypes.ChannelOptions{MapUsersPresenceChannelPrefix: "users:"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.options.ServerTagsFilterTemplate = sessionTemplate()
			require.ErrorContains(t, validateServerTagsFilterTemplate("chat", tt.options), `namespace "chat": presence, join_leave`)
		})
	}
	// Without a template the options are not refused here.
	require.NoError(t, validateServerTagsFilterTemplate("chat", configtypes.ChannelOptions{Presence: true, JoinLeave: true}))
}

func TestValidateServerTagsFilterTemplateRefusesSharedPoll(t *testing.T) {
	options := configtypes.ChannelOptions{SubscriptionType: "shared_poll", ServerTagsFilterTemplate: sessionTemplate()}
	require.ErrorContains(t, validateServerTagsFilterTemplate("feed", options), `namespace "feed": server_tags_filter_template is not supported with subscription_type shared_poll`)
	// Without a template the subscription type is not refused here.
	require.NoError(t, validateServerTagsFilterTemplate("feed", configtypes.ChannelOptions{SubscriptionType: "shared_poll"}))
	for _, subscriptionType := range []string{"", "stream", "map", "map_clients", "map_users"} {
		options.SubscriptionType = subscriptionType
		require.NoError(t, validateServerTagsFilterTemplate("feed", options), subscriptionType)
	}
}

func TestConfigValidateRefusesSharedPollOfTemplatedNamespace(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SharedPoll.HMACSecretKey = "secret"
	cfg.Channel.Proxy.SharedPollRefresh.Endpoint = "http://127.0.0.1/shared_poll"
	ns := namespaceWithTemplate("feed", sessionTemplate())
	ns.SubscriptionType = "shared_poll"
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{ns}
	err := cfg.Validate()
	require.ErrorContains(t, err, `namespace "feed": server_tags_filter_template is not supported with subscription_type shared_poll`)
}

func TestConfigValidateRefusesPresenceOfTemplatedNamespace(t *testing.T) {
	cfg := DefaultConfig()
	ns := namespaceWithTemplate("tenant", sessionTemplate())
	ns.Presence = true
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{ns}
	err := cfg.Validate()
	require.ErrorContains(t, err, `namespace "tenant": presence`)
	require.NotContains(t, err.Error(), "in channel.without_namespace")
}

func TestConfigValidateNamesNamespaceOfTemplateError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		namespaceWithTemplate("tenant", configtypes.ServerTagsFilterTemplate{
			Filter: configtypes.TagsFilterNode{"key": "k", "cmp": "eq", "val": "{{claims.}}"},
		}),
	}
	err := cfg.Validate()
	require.ErrorContains(t, err, `namespace "tenant"`)
	_, err = NewContainer(cfg)
	require.ErrorContains(t, err, `namespace "tenant"`)
}

func TestServerTagsFilterTemplateFromEnvJSON(t *testing.T) {
	var namespaces configtypes.ChannelNamespaces
	err := namespaces.Decode(`[{"name": "chat", "server_tags_filter_template": {"filter": {"key": "session", "cmp": "neq", "val": "{{claims.sid}}"}, "on_missing_value": "skip"}}]`)
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	template := namespaces[0].ServerTagsFilterTemplate
	require.Equal(t, "skip", template.OnMissingValue)
	require.Equal(t, "{{claims.sid}}", template.Filter["val"])
}

func TestConfigValidateNamesWithoutNamespaceOfTemplateErrorOnce(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Channel.WithoutNamespace.ServerTagsFilterTemplate = configtypes.ServerTagsFilterTemplate{
		Filter: configtypes.TagsFilterNode{"key": "k", "cmp": "eq", "val": "{{claims.}}"},
	}
	err := cfg.Validate()
	require.ErrorContains(t, err, "channel.without_namespace: filter")
	require.NotContains(t, err.Error(), "in channel.without_namespace")
}
