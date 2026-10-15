package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

func namedRedisBroker(name string, address string, prefix string) configtypes.NamedBroker {
	var b configtypes.NamedBroker
	b.Name = name
	b.Enabled = true
	b.Type = "redis"
	b.Redis.Prefix = prefix
	b.Redis.Redis.Address = []string{address}
	return b
}

func namedNatsBroker(name string, url string) configtypes.NamedBroker {
	var b configtypes.NamedBroker
	b.Name = name
	b.Enabled = true
	b.Type = "nats"
	b.Nats.URL = url
	return b
}

func namedPostgresBroker(name string) configtypes.NamedBroker {
	var b configtypes.NamedBroker
	b.Name = name
	b.Enabled = true
	b.Type = "postgres"
	return b
}

func namedRedisPresenceManager(name string, address string) configtypes.NamedPresenceManager {
	var pm configtypes.NamedPresenceManager
	pm.Name = name
	pm.Enabled = true
	pm.Type = "redis"
	pm.Redis.Redis.Address = []string{address}
	return pm
}

// namespaceEnginesConfig is the configuration of the example in the README: a Redis broker, a Nats broker
// and a Postgres broker, a Redis presence manager, and namespaces which select them.
func namespaceEnginesConfig() Config {
	cfg := DefaultConfig()
	cfg.Brokers = configtypes.NamedBrokers{
		namedRedisBroker("broadcast", "redis://10.0.0.2:6379", "centrifugo-broadcast"),
		namedNatsBroker("rates", "nats://localhost:4222"),
		namedPostgresBroker("orders"),
	}
	cfg.PresenceManagers = configtypes.NamedPresenceManagers{
		namedRedisPresenceManager("presence", "redis://10.0.0.3:6379"),
	}
	cfg.Channel.Namespaces = configtypes.ChannelNamespaces{
		{Name: "broadcast", ChannelOptions: configtypes.ChannelOptions{BrokerName: "broadcast", PresenceManagerName: "presence"}},
		{Name: "rates", ChannelOptions: configtypes.ChannelOptions{BrokerName: "rates"}},
	}
	return cfg
}

func TestValidateNamespaceEngines(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(cfg *Config)
		wantErr string
	}{
		{
			name:    "valid configuration",
			modify:  func(cfg *Config) {},
			wantErr: "",
		},
		{
			name: "invalid broker name",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Name = "a"
			},
			wantErr: "invalid broker name: a",
		},
		{
			name: "duplicate broker name",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Name = "broadcast"
			},
			wantErr: "invalid broker name: broadcast, must be unique",
		},
		{
			name: "unknown broker type redisnats",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Type = "redisnats"
			},
			wantErr: "unknown broker type: redisnats",
		},
		{
			name: "disabled broker with unknown type is still checked",
			modify: func(cfg *Config) {
				cfg.Brokers[2].Enabled = false
				cfg.Brokers[2].Type = "kafka"
			},
			wantErr: "unknown broker type: kafka",
		},
		{
			name: "invalid presence manager name",
			modify: func(cfg *Config) {
				cfg.PresenceManagers[0].Name = "p"
			},
			wantErr: "invalid presence manager name: p",
		},
		{
			name: "presence manager of type nats",
			modify: func(cfg *Config) {
				cfg.PresenceManagers[0].Type = "nats"
			},
			wantErr: "unknown presence manager type: nats",
		},
		{
			name: "broker_name references a disabled entry",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Enabled = false
			},
			wantErr: `namespace "rates": broker_name "rates" is not an enabled entry of brokers`,
		},
		{
			name: "broker_name references an unknown entry",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces[1].BrokerName = "unknown"
			},
			wantErr: `namespace "rates": broker_name "unknown" is not an enabled entry of brokers`,
		},
		{
			name: "presence_manager_name references an unknown entry",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces[0].PresenceManagerName = "unknown"
			},
			wantErr: `namespace "broadcast": presence_manager_name "unknown" is not an enabled entry of presence_managers`,
		},
		{
			name: "broker_name of channel.without_namespace references an unknown entry",
			modify: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.BrokerName = "unknown"
			},
			wantErr: `channel.without_namespace: broker_name "unknown" is not an enabled entry of brokers`,
		},
		{
			name: "broker_name of channel.without_namespace references an enabled entry",
			modify: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.BrokerName = "rates"
			},
			wantErr: "",
		},
		{
			name: "engine redis and named redis broker with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Redis.Address = []string{"redis://10.0.0.2:6379"}
				cfg.Engine.Redis.Prefix = "centrifugo-broadcast"
			},
			wantErr: `brokers: "engine" and "broadcast" use the same Redis and the same prefix`,
		},
		{
			name: "engine redis and named redis broker with the same address and another prefix",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Redis.Address = []string{"redis://10.0.0.2:6379"}
				cfg.Engine.Redis.Prefix = "centrifugo-engine"
			},
			wantErr: "",
		},
		{
			name: "engine redis is not checked when the broker section is enabled",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Redis.Address = []string{"redis://10.0.0.2:6379"}
				cfg.Engine.Redis.Prefix = "centrifugo-broadcast"
				cfg.Broker.Enabled = true
				cfg.Broker.Type = "memory"
			},
			wantErr: "",
		},
		{
			name: "two named redis brokers with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers: "broadcast" and "rates" use the same Redis and the same prefix`,
		},
		{
			name: "two named redis brokers with the same prefix on another redis",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.9:6379", "centrifugo-broadcast")
			},
			wantErr: "",
		},
		{
			name: "two named redis brokers with the same address but a different cluster address",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.ClusterAddress = []string{"redis://10.0.0.5:6379"}
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcast")
			},
			wantErr: "",
		},
		{
			name: "controller of type redis",
			modify: func(cfg *Config) {
				cfg.Controller.Enabled = true
				cfg.Controller.Type = "redis"
			},
			wantErr: "",
		},
		{
			name: "controller of type nats",
			modify: func(cfg *Config) {
				cfg.Controller.Enabled = true
				cfg.Controller.Type = "nats"
			},
			wantErr: "",
		},
		{
			name: "controller of unknown type",
			modify: func(cfg *Config) {
				cfg.Controller.Enabled = true
				cfg.Controller.Type = "foo"
			},
			wantErr: "unknown controller type: foo",
		},
		{
			name: "controller of unknown type while disabled",
			modify: func(cfg *Config) {
				cfg.Controller.Enabled = false
				cfg.Controller.Type = "foo"
			},
			wantErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := namespaceEnginesConfig()
			tt.modify(&cfg)
			err := validateNamespaceEngines(cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestCheckNamespaceEnginesReload(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(cfg *Config)
		wantErr string
	}{
		{
			name:    "identical configuration",
			modify:  func(cfg *Config) {},
			wantErr: "",
		},
		{
			name: "new namespace routed to an existing entry",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, configtypes.ChannelNamespace{
					Name:           "news",
					ChannelOptions: configtypes.ChannelOptions{BrokerName: "orders"},
				})
			},
			wantErr: "",
		},
		{
			name: "removed namespace",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces = cfg.Channel.Namespaces[:1]
			},
			wantErr: "",
		},
		{
			name: "broker_name of a namespace changed",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces[1].BrokerName = "orders"
			},
			wantErr: `namespace engines: broker_name of namespace "rates" changed, a restart is needed`,
		},
		{
			name: "broker_name of a namespace removed",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces[1].BrokerName = ""
			},
			wantErr: `namespace engines: broker_name of namespace "rates" changed`,
		},
		{
			name: "presence_manager_name of a namespace changed",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces[0].PresenceManagerName = ""
			},
			wantErr: `namespace engines: presence_manager_name of namespace "broadcast" changed`,
		},
		{
			name: "broker_name of channel.without_namespace changed",
			modify: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.BrokerName = "rates"
			},
			wantErr: "namespace engines: broker_name of channel.without_namespace changed",
		},
		{
			name: "renamed entry",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Name = "fares"
			},
			wantErr: "namespace engines: enabled entries of brokers changed",
		},
		{
			name: "changed entry type",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-rates")
			},
			wantErr: "namespace engines: enabled entries of brokers changed",
		},
		{
			name: "entry disabled",
			modify: func(cfg *Config) {
				cfg.Brokers[2].Enabled = false
			},
			wantErr: "namespace engines: enabled entries of brokers changed",
		},
		{
			name: "changed presence manager entry type",
			modify: func(cfg *Config) {
				cfg.PresenceManagers[0].Type = "memory"
			},
			wantErr: "namespace engines: enabled entries of presence_managers changed",
		},
		{
			name: "changed address of an entry",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.Address = []string{"redis://10.0.0.7:6379"}
				cfg.PresenceManagers[0].Redis.Redis.Address = []string{"redis://10.0.0.8:6379"}
			},
			wantErr: "",
		},
		{
			name: "controller type changed",
			modify: func(cfg *Config) {
				cfg.Controller.Enabled = true
				cfg.Controller.Type = "redis"
			},
			wantErr: "namespace engines: controller changed, a restart is needed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldCfg := namespaceEnginesConfig()
			newCfg := namespaceEnginesConfig()
			tt.modify(&newCfg)
			err := CheckNamespaceEnginesReload(oldCfg, newCfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
