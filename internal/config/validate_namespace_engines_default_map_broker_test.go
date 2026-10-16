package config

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// TestValidateNamespaceEnginesDefaultMapBroker checks the duplicate-engine guards for the default map_broker: it is
// compared with the named brokers and map brokers, but not with the default broker.
func TestValidateNamespaceEnginesDefaultMapBroker(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(cfg *Config)
		wantErr string
	}{
		{
			name: "default redis map broker and named redis broker with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.MapBroker.Type = "redis"
				cfg.MapBroker.Redis.Prefix = "centrifugo-broadcast"
				cfg.MapBroker.Redis.Redis.Address = []string{"redis://10.0.0.2:6379"}
			},
			wantErr: `map_broker and brokers "broadcast" use the same Redis and the same prefix`,
		},
		{
			name: "default redis map broker and named redis broker with the same address and another prefix",
			modify: func(cfg *Config) {
				cfg.MapBroker.Type = "redis"
				cfg.MapBroker.Redis.Prefix = "centrifugo-maps"
				cfg.MapBroker.Redis.Redis.Address = []string{"redis://10.0.0.2:6379"}
			},
			wantErr: "",
		},
		{
			name: "default redis map broker and named redis map broker with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.MapBroker.Type = "redis"
				cfg.MapBroker.Redis.Prefix = "centrifugo-cursors"
				cfg.MapBroker.Redis.Redis.Address = []string{"redis://10.0.0.4:6379"}
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
			},
			wantErr: `map_broker and map_brokers "cursors" use the same Redis and the same prefix`,
		},
		{
			name: "default redis map broker and default redis broker with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.Broker.Enabled = true
				cfg.Broker.Type = "redis"
				cfg.Broker.Redis.Prefix = "defaults"
				cfg.Broker.Redis.Redis.Address = []string{"redis://10.0.0.9:6379"}
				cfg.MapBroker.Type = "redis"
				cfg.MapBroker.Redis.Prefix = "defaults"
				cfg.MapBroker.Redis.Redis.Address = []string{"redis://10.0.0.9:6379"}
			},
			wantErr: "",
		},
		{
			name: "default postgres map broker and named postgres broker with the same DSN and table prefix",
			modify: func(cfg *Config) {
				cfg.MapBroker.Type = "postgres"
				cfg.MapBroker.Postgres.DSN = "postgres://db/main"
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
			},
			wantErr: `map_broker and brokers "orders" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "default postgres map broker and named postgres broker with distinct table prefixes",
			modify: func(cfg *Config) {
				cfg.MapBroker.Type = "postgres"
				cfg.MapBroker.Postgres.DSN = "postgres://db/main"
				cfg.MapBroker.Postgres.TablePrefix = "maps"
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
			},
			wantErr: "",
		},
		{
			name: "default postgres map broker and named postgres map broker with the same DSN and table prefix",
			modify: func(cfg *Config) {
				cfg.MapBroker.Type = "postgres"
				cfg.MapBroker.Postgres.DSN = "postgres://db/app"
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedPostgresMapBroker("cursors", "postgres://db/app", "cf_")}
			},
			wantErr: `map_broker and map_brokers "cursors" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "default postgres map broker and default postgres broker with the same DSN and table prefix",
			modify: func(cfg *Config) {
				cfg.Broker.Enabled = true
				cfg.Broker.Type = "postgres"
				cfg.Broker.Postgres.DSN = "postgres://db/main"
				cfg.MapBroker.Type = "postgres"
				cfg.MapBroker.Postgres.DSN = "postgres://db/main"
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

// TestCheckNamespaceEnginesReloadMapBrokerDefaultPrefix checks that an empty Redis prefix of a map broker and the
// default prefix name the same running map broker, so a reload which only writes the default is not refused.
func TestCheckNamespaceEnginesReloadMapBrokerDefaultPrefix(t *testing.T) {
	oldCfg := namespaceEnginesConfig()
	oldCfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "")}
	newCfg := namespaceEnginesConfig()
	newCfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifuge")}
	require.NoError(t, CheckNamespaceEnginesReload(oldCfg, newCfg, nil))
}
