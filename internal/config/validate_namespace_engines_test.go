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

func namedRedisMapBroker(name string, address string, prefix string) configtypes.NamedMapBroker {
	var mb configtypes.NamedMapBroker
	mb.Name = name
	mb.Enabled = true
	mb.Type = "redis"
	mb.Redis.Prefix = prefix
	mb.Redis.Redis.Address = []string{address}
	return mb
}

func namedPostgresMapBroker(name string, dsn string, tablePrefix string) configtypes.NamedMapBroker {
	var mb configtypes.NamedMapBroker
	mb.Name = name
	mb.Enabled = true
	mb.Type = "postgres"
	mb.Postgres.DSN = dsn
	mb.Postgres.TablePrefix = tablePrefix
	return mb
}

// namespaceWithMapBroker returns a namespace which selects the named map broker for its map channels.
func namespaceWithMapBroker(name string, mapBrokerName string) configtypes.ChannelNamespace {
	var ns configtypes.ChannelNamespace
	ns.Name = name
	ns.Map.BrokerName = mapBrokerName
	return ns
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
			name: "redisnats section on a named broker is refused",
			modify: func(cfg *Config) {
				cfg.Brokers[0].RedisNats = &configtypes.RedisNatsBroker{}
			},
			wantErr: `brokers "broadcast": redisnats is not supported on named brokers`,
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
			name: "broker named like the controller",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Name = "controller"
			},
			wantErr: "invalid broker name: controller is reserved",
		},
		{
			name: "presence manager named like the controller is allowed, the name is reserved for brokers only",
			modify: func(cfg *Config) {
				cfg.PresenceManagers[0].Name = "controller"
				cfg.Channel.Namespaces[0].PresenceManagerName = "controller"
			},
			wantErr: "",
		},
		{
			name: "names which set the same environment variables",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Name = "broad_cast"
				cfg.Brokers[1].Name = "broad-cast"
			},
			wantErr: `invalid broker name: broad_cast and broad-cast set the same environment variables`,
		},
		{
			name: "names which differ only in case set the same environment variables",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Name = "BROADCAST"
			},
			wantErr: `invalid broker name: broadcast and BROADCAST set the same environment variables`,
		},
		{
			name: "same name for a broker and a presence manager",
			modify: func(cfg *Config) {
				cfg.PresenceManagers[0].Name = "broadcast"
				cfg.Channel.Namespaces[0].PresenceManagerName = "broadcast"
			},
			wantErr: "",
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
			wantErr: `engine and brokers "broadcast" use the same Redis and overlapping prefixes`,
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
			name: "two named redis brokers on one host whose prefixes overlap at a token boundary",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Prefix = "centrifugo-broadcast"
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcast.client")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers on one host whose prefixes only start alike",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcasts")
			},
			wantErr: "",
		},
		{
			name: "two named redis brokers with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers with the same prefix and another database",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.DB = 1
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers with the same prefix and the same host in another form",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "rediss://user:secret@10.0.0.2:6379/3", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers, one host without a port and the same host with the default port",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.Address = []string{"redis://10.0.0.2"}
				cfg.Brokers[1] = namedRedisBroker("rates", "10.0.0.2:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers, one with an addr host which the other one uses",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.Address = []string{"redis://10.0.0.2:6379?addr=10.0.0.4:6379"}
				cfg.Brokers[1] = namedRedisBroker("rates", "10.0.0.4:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "a redis+cluster address and a standalone redis broker with the same prefix on other hosts",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.Address = []string{"redis+cluster://10.0.0.2:6379"}
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.9:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "engine redis without the scheme and a named redis broker with it",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Redis.Address = []string{"10.0.0.2:6379"}
				cfg.Engine.Redis.Prefix = "centrifugo-broadcast"
			},
			wantErr: `engine and brokers "broadcast" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers with the same prefix on another redis",
			modify: func(cfg *Config) {
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.9:6379", "centrifugo-broadcast")
			},
			wantErr: "",
		},
		{
			name: "two named redis brokers with an empty prefix and the default prefix",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedRedisBroker("broadcast", "redis://10.0.0.2:6379", "")
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifuge")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "engine redis with an empty prefix and a named redis broker with the default prefix",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Redis.Address = []string{"redis://10.0.0.2:6379"}
				cfg.Engine.Redis.Prefix = ""
				cfg.Brokers[0] = namedRedisBroker("broadcast", "redis://10.0.0.2:6379", "centrifuge")
			},
			wantErr: `engine and brokers "broadcast" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers with the default address and the same prefix",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedRedisBroker("broadcast", "", "centrifugo-broadcast")
				cfg.Brokers[1] = namedRedisBroker("rates", "127.0.0.1:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named redis brokers with comma-separated cluster hosts in another order",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedRedisBroker("broadcast", "", "centrifugo-broadcast")
				cfg.Brokers[0].Redis.Redis.ClusterAddress = []string{"10.0.0.2:6379,10.0.0.3:6379"}
				cfg.Brokers[1] = namedRedisBroker("rates", "", "centrifugo-broadcast")
				cfg.Brokers[1].Redis.Redis.ClusterAddress = []string{"10.0.0.3:6379", "10.0.0.2:6379"}
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "engine redis cluster address and a named redis address on the same host",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Redis.ClusterAddress = []string{"10.0.0.2:6379"}
				cfg.Engine.Redis.Prefix = "centrifugo-broadcast"
			},
			wantErr: `engine and brokers "broadcast" use the same Redis and overlapping prefixes`,
		},
		{
			name: "a cluster redis broker and a standalone redis broker with the same prefix on other hosts",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Redis.ClusterAddress = []string{"redis://10.0.0.5:6379"}
				cfg.Brokers[1] = namedRedisBroker("rates", "redis://10.0.0.2:6379", "centrifugo-broadcast")
			},
			wantErr: `brokers "broadcast" and brokers "rates" use the same Redis and overlapping prefixes`,
		},
		{
			name: "engine redis cluster and a named redis cluster with the same prefix and hosts written differently",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Prefix = "shared"
				cfg.Engine.Redis.ClusterAddress = []string{"localhost:7000"}
				cfg.Brokers[0].Redis.Address = nil
				cfg.Brokers[0].Redis.ClusterAddress = []string{"127.0.0.1:7000"}
				cfg.Brokers[0].Redis.Prefix = "shared"
			},
			wantErr: `engine and brokers "broadcast" use the same Redis and overlapping prefixes`,
		},
		{
			name: "engine redis cluster and a named redis standalone with the same prefix on other hosts",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Prefix = "shared"
				cfg.Engine.Redis.ClusterAddress = []string{"10.0.0.5:7000"}
				cfg.Brokers[0].Redis.Prefix = "shared"
			},
			wantErr: `engine and brokers "broadcast" use the same Redis and overlapping prefixes`,
		},
		{
			name: "engine redis cluster and a named redis standalone with another prefix",
			modify: func(cfg *Config) {
				cfg.Engine.Type = "redis"
				cfg.Engine.Redis.Prefix = "shared"
				cfg.Engine.Redis.ClusterAddress = []string{"10.0.0.5:7000"}
			},
			wantErr: "",
		},
		{
			name: "two named postgres brokers with the same DSN and table prefix",
			modify: func(cfg *Config) {
				cfg.Brokers = append(cfg.Brokers, namedPostgresBroker("ledger"))
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
				cfg.Brokers[3].Postgres.DSN = "postgres://db/main"
			},
			wantErr: `brokers "orders" and brokers "ledger" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "two named postgres brokers with the same DSN and table prefixes written as cf and cf_",
			modify: func(cfg *Config) {
				cfg.Brokers = append(cfg.Brokers, namedPostgresBroker("ledger"))
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
				cfg.Brokers[2].Postgres.TablePrefix = "cf"
				cfg.Brokers[3].Postgres.DSN = "postgres://db/main"
				cfg.Brokers[3].Postgres.TablePrefix = "cf_"
			},
			wantErr: `brokers "orders" and brokers "ledger" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "two named postgres brokers whose DSNs reach the same database with other spelling",
			modify: func(cfg *Config) {
				cfg.Brokers = append(cfg.Brokers, namedPostgresBroker("ledger"))
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
				cfg.Brokers[3].Postgres.DSN = "postgresql://Db:5432/main?sslmode=disable"
			},
			wantErr: `brokers "orders" and brokers "ledger" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "two named postgres brokers whose DSNs differ only in the database name case",
			modify: func(cfg *Config) {
				cfg.Brokers = append(cfg.Brokers, namedPostgresBroker("ledger"))
				cfg.Brokers[2].Postgres.DSN = "postgres://db/Main"
				cfg.Brokers[3].Postgres.DSN = "postgres://db/main"
			},
			wantErr: "",
		},
		{
			name: "two named postgres brokers with the same database and other users",
			modify: func(cfg *Config) {
				cfg.Brokers = append(cfg.Brokers, namedPostgresBroker("ledger"))
				cfg.Brokers[2].Postgres.DSN = "postgres://alpha@db/main"
				cfg.Brokers[3].Postgres.DSN = "postgres://beta@db/main"
			},
			wantErr: `brokers "orders" and brokers "ledger" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "two named postgres brokers with the same DSN and another table prefix",
			modify: func(cfg *Config) {
				cfg.Brokers = append(cfg.Brokers, namedPostgresBroker("ledger"))
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
				cfg.Brokers[3].Postgres.DSN = "postgres://db/main"
				cfg.Brokers[3].Postgres.TablePrefix = "ledger"
			},
			wantErr: "",
		},
		{
			name: "default postgres broker and a named postgres broker with the same DSN",
			modify: func(cfg *Config) {
				cfg.Broker.Enabled = true
				cfg.Broker.Type = "postgres"
				cfg.Broker.Postgres.DSN = "postgres://db/main"
				cfg.Brokers[2].Postgres.DSN = "postgres://db/main"
			},
			wantErr: `broker and brokers "orders" use the same PostgreSQL and the same table_prefix`,
		},
		{
			name: "valid map broker reference of a namespace",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, namespaceWithMapBroker("cursors", "cursors"))
			},
			wantErr: "",
		},
		{
			name: "map.broker_name of channel.without_namespace references an unknown entry",
			modify: func(cfg *Config) {
				cfg.Channel.WithoutNamespace.Map.BrokerName = "cursors"
			},
			wantErr: `channel.without_namespace: map.broker_name "cursors" is not an enabled entry of map_brokers`,
		},
		{
			name: "map.broker_name references a disabled map broker",
			modify: func(cfg *Config) {
				mb := namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")
				mb.Enabled = false
				cfg.MapBrokers = configtypes.NamedMapBrokers{mb}
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, namespaceWithMapBroker("cursors", "cursors"))
			},
			wantErr: `namespace "cursors": map.broker_name "cursors" is not an enabled entry of map_brokers`,
		},
		{
			name: "map broker of unknown type",
			modify: func(cfg *Config) {
				mb := namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")
				mb.Type = "nats"
				cfg.MapBrokers = configtypes.NamedMapBrokers{mb}
			},
			wantErr: "map broker cursors: unknown map broker type: nats",
		},
		{
			name: "map broker name which sets the same environment variables as another map broker",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{
					namedRedisMapBroker("cur_sors", "redis://10.0.0.4:6379", "centrifugo-cursors"),
					namedRedisMapBroker("cur-sors", "redis://10.0.0.5:6379", "centrifugo-scores"),
				}
			},
			wantErr: `invalid map broker name: cur_sors and cur-sors set the same environment variables`,
		},
		{
			name: "map broker named like a broker",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("broadcast", "redis://10.0.0.4:6379", "centrifugo-cursors")}
			},
			wantErr: "",
		},
		{
			name: "two named redis map brokers with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{
					namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors"),
					namedRedisMapBroker("scores", "redis://10.0.0.4:6379", "centrifugo-cursors"),
				}
			},
			wantErr: `map_brokers "cursors" and map_brokers "scores" use the same Redis and overlapping prefixes`,
		},
		{
			name: "named redis map broker and named redis broker with the same address and prefix",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{
					namedRedisMapBroker("cursors", "redis://10.0.0.2:6379", "centrifugo-broadcast"),
				}
			},
			wantErr: `brokers "broadcast" and map_brokers "cursors" use the same Redis and overlapping prefixes`,
		},
		{
			name: "two named postgres map brokers with the same DSN and table prefix",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{
					namedPostgresMapBroker("cursors", "postgres://db/app", "cf"),
					namedPostgresMapBroker("scores", "postgres://db/app", "cf_"),
				}
			},
			wantErr: `map_brokers "cursors" and map_brokers "scores" use the same PostgreSQL and the same table_prefix`,
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
			name: "named nats broker in raw mode under the channel space of a non-raw broker with wildcards",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "centrifugo"
				cfg.Brokers[0].Nats.AllowWildcards = true
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "centrifugo.client."
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode whose root is the prefix of a non-raw channel space with wildcards",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "a"
				cfg.Brokers[0].Nats.AllowWildcards = true
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "a"
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode with a root inside a non-raw channel space with wildcards",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "a"
				cfg.Brokers[0].Nats.AllowWildcards = true
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "a.client"
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "two non-raw nats brokers whose prefixes nest at a token boundary with wildcards",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "a"
				cfg.Brokers[0].Nats.AllowWildcards = true
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.Prefix = "a.client"
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode with a root which only starts like a non-raw channel space",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "a"
				cfg.Brokers[0].Nats.AllowWildcards = true
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "a.clientx"
			},
			wantErr: "",
		},
		{
			name: "two named nats brokers with the same prefix and no wildcards",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "shared"
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.Prefix = "shared"
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode with an empty root and a non-raw broker",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "foobar"
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = ""
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode whose root is a string prefix of a non-raw prefix",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "foobar"
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "foo"
			},
			wantErr: `brokers "broadcast" and brokers "rates" use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode under the subjects of a nats controller",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "centrifugo."
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: `brokers "rates" and controller use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode under the node subjects of a nats controller",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "centrifugo.node."
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: `brokers "rates" and controller use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode with a root inside the control subject of a nats controller",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "centrifugo.con"
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: `brokers "rates" and controller use overlapping NATS subjects`,
		},
		{
			name: "named nats broker in raw mode with a root which only starts like the control subject",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "centrifugo.controlx"
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: "",
		},
		{
			name: "named nats broker in raw mode under the channel space of a nats controller prefix",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "centrifugo.client."
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: "",
		},
		{
			name: "named nats broker with the prefix of a nats controller",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.Prefix = "centrifugo"
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: "",
		},
		{
			name: "default nats broker with the prefix of a nats controller",
			modify: func(cfg *Config) {
				cfg.Broker.Enabled = true
				cfg.Broker.Type = "nats"
				cfg.Broker.Nats.Prefix = "centrifugo"
				cfg.Brokers[1].Nats.Prefix = "rates"
				cfg.Controller = natsControllerConfig("centrifugo")
			},
			wantErr: "",
		},
		{
			name: "two named nats brokers with wildcards and distinct prefixes",
			modify: func(cfg *Config) {
				cfg.Brokers[0] = namedNatsBroker("broadcast", "nats://localhost:4222")
				cfg.Brokers[0].Nats.Prefix = "one"
				cfg.Brokers[0].Nats.AllowWildcards = true
				cfg.Brokers[1] = namedNatsBroker("rates", "nats://localhost:4222")
				cfg.Brokers[1].Nats.Prefix = "two"
			},
			wantErr: "",
		},
		{
			name: "default nats broker and a named nats broker with the same prefix",
			modify: func(cfg *Config) {
				cfg.Broker.Enabled = true
				cfg.Broker.Type = "nats"
				cfg.Broker.Nats.Prefix = "shared"
				cfg.Brokers[1].Nats.Prefix = "shared"
			},
			wantErr: `broker and brokers "rates" use overlapping NATS subjects`,
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

func redisControllerConfig(prefix string) configtypes.Controller {
	var c configtypes.Controller
	c.Enabled = true
	c.Type = "redis"
	c.Redis.Prefix = prefix
	c.Redis.Redis.Address = []string{"redis://10.0.0.8:6379"}
	return c
}

func natsControllerConfig(prefix string) configtypes.Controller {
	var c configtypes.Controller
	c.Enabled = true
	c.Type = "nats"
	c.Nats.Prefix = prefix
	return c
}

func postgresControllerConfig(tablePrefix string) configtypes.Controller {
	var c configtypes.Controller
	c.Enabled = true
	c.Type = "postgres"
	c.Postgres.TablePrefix = tablePrefix
	return c
}

func TestCheckNamespaceEnginesReload(t *testing.T) {
	tests := []struct {
		name      string
		oldModify func(cfg *Config)
		modify    func(cfg *Config)
		wantErr   string
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
			name: "removed namespace which selects a named broker",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces = cfg.Channel.Namespaces[:1]
			},
			wantErr: `namespace engines: namespace "rates" selecting broker_name "rates" was removed, a restart is needed`,
		},
		{
			name: "removed namespace which selects a broker and a presence manager",
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces = cfg.Channel.Namespaces[1:]
			},
			wantErr: `namespace engines: namespace "broadcast" selecting broker_name "broadcast" and presence_manager_name "presence" was removed, a restart is needed`,
		},
		{
			name: "map.broker_name of a namespace changed",
			oldModify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, namespaceWithMapBroker("cursors", "cursors"))
			},
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, namespaceWithMapBroker("cursors", ""))
			},
			wantErr: `namespace engines: map.broker_name of namespace "cursors" changed, a restart is needed`,
		},
		{
			name: "removed namespace which selects a map broker",
			oldModify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, namespaceWithMapBroker("cursors", "cursors"))
			},
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
				cfg.Channel.Namespaces = cfg.Channel.Namespaces[:2]
			},
			wantErr: `namespace engines: namespace "cursors" selecting map.broker_name "cursors" was removed, a restart is needed`,
		},
		{
			name: "map broker added to the enabled entries",
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "centrifugo-cursors")}
			},
			wantErr: "namespace engines: enabled entries of map_brokers changed, a restart is needed",
		},
		{
			name: "prefix of a named map broker changed",
			oldModify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "a")}
			},
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "b")}
			},
			wantErr: "namespace engines: enabled entries of map_brokers changed, a restart is needed",
		},
		{
			name: "address of a named map broker changed",
			oldModify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.4:6379", "a")}
			},
			modify: func(cfg *Config) {
				cfg.MapBrokers = configtypes.NamedMapBrokers{namedRedisMapBroker("cursors", "redis://10.0.0.6:6379", "a")}
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
			name: "prefix of an entry changed",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.Prefix = "centrifugo-other"
			},
			wantErr: "namespace engines: enabled entries of brokers changed, a restart is needed",
		},
		{
			name: "use_lists of an entry changed",
			modify: func(cfg *Config) {
				cfg.Brokers[0].Redis.UseLists = true
			},
			wantErr: "namespace engines: enabled entries of brokers changed, a restart is needed",
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
		{
			name: "redis controller prefix changed",
			oldModify: func(cfg *Config) {
				cfg.Controller = redisControllerConfig("a")
			},
			modify: func(cfg *Config) {
				cfg.Controller = redisControllerConfig("b")
			},
			wantErr: "namespace engines: prefix of controller changed, a restart is needed",
		},
		{
			name: "redis controller prefix set to the default one",
			oldModify: func(cfg *Config) {
				cfg.Controller = redisControllerConfig("centrifuge")
			},
			modify: func(cfg *Config) {
				cfg.Controller = redisControllerConfig("")
			},
			wantErr: "",
		},
		{
			name: "redis controller address changed",
			oldModify: func(cfg *Config) {
				cfg.Controller = redisControllerConfig("a")
			},
			modify: func(cfg *Config) {
				cfg.Controller = redisControllerConfig("a")
				cfg.Controller.Redis.Redis.Address = []string{"redis://10.0.0.9:6379"}
			},
			wantErr: "",
		},
		{
			name: "nats controller prefix changed",
			oldModify: func(cfg *Config) {
				cfg.Controller = natsControllerConfig("a")
			},
			modify: func(cfg *Config) {
				cfg.Controller = natsControllerConfig("b")
			},
			wantErr: "namespace engines: prefix of controller changed, a restart is needed",
		},
		{
			name: "postgres controller table prefix changed",
			oldModify: func(cfg *Config) {
				cfg.Controller = postgresControllerConfig("cf")
			},
			modify: func(cfg *Config) {
				cfg.Controller = postgresControllerConfig("other")
			},
			wantErr: "namespace engines: prefix of controller changed, a restart is needed",
		},
		{
			name: "namespace boundary changed",
			modify: func(cfg *Config) {
				cfg.Channel.NamespaceBoundary = "/"
			},
			wantErr: "namespace engines: channel.namespace_boundary changed, a restart is needed",
		},
		{
			name: "private prefix changed",
			modify: func(cfg *Config) {
				cfg.Channel.PrivatePrefix = "!"
			},
			wantErr: "namespace engines: channel.private_prefix changed, a restart is needed",
		},
		{
			name: "prefix of a named Nats broker changed",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.Prefix = "centrifugo-v2"
			},
			wantErr: "namespace engines: enabled entries of brokers changed, a restart is needed",
		},
		{
			name: "raw mode prefix of a named Nats broker changed",
			oldModify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "raw-a"
			},
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.Prefix = "raw-b"
			},
			wantErr: "namespace engines: enabled entries of brokers changed, a restart is needed",
		},
		{
			name: "raw mode channel replacements of a named Nats broker changed",
			oldModify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.ChannelReplacements = map[string]string{":": "."}
			},
			modify: func(cfg *Config) {
				cfg.Brokers[1].Nats.RawMode.Enabled = true
				cfg.Brokers[1].Nats.RawMode.ChannelReplacements = map[string]string{":": "/"}
			},
			wantErr: "namespace engines: enabled entries of brokers changed, a restart is needed",
		},
		{
			name: "Redis prefix of a named Nats broker is not used",
			modify: func(cfg *Config) {
				cfg.Brokers[1].Redis.Prefix = "unused"
			},
			wantErr: "",
		},
		{
			name: "table prefix of a named Postgres broker changed",
			modify: func(cfg *Config) {
				cfg.Brokers[2].Postgres.TablePrefix = "orders"
			},
			wantErr: "namespace engines: enabled entries of brokers changed, a restart is needed",
		},
		{
			name: "table prefix of a named Postgres broker written with a trailing underscore",
			modify: func(cfg *Config) {
				cfg.Brokers[2].Postgres.TablePrefix = "cf_"
			},
			wantErr: "",
		},
		{
			name: "postgres controller table prefix written with a trailing underscore",
			oldModify: func(cfg *Config) {
				cfg.Controller = postgresControllerConfig("cf")
			},
			modify: func(cfg *Config) {
				cfg.Controller = postgresControllerConfig("cf_")
			},
			wantErr: "",
		},
		{
			name: "prefix of a Redis presence manager changed",
			modify: func(cfg *Config) {
				cfg.PresenceManagers[0].Redis.Prefix = "centrifugo-other"
			},
			wantErr: "namespace engines: enabled entries of presence_managers changed, a restart is needed",
		},
		{
			name: "removed namespace which selects no engine",
			oldModify: func(cfg *Config) {
				cfg.Channel.Namespaces = append(cfg.Channel.Namespaces, configtypes.ChannelNamespace{Name: "news"})
			},
			modify: func(cfg *Config) {
				cfg.Channel.Namespaces = cfg.Channel.Namespaces[:2]
			},
			wantErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldCfg := namespaceEnginesConfig()
			if tt.oldModify != nil {
				tt.oldModify(&oldCfg)
			}
			newCfg := namespaceEnginesConfig()
			tt.modify(&newCfg)
			err := CheckNamespaceEnginesReload(oldCfg, newCfg, NewNamespaceEnginesRecord(oldCfg))
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestNamespaceEnginesRecordKeepsRemovedNamespace(t *testing.T) {
	require.Nil(t, NewNamespaceEnginesRecord(DefaultConfig()))

	startCfg := namespaceEnginesConfig()
	startCfg.Channel.Namespaces = append(startCfg.Channel.Namespaces, configtypes.ChannelNamespace{Name: "news"})
	record := NewNamespaceEnginesRecord(startCfg)
	require.NotNil(t, record)

	// The reload removes news, which selects no engine. Its subscriptions stay on the default broker.
	removedCfg := namespaceEnginesConfig()
	require.NoError(t, CheckNamespaceEnginesReload(startCfg, removedCfg, record))
	record.Commit(removedCfg)

	// A later reload may not route the removed name to a named broker: its subscriptions would not be released.
	routedCfg := namespaceEnginesConfig()
	routedCfg.Channel.Namespaces = append(routedCfg.Channel.Namespaces, configtypes.ChannelNamespace{
		Name:           "news",
		ChannelOptions: configtypes.ChannelOptions{BrokerName: "rates"},
	})
	require.ErrorContains(t, CheckNamespaceEnginesReload(removedCfg, routedCfg, record),
		`namespace engines: broker_name of namespace "news" changed, a restart is needed`)

	// Adding it back without an engine, and adding a new name which selects an engine, are both allowed.
	unroutedCfg := namespaceEnginesConfig()
	unroutedCfg.Channel.Namespaces = append(unroutedCfg.Channel.Namespaces,
		configtypes.ChannelNamespace{Name: "news"},
		configtypes.ChannelNamespace{Name: "fresh", ChannelOptions: configtypes.ChannelOptions{BrokerName: "rates"}},
	)
	require.NoError(t, CheckNamespaceEnginesReload(removedCfg, unroutedCfg, record))
}

func TestCheckNamespaceEnginesReloadWithoutEnginesAllowsBoundaryChange(t *testing.T) {
	oldCfg := DefaultConfig()
	newCfg := DefaultConfig()
	newCfg.Channel.NamespaceBoundary = "/"
	newCfg.Channel.PrivatePrefix = "!"
	require.NoError(t, CheckNamespaceEnginesReload(oldCfg, newCfg, NewNamespaceEnginesRecord(oldCfg)))
}
