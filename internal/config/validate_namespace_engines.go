package config

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

var knownNamedBrokerTypes = []string{"memory", "redis", "nats", "postgres"}

var knownNamedPresenceManagerTypes = []string{"memory", "redis"}

var knownNamedMapBrokerTypes = []string{"memory", "redis", "postgres"}

var knownControllerTypes = []string{"postgres", "redis", "nats"}

// reservedEngineName is the name of the Redis controller's broker. Its metrics and logs carry this name, so
// no broker may use it.
const reservedEngineName = "controller"

// defaultRedisPrefix is the prefix which Core uses when a Redis broker or presence manager has none.
const defaultRedisPrefix = "centrifuge"

// defaultRedisAddress is the address which confighelpers uses when a Redis configuration has none.
const defaultRedisAddress = "127.0.0.1:6379"

// defaultRedisPort is the port which a Redis host gets when its address has none.
const defaultRedisPort = "6379"

// redisEngineRef is a Redis broker of the configuration: the default broker or engine, the default map broker,
// a named broker or a named map broker. isDefault marks the default ones.
type redisEngineRef struct {
	label     string
	hosts     []string
	prefix    string
	cluster   bool
	isDefault bool
}

// postgresEngineRef is a Postgres broker of the configuration: the default broker, the default map broker, a named
// broker or a named map broker. isDefault marks the default ones.
type postgresEngineRef struct {
	label       string
	dsn         string
	tablePrefix string
	isDefault   bool
}

// validateNamespaceEngines validates the named brokers, presence managers and map brokers, the references to
// them from channel options, and the controller type.
func validateNamespaceEngines(c Config) error {
	var brokerNames []string
	var enabledBrokers []string
	for _, b := range c.Brokers {
		if err := validateEntry("broker", b.Name, b.Type, brokerNames, knownNamedBrokerTypes); err != nil {
			return err
		}
		if b.Name == reservedEngineName {
			return fmt.Errorf("invalid broker name: %s is reserved", b.Name)
		}
		brokerNames = append(brokerNames, b.Name)
		if b.Enabled {
			enabledBrokers = append(enabledBrokers, b.Name)
		}
	}

	var presenceManagerNames []string
	var enabledPresenceManagers []string
	for _, pm := range c.PresenceManagers {
		if err := validateEntry(
			"presence manager", pm.Name, pm.Type, presenceManagerNames, knownNamedPresenceManagerTypes,
		); err != nil {
			return err
		}
		presenceManagerNames = append(presenceManagerNames, pm.Name)
		if pm.Enabled {
			enabledPresenceManagers = append(enabledPresenceManagers, pm.Name)
		}
	}

	var mapBrokerNames []string
	var enabledMapBrokers []string
	for _, mb := range c.MapBrokers {
		if err := validateEntry("map broker", mb.Name, mb.Type, mapBrokerNames, knownNamedMapBrokerTypes); err != nil {
			return err
		}
		mapBrokerNames = append(mapBrokerNames, mb.Name)
		if mb.Enabled {
			enabledMapBrokers = append(enabledMapBrokers, mb.Name)
		}
	}

	if err := validateNamespaceEngineRef(
		"channel.without_namespace", c.Channel.WithoutNamespace,
		enabledBrokers, enabledPresenceManagers, enabledMapBrokers,
	); err != nil {
		return err
	}
	for _, ns := range c.Channel.Namespaces {
		if err := validateNamespaceEngineRef(
			fmt.Sprintf("namespace %q", ns.Name), ns.ChannelOptions,
			enabledBrokers, enabledPresenceManagers, enabledMapBrokers,
		); err != nil {
			return err
		}
	}

	if err := validateNamedRedisPrefixes(c); err != nil {
		return err
	}
	if err := validateNamedPostgresTables(c); err != nil {
		return err
	}

	if c.Controller.Enabled && !slices.Contains(knownControllerTypes, c.Controller.Type) {
		return fmt.Errorf("unknown controller type: %s", c.Controller.Type)
	}
	return nil
}

// validateEntry checks the name and the type of a named broker, presence manager or map broker. The name must
// match the name pattern and must be unique among names. No other name may map to the same environment variables,
// since CENTRIFUGO_BROKERS_A_B_* would set both a-b and a_b. The type must be a known type. kind is the entry kind,
// which the errors name.
func validateEntry(kind string, name string, entryType string, names []string, knownTypes []string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid %s name: %s, must match %s regular expression", kind, name, namePattern)
	}
	if slices.Contains(names, name) {
		return fmt.Errorf("invalid %s name: %s, must be unique", kind, name)
	}
	for _, other := range names {
		if strings.EqualFold(configtypes.NameForEnv(other), configtypes.NameForEnv(name)) {
			return fmt.Errorf("invalid %s name: %s and %s set the same environment variables, use distinct names", kind, other, name)
		}
	}
	if !slices.Contains(knownTypes, entryType) {
		return fmt.Errorf("%s %s: unknown %s type: %s", kind, name, kind, entryType)
	}
	return nil
}

// validateNamespaceEngineRef checks that the broker, presence manager and map broker named by the channel options
// are enabled entries.
func validateNamespaceEngineRef(
	label string,
	opts configtypes.ChannelOptions,
	enabledBrokers []string,
	enabledPresenceManagers []string,
	enabledMapBrokers []string,
) error {
	if opts.BrokerName != "" && !slices.Contains(enabledBrokers, opts.BrokerName) {
		return fmt.Errorf("%s: broker_name %q is not an enabled entry of brokers", label, opts.BrokerName)
	}
	if opts.PresenceManagerName != "" && !slices.Contains(enabledPresenceManagers, opts.PresenceManagerName) {
		return fmt.Errorf("%s: presence_manager_name %q is not an enabled entry of presence_managers", label, opts.PresenceManagerName)
	}
	if opts.Map.BrokerName != "" && !slices.Contains(enabledMapBrokers, opts.Map.BrokerName) {
		return fmt.Errorf("%s: map.broker_name %q is not an enabled entry of map_brokers", label, opts.Map.BrokerName)
	}
	return nil
}

// validateNamedRedisPrefixes rejects two Redis brokers, default or named, and named map brokers which use the same
// Redis and the same prefix. The default broker and the default map broker are not compared with each other: they
// have always been accepted together. Core subscribes a channel only on the broker which routes it, so in standalone
// mode such brokers do not duplicate publications. Core turns on sharded PUB/SUB only with
// NumShardedPubSubPartitions, which the server does not set; then brokers with one prefix subscribe the same
// partition channels and deliver each publication twice. The rule keeps that mode safe. A cluster broker conflicts
// with any broker of the same prefix, since the cluster hosts are not resolved when the configuration is validated.
func validateNamedRedisPrefixes(c Config) error {
	var refs []redisEngineRef
	if !c.Broker.Enabled && c.Engine.Type == "redis" {
		refs = append(refs, newRedisEngineRef("engine", c.Engine.Redis.Redis, c.Engine.Redis.Prefix, true))
	} else if c.Broker.Enabled && c.Broker.Type == "redis" {
		refs = append(refs, newRedisEngineRef("broker", c.Broker.Redis.Redis, c.Broker.Redis.Prefix, true))
	}
	if c.MapBroker.Type == "redis" {
		refs = append(refs, newRedisEngineRef("map_broker", c.MapBroker.Redis.Redis, c.MapBroker.Redis.Prefix, true))
	}
	for _, b := range c.Brokers {
		if b.Enabled && b.Type == "redis" {
			refs = append(refs, newRedisEngineRef(fmt.Sprintf("brokers %q", b.Name), b.Redis.Redis, b.Redis.Prefix, false))
		}
	}
	for _, mb := range c.MapBrokers {
		if mb.Enabled && mb.Type == "redis" {
			refs = append(refs, newRedisEngineRef(fmt.Sprintf("map_brokers %q", mb.Name), mb.Redis.Redis, mb.Redis.Prefix, false))
		}
	}
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if refs[i].isDefault && refs[j].isDefault {
				continue
			}
			if refs[i].prefix != refs[j].prefix {
				continue
			}
			if refs[i].cluster || refs[j].cluster || sharesHost(refs[i].hosts, refs[j].hosts) {
				return fmt.Errorf(
					"%s and %s use the same Redis and the same prefix; set a distinct prefix",
					refs[i].label, refs[j].label,
				)
			}
		}
	}
	return nil
}

// newRedisEngineRef returns the reference of a Redis broker with the connection and the prefix which it uses.
func newRedisEngineRef(label string, redisCfg configtypes.Redis, prefix string, isDefault bool) redisEngineRef {
	return redisEngineRef{
		label:     label,
		hosts:     redisHosts(redisCfg),
		prefix:    redisPrefix(prefix),
		cluster:   usesRedisCluster(redisCfg),
		isDefault: isDefault,
	}
}

// validateNamedPostgresTables rejects two Postgres brokers, default or named, and named map brokers which use the
// same DSN and the same table prefix. Each broker's outbox worker reads every row of its tables, so a node would
// deliver each publication once per broker. The default broker and the default map broker are not compared with
// each other, as for Redis. The DSNs are compared as written.
func validateNamedPostgresTables(c Config) error {
	var refs []postgresEngineRef
	if c.Broker.Enabled && c.Broker.Type == "postgres" {
		refs = append(refs, newPostgresEngineRef("broker", c.Broker.Postgres.DSN, c.Broker.Postgres.TablePrefix, true))
	}
	if c.MapBroker.Type == "postgres" {
		refs = append(refs, newPostgresEngineRef("map_broker", c.MapBroker.Postgres.DSN, c.MapBroker.Postgres.TablePrefix, true))
	}
	for _, b := range c.Brokers {
		if b.Enabled && b.Type == "postgres" {
			refs = append(refs, newPostgresEngineRef(fmt.Sprintf("brokers %q", b.Name), b.Postgres.DSN, b.Postgres.TablePrefix, false))
		}
	}
	for _, mb := range c.MapBrokers {
		if mb.Enabled && mb.Type == "postgres" {
			refs = append(refs, newPostgresEngineRef(fmt.Sprintf("map_brokers %q", mb.Name), mb.Postgres.DSN, mb.Postgres.TablePrefix, false))
		}
	}
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if refs[i].isDefault && refs[j].isDefault {
				continue
			}
			if refs[i].dsn == refs[j].dsn && refs[i].tablePrefix == refs[j].tablePrefix {
				return fmt.Errorf(
					"%s and %s use the same PostgreSQL and the same table_prefix; set a distinct table_prefix",
					refs[i].label, refs[j].label,
				)
			}
		}
	}
	return nil
}

// newPostgresEngineRef returns the reference of a Postgres broker with the DSN and the table prefix which it uses.
func newPostgresEngineRef(label string, dsn string, tablePrefix string, isDefault bool) postgresEngineRef {
	return postgresEngineRef{
		label:       label,
		dsn:         dsn,
		tablePrefix: postgresBrokerPrefix(tablePrefix),
		isDefault:   isDefault,
	}
}

// usesRedisCluster reports whether a Redis configuration connects in cluster mode: it has cluster addresses, or
// one of its addresses uses the redis+cluster scheme, as in redisshard.
func usesRedisCluster(redisCfg configtypes.Redis) bool {
	if len(redisCfg.ClusterAddress) > 0 {
		return true
	}
	for _, address := range redisCfg.Address {
		if strings.HasPrefix(address, "redis+cluster://") {
			return true
		}
	}
	return false
}

// redisHosts returns the lowercased hosts which a Redis configuration connects to. As in confighelpers, the
// cluster addresses take precedence over the sentinel addresses, which take precedence over the addresses, and
// an empty configuration connects to the default address. Each entry may list comma-separated hosts, and the
// addr query parameter of an address URL adds hosts, as in redisshard. The scheme, user information and path of
// an address URL are dropped, and a host without a port gets the default Redis port, so "redis://h", "h" and
// "h:6379" give the same host. The database is left out: PUB/SUB is global across databases, so brokers which
// differ only in database still subscribe the same channels.
func redisHosts(redisCfg configtypes.Redis) []string {
	addresses := redisCfg.ClusterAddress
	if len(addresses) == 0 {
		addresses = redisCfg.SentinelAddress
	}
	if len(addresses) == 0 {
		addresses = redisCfg.Address
	}
	var hosts []string
	for _, address := range addresses {
		for _, part := range strings.Split(address, ",") {
			hosts = appendRedisHost(hosts, part)
			if u, err := url.Parse(part); err == nil {
				for _, addr := range u.Query()["addr"] {
					hosts = appendRedisHost(hosts, addr)
				}
			}
		}
	}
	if len(hosts) == 0 {
		hosts = append(hosts, defaultRedisAddress)
	}
	return hosts
}

// appendRedisHost appends the host of an address, or of an address URL, in the form redisHosts compares.
func appendRedisHost(hosts []string, address string) []string {
	host := address
	if u, err := url.Parse(address); err == nil && u.Host != "" {
		host = u.Host
	}
	if host == "" {
		return hosts
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(strings.TrimSuffix(strings.TrimPrefix(host, "["), "]"), defaultRedisPort)
	}
	return append(hosts, strings.ToLower(host))
}

// sharesHost reports whether the two lists of hosts have a host in common.
func sharesHost(hosts []string, others []string) bool {
	for _, host := range hosts {
		if slices.Contains(others, host) {
			return true
		}
	}
	return false
}

// redisPrefix returns the prefix which Core uses for the given configured prefix.
func redisPrefix(prefix string) string {
	if prefix == "" {
		return defaultRedisPrefix
	}
	return prefix
}

// postgresBrokerPrefix returns the table prefix which the Postgres brokers use: trailing underscores are
// trimmed, and an empty prefix is cf.
func postgresBrokerPrefix(tablePrefix string) string {
	prefix := strings.TrimRight(tablePrefix, "_")
	if prefix == "" {
		return "cf"
	}
	return prefix
}
