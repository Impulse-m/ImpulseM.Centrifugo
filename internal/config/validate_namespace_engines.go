package config

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/jackc/pgx/v5"
)

var knownNamedBrokerTypes = []string{"memory", "redis", "nats", "postgres"}

var knownNamedPresenceManagerTypes = []string{"memory", "redis"}

var knownNamedMapBrokerTypes = []string{"memory", "redis", "postgres"}

var knownControllerTypes = []string{"postgres", "redis", "nats"}

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
	label string
	// database identifies the database which the DSN reaches, see postgresDatabase.
	database    string
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
		// The entry embeds the standalone broker section, which has the redisnats key too. It is not a named type, so
		// the key is refused rather than ignored.
		if b.RedisNats != nil {
			return fmt.Errorf("brokers %q: redisnats is not supported on named brokers", b.Name)
		}
		if b.Name == configtypes.RedisControllerBrokerName {
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
	if err := validateNamedNatsSubjects(c); err != nil {
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
// Redis and prefixes whose channel spaces overlap, see channelSpace. The default broker and the default map broker
// are not compared with each other: they have always been accepted together. Core subscribes a channel only on the
// broker which routes it, so in standalone mode such brokers do not duplicate publications. Core turns on sharded
// PUB/SUB only with NumShardedPubSubPartitions, which the server does not set; then brokers with one prefix subscribe
// the same partition channels and deliver each publication twice. The rule keeps that mode safe. A cluster broker
// conflicts with any broker of the same prefix, since the cluster hosts are not resolved when the configuration is
// validated.
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
	first, second, found := firstEngineConflict(
		refs,
		func(ref redisEngineRef) bool {
			return ref.isDefault
		},
		func(a redisEngineRef, b redisEngineRef) bool {
			return subjectSpacesOverlap(a.channelSpace(), b.channelSpace()) && (a.cluster || b.cluster || sharesHost(a.hosts, b.hosts))
		},
	)
	if found {
		return fmt.Errorf(
			"%s and %s use the same Redis and overlapping prefixes; set a distinct prefix",
			first.label, second.label,
		)
	}
	return nil
}

// firstEngineConflict returns the first pair of references which conflicts, in list order. The pair of two default
// references is never a conflict, since the defaults are accepted together. found is false when no pair conflicts.
func firstEngineConflict[T any](
	refs []T,
	isDefault func(ref T) bool,
	conflicts func(a T, b T) bool,
) (first T, second T, found bool) {
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if isDefault(refs[i]) && isDefault(refs[j]) {
				continue
			}
			if conflicts(refs[i], refs[j]) {
				return refs[i], refs[j], true
			}
		}
	}
	return first, second, false
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

// channelSpace returns the space of the Redis channels which the broker publishes and subscribes: its prefix
// followed by .client., as in Core. Two prefixes whose spaces overlap, such as "a" and "a.client", can name the
// same channel.
func (ref redisEngineRef) channelSpace() string {
	return ref.prefix + ".client."
}

// natsEngineRef is a NATS broker of the configuration: the default broker or a named broker. space is the subject
// space which its channels are published and subscribed under, see newNatsEngineRef, and wildcards marks a broker
// which subscribes wildcard subjects. isDefault marks the default broker.
type natsEngineRef struct {
	label     string
	space     string
	wildcards bool
	isDefault bool
}

// validateNamedNatsSubjects rejects two NATS brokers, default or named, whose subject spaces overlap when one of them
// allows wildcard subscriptions. A wildcard subscription of a broker receives the publications of the other broker
// too, since they match the same subjects, so a node would deliver each publication twice.
func validateNamedNatsSubjects(c Config) error {
	var refs []natsEngineRef
	if c.Broker.Enabled && c.Broker.Type == "nats" {
		refs = append(refs, newNatsEngineRef("broker", c.Broker.Nats, true))
	}
	for _, b := range c.Brokers {
		if b.Enabled && b.Type == "nats" {
			refs = append(refs, newNatsEngineRef(fmt.Sprintf("brokers %q", b.Name), b.Nats, false))
		}
	}
	first, second, found := firstEngineConflict(
		refs,
		func(ref natsEngineRef) bool {
			return ref.isDefault
		},
		func(a natsEngineRef, b natsEngineRef) bool {
			return subjectSpacesOverlap(a.space, b.space) && (a.wildcards || b.wildcards)
		},
	)
	if found {
		return fmt.Errorf(
			"%s and %s use overlapping NATS subjects and one of them allows wildcards; set a distinct prefix",
			first.label, second.label,
		)
	}
	return nil
}

// newNatsEngineRef returns the reference of a NATS broker with the subject space which its channels use. A broker
// in raw mode uses the raw mode prefix as its space, since its subjects are the prefix followed by the channel.
// Any other broker uses its prefix followed by .client., as in natsbroker.
func newNatsEngineRef(label string, natsCfg configtypes.NatsBroker, isDefault bool) natsEngineRef {
	space := natsCfg.Prefix + ".client."
	if natsCfg.RawMode.Enabled {
		space = natsCfg.RawMode.Prefix
	}
	return natsEngineRef{
		label:     label,
		space:     space,
		wildcards: natsCfg.AllowWildcards,
		isDefault: isDefault,
	}
}

// subjectSpacesOverlap reports whether two subject spaces can match the same subject: they are equal, or one
// continues the other at a token boundary. "a.client." and "a.client.x" overlap, since a channel may add "x";
// "a.client." and "a.clientx" do not.
func subjectSpacesOverlap(a string, b string) bool {
	return subjectSpaceWithin(a, b) || subjectSpaceWithin(b, a)
}

// subjectSpaceWithin reports whether space is outer or lies inside it at a token boundary. A space which ends with
// a dot is a boundary by itself.
func subjectSpaceWithin(space string, outer string) bool {
	if !strings.HasPrefix(space, outer) {
		return false
	}
	return len(space) == len(outer) || space[len(outer)] == '.' || strings.HasSuffix(outer, ".")
}

// validateNamedPostgresTables rejects two Postgres brokers, default or named, and named map brokers which use the
// same DSN and the same table prefix. Each broker's outbox worker reads every row of its tables, so a node would
// deliver each publication once per broker. The default broker and the default map broker are not compared with
// each other, as for Redis. Two DSNs reach the same database when their hosts, ports and database names are equal.
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
	first, second, found := firstEngineConflict(
		refs,
		func(ref postgresEngineRef) bool {
			return ref.isDefault
		},
		func(a postgresEngineRef, b postgresEngineRef) bool {
			return a.database == b.database && a.tablePrefix == b.tablePrefix
		},
	)
	if found {
		return fmt.Errorf(
			"%s and %s use the same PostgreSQL and the same table_prefix; set a distinct table_prefix",
			first.label, second.label,
		)
	}
	return nil
}

// newPostgresEngineRef returns the reference of a Postgres broker with the DSN and the table prefix which it uses.
func newPostgresEngineRef(label string, dsn string, tablePrefix string, isDefault bool) postgresEngineRef {
	return postgresEngineRef{
		label:       label,
		database:    postgresDatabase(dsn),
		tablePrefix: postgresBrokerPrefix(tablePrefix),
		isDefault:   isDefault,
	}
}

// postgresDatabase returns the identity of the database which a DSN reaches: its host and port, the fallback hosts
// and the database name. The user and the other parameters do not change which rows the outbox reads, so they are
// left out. A DSN which pgx cannot parse is compared as written, so it matches only the same text.
func postgresDatabase(dsn string) string {
	conf, err := pgx.ParseConfig(dsn)
	if err != nil {
		return dsn
	}
	hosts := []string{hostPortKey(conf.Host, conf.Port)}
	for _, fallback := range conf.Fallbacks {
		hosts = append(hosts, hostPortKey(fallback.Host, fallback.Port))
	}
	// pgx adds a fallback to the same host for sslmode=prefer, so the hosts are sorted and deduplicated.
	slices.Sort(hosts)
	hosts = slices.Compact(hosts)
	// Host names are case-insensitive, database names are not.
	return strings.Join(hosts, ",") + "/" + conf.Database
}

// hostPortKey returns a host and a port in the form postgresDatabase compares. Host names are lowercased.
func hostPortKey(host string, port uint16) string {
	return net.JoinHostPort(strings.ToLower(host), strconv.Itoa(int(port)))
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
