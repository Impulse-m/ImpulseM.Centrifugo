package config

import (
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

var knownNamedBrokerTypes = []string{"memory", "redis", "nats", "postgres"}

var knownNamedPresenceManagerTypes = []string{"memory", "redis"}

var knownControllerTypes = []string{"postgres", "redis", "nats"}

// reservedEngineName is the name of the Redis controller's broker. Its metrics and logs carry this name, so
// no entry may use it.
const reservedEngineName = "controller"

// defaultRedisPrefix is the prefix which Core uses when a Redis broker or presence manager has none.
const defaultRedisPrefix = "centrifuge"

// defaultRedisAddress is the address which confighelpers uses when a Redis configuration has none.
const defaultRedisAddress = "127.0.0.1:6379"

// redisEngineRef is a Redis broker of the configuration: the default one or a named one.
type redisEngineRef struct {
	label   string
	hosts   []string
	prefix  string
	cluster bool
}

// postgresEngineRef is a Postgres stream broker of the configuration: the default one or a named one.
type postgresEngineRef struct {
	label       string
	dsn         string
	tablePrefix string
}

// namespaceRouting is the engine selection of a namespace. The top-level
// channel.without_namespace is reported with an empty name.
type namespaceRouting struct {
	name                string
	brokerName          string
	presenceManagerName string
}

// validateNamespaceEngines validates the named brokers and presence managers, the references to them from
// channel options, and the controller type.
func validateNamespaceEngines(c Config) error {
	var brokerNames []string
	var enabledBrokers []string
	for _, b := range c.Brokers {
		if !nameRe.MatchString(b.Name) {
			return fmt.Errorf("invalid broker name: %s, must match %s regular expression", b.Name, namePattern)
		}
		if b.Name == reservedEngineName {
			return fmt.Errorf("invalid broker name: %s is reserved", b.Name)
		}
		if slices.Contains(brokerNames, b.Name) {
			return fmt.Errorf("invalid broker name: %s, must be unique", b.Name)
		}
		if !slices.Contains(knownNamedBrokerTypes, b.Type) {
			return fmt.Errorf("broker %s: unknown broker type: %s", b.Name, b.Type)
		}
		brokerNames = append(brokerNames, b.Name)
		if b.Enabled {
			enabledBrokers = append(enabledBrokers, b.Name)
		}
	}

	var presenceManagerNames []string
	var enabledPresenceManagers []string
	for _, pm := range c.PresenceManagers {
		if !nameRe.MatchString(pm.Name) {
			return fmt.Errorf("invalid presence manager name: %s, must match %s regular expression", pm.Name, namePattern)
		}
		if pm.Name == reservedEngineName {
			return fmt.Errorf("invalid presence manager name: %s is reserved", pm.Name)
		}
		if slices.Contains(presenceManagerNames, pm.Name) {
			return fmt.Errorf("invalid presence manager name: %s, must be unique", pm.Name)
		}
		if !slices.Contains(knownNamedPresenceManagerTypes, pm.Type) {
			return fmt.Errorf("presence manager %s: unknown presence manager type: %s", pm.Name, pm.Type)
		}
		presenceManagerNames = append(presenceManagerNames, pm.Name)
		if pm.Enabled {
			enabledPresenceManagers = append(enabledPresenceManagers, pm.Name)
		}
	}

	if err := validateNamespaceEngineRef(
		"channel.without_namespace", c.Channel.WithoutNamespace, enabledBrokers, enabledPresenceManagers,
	); err != nil {
		return err
	}
	for _, ns := range c.Channel.Namespaces {
		if err := validateNamespaceEngineRef(
			fmt.Sprintf("namespace %q", ns.Name), ns.ChannelOptions, enabledBrokers, enabledPresenceManagers,
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

// validateNamespaceEngineRef checks that the broker and presence manager named by the channel options
// are enabled entries.
func validateNamespaceEngineRef(
	label string,
	opts configtypes.ChannelOptions,
	enabledBrokers []string,
	enabledPresenceManagers []string,
) error {
	if opts.BrokerName != "" && !slices.Contains(enabledBrokers, opts.BrokerName) {
		return fmt.Errorf("%s: broker_name %q is not an enabled entry of brokers", label, opts.BrokerName)
	}
	if opts.PresenceManagerName != "" && !slices.Contains(enabledPresenceManagers, opts.PresenceManagerName) {
		return fmt.Errorf("%s: presence_manager_name %q is not an enabled entry of presence_managers", label, opts.PresenceManagerName)
	}
	return nil
}

// validateNamedRedisPrefixes rejects two Redis brokers which use the same Redis and the same prefix. In
// cluster or sharded PUB/SUB modes such brokers subscribe the same channels, so a node would receive and
// deliver each publication twice. A cluster broker conflicts with any broker of the same prefix, since the
// cluster hosts are not resolved when the configuration is validated.
func validateNamedRedisPrefixes(c Config) error {
	var refs []redisEngineRef
	if !c.Broker.Enabled && c.Engine.Type == "redis" {
		refs = append(refs, redisEngineRef{
			label:   "engine",
			hosts:   redisHosts(c.Engine.Redis.Redis),
			prefix:  redisPrefix(c.Engine.Redis.Prefix),
			cluster: usesRedisCluster(c.Engine.Redis.Redis),
		})
	} else if c.Broker.Enabled && c.Broker.Type == "redis" {
		refs = append(refs, redisEngineRef{
			label:   "broker",
			hosts:   redisHosts(c.Broker.Redis.Redis),
			prefix:  redisPrefix(c.Broker.Redis.Prefix),
			cluster: usesRedisCluster(c.Broker.Redis.Redis),
		})
	}
	for _, b := range c.Brokers {
		if !b.Enabled || b.Type != "redis" {
			continue
		}
		refs = append(refs, redisEngineRef{
			label:   b.Name,
			hosts:   redisHosts(b.Redis.Redis),
			prefix:  redisPrefix(b.Redis.Prefix),
			cluster: usesRedisCluster(b.Redis.Redis),
		})
	}
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if refs[i].prefix != refs[j].prefix {
				continue
			}
			if refs[i].cluster || refs[j].cluster || sharesHost(refs[i].hosts, refs[j].hosts) {
				return fmt.Errorf(
					"brokers: %q and %q use the same Redis and the same prefix; set a distinct prefix",
					refs[i].label, refs[j].label,
				)
			}
		}
	}
	return nil
}

// validateNamedPostgresTables rejects two Postgres stream brokers which use the same DSN and the same table prefix.
// Each broker's outbox worker reads every row of its tables, so a node would deliver each publication once per
// broker. The DSNs are compared as written.
func validateNamedPostgresTables(c Config) error {
	var refs []postgresEngineRef
	if c.Broker.Enabled && c.Broker.Type == "postgres" {
		refs = append(refs, postgresEngineRef{
			label:       "broker",
			dsn:         c.Broker.Postgres.DSN,
			tablePrefix: postgresBrokerPrefix(c.Broker.Postgres.TablePrefix),
		})
	}
	for _, b := range c.Brokers {
		if !b.Enabled || b.Type != "postgres" {
			continue
		}
		refs = append(refs, postgresEngineRef{
			label:       b.Name,
			dsn:         b.Postgres.DSN,
			tablePrefix: postgresBrokerPrefix(b.Postgres.TablePrefix),
		})
	}
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if refs[i].dsn == refs[j].dsn && refs[i].tablePrefix == refs[j].tablePrefix {
				return fmt.Errorf(
					"brokers: %q and %q use the same PostgreSQL and the same table_prefix; set a distinct table_prefix",
					refs[i].label, refs[j].label,
				)
			}
		}
	}
	return nil
}

// usesRedisCluster reports whether a Redis configuration connects in cluster mode.
func usesRedisCluster(redisCfg configtypes.Redis) bool {
	return len(redisCfg.ClusterAddress) > 0
}

// redisHosts returns the lowercased hosts which a Redis configuration connects to. As in confighelpers, the
// cluster addresses take precedence over the sentinel addresses, which take precedence over the addresses, and
// an empty configuration connects to the default address. Each entry may list comma-separated hosts. The scheme,
// user information and path of an address URL are dropped, so "redis://h:6379" and "h:6379" give the same host.
// The database is left out: PUB/SUB is global across databases, so brokers which differ only in database still
// subscribe the same channels.
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
			host := part
			if u, err := url.Parse(part); err == nil && u.Host != "" {
				host = u.Host
			}
			if host != "" {
				hosts = append(hosts, strings.ToLower(host))
			}
		}
	}
	if len(hosts) == 0 {
		hosts = append(hosts, defaultRedisAddress)
	}
	return hosts
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

// NamespaceEnginesRecord holds the engine selection of every namespace name which the process has seen since
// startup, including the names which a reload removed. The subscriptions of a channel stay on the engine which its
// namespace selected when they were made, so a reload must keep the selection of every recorded name. A nil record
// is the record of a startup configuration without enabled entries: no namespace selects an engine then, and the
// enabled entries cannot change on reload, so no reload can select one.
type NamespaceEnginesRecord struct {
	routings map[string]namespaceRouting
}

// NewNamespaceEnginesRecord records the namespaces of the startup configuration. It returns nil when cfg has no
// enabled entry of brokers or presence_managers.
func NewNamespaceEnginesRecord(cfg Config) *NamespaceEnginesRecord {
	if len(enabledBrokerKeys(cfg)) == 0 && len(enabledPresenceManagerKeys(cfg)) == 0 {
		return nil
	}
	record := &NamespaceEnginesRecord{routings: map[string]namespaceRouting{}}
	record.Commit(cfg)
	return record
}

// Commit records the namespaces of a configuration which a reload has applied. A recorded name keeps its selection:
// CheckNamespaceEnginesReload refuses a reload which changes it, so only the names which are new to the record are added.
func (r *NamespaceEnginesRecord) Commit(cfg Config) {
	if r == nil {
		return
	}
	for name, routing := range namespaceRoutingsByName(cfg) {
		if _, ok := r.routings[name]; !ok {
			r.routings[name] = routing
		}
	}
}

// check refuses a reload which selects another engine for a recorded namespace. A namespace which the reload removes
// has the empty selection, so removing a namespace which selects an engine is refused, and a namespace which was
// removed while it selected none cannot be added back with an engine: its subscriptions were made on the default one.
func (r *NamespaceEnginesRecord) check(newCfg Config) error {
	if r == nil {
		return nil
	}
	newRoutings := namespaceRoutingsByName(newCfg)
	for _, name := range slices.Sorted(maps.Keys(r.routings)) {
		recorded := r.routings[name]
		newRouting := newRoutings[name]
		if recorded.brokerName != newRouting.brokerName {
			return namespaceEnginesChanged("broker_name of " + routingLabel(name))
		}
		if recorded.presenceManagerName != newRouting.presenceManagerName {
			return namespaceEnginesChanged("presence_manager_name of " + routingLabel(name))
		}
	}
	return nil
}

// CheckNamespaceEnginesReload refuses a reload which changes what the named brokers and presence managers route:
// the set of enabled entries with their types and Redis prefixes, the engine selection of a namespace (recorded
// since startup, see NamespaceEnginesRecord), and the controller with its type and prefix. Such a change needs a
// restart. Connection details of the entries and of the controller are applied on reload as for the default engine.
func CheckNamespaceEnginesReload(oldCfg Config, newCfg Config, record *NamespaceEnginesRecord) error {
	if !slices.Equal(enabledBrokerKeys(oldCfg), enabledBrokerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of brokers")
	}
	if !slices.Equal(enabledPresenceManagerKeys(oldCfg), enabledPresenceManagerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of presence_managers")
	}
	if err := record.check(newCfg); err != nil {
		return err
	}

	if oldCfg.Controller.Enabled != newCfg.Controller.Enabled || oldCfg.Controller.Type != newCfg.Controller.Type {
		return namespaceEnginesChanged("controller")
	}
	if newCfg.Controller.Enabled && controllerPrefix(oldCfg.Controller) != controllerPrefix(newCfg.Controller) {
		return namespaceEnginesChanged("prefix of controller")
	}
	return nil
}

// controllerPrefix returns the prefix which the controller keeps from startup: the Redis and Nats channel prefix,
// or the Postgres table prefix. An empty Redis prefix is the default one, as in Core.
func controllerPrefix(controller configtypes.Controller) string {
	switch controller.Type {
	case "redis":
		return redisPrefix(controller.Redis.Prefix)
	case "nats":
		return controller.Nats.Prefix
	case "postgres":
		return postgresControllerPrefix(controller.Postgres.TablePrefix)
	default:
		return ""
	}
}

// postgresBrokerPrefix returns the table prefix which the Postgres stream broker uses: trailing underscores are
// trimmed, and an empty prefix is cf.
func postgresBrokerPrefix(tablePrefix string) string {
	prefix := strings.TrimRight(tablePrefix, "_")
	if prefix == "" {
		return "cf"
	}
	return prefix
}

// postgresControllerPrefix returns the table prefix which the Postgres controller uses: an empty prefix is cf, and
// trailing underscores are trimmed.
func postgresControllerPrefix(tablePrefix string) string {
	if tablePrefix == "" {
		tablePrefix = "cf"
	}
	return strings.TrimRight(tablePrefix, "_")
}

func namespaceEnginesChanged(what string) error {
	return fmt.Errorf("namespace engines: %s changed, a restart is needed", what)
}

// enabledBrokerKeys identifies the enabled entries by name, type and the settings which the running broker keeps
// from startup. The connection details are left out, since a reload applies them.
func enabledBrokerKeys(cfg Config) []string {
	var keys []string
	for _, b := range cfg.Brokers {
		if b.Enabled {
			keys = append(keys, brokerKey(b))
		}
	}
	slices.Sort(keys)
	return keys
}

// brokerKey returns the key of an enabled entry of brokers. Only the settings which the type of the entry uses are
// part of the key: the Redis prefix and lists mode, the Nats prefix and raw mode, or the Postgres table prefix.
func brokerKey(b configtypes.NamedBroker) string {
	key := b.Name + "/" + b.Type
	switch b.Type {
	case "redis":
		key += fmt.Sprintf("/%s/%t", redisPrefix(b.Redis.Prefix), b.Redis.UseLists)
	case "nats":
		key += fmt.Sprintf("/%s/%t", b.Nats.Prefix, b.Nats.RawMode.Enabled)
		if b.Nats.RawMode.Enabled {
			key += fmt.Sprintf("/%s/%v", b.Nats.RawMode.Prefix, b.Nats.RawMode.ChannelReplacements)
		}
	case "postgres":
		key += "/" + postgresBrokerPrefix(b.Postgres.TablePrefix)
	}
	return key
}

// enabledPresenceManagerKeys identifies the enabled entries by name, type and, for Redis, the prefix which the
// running presence manager keeps from startup.
func enabledPresenceManagerKeys(cfg Config) []string {
	var keys []string
	for _, pm := range cfg.PresenceManagers {
		if !pm.Enabled {
			continue
		}
		key := pm.Name + "/" + pm.Type
		if pm.Type == "redis" {
			key += "/" + redisPrefix(pm.Redis.Prefix)
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// namespaceRoutings returns the engine selection of channel.without_namespace followed by the one of each namespace.
func namespaceRoutings(cfg Config) []namespaceRouting {
	routings := []namespaceRouting{{
		name:                "",
		brokerName:          cfg.Channel.WithoutNamespace.BrokerName,
		presenceManagerName: cfg.Channel.WithoutNamespace.PresenceManagerName,
	}}
	for _, ns := range cfg.Channel.Namespaces {
		routings = append(routings, namespaceRouting{
			name:                ns.Name,
			brokerName:          ns.BrokerName,
			presenceManagerName: ns.PresenceManagerName,
		})
	}
	return routings
}

// namespaceRoutingsByName returns the engine selection of each name. As in the container, the first namespace of a
// name wins.
func namespaceRoutingsByName(cfg Config) map[string]namespaceRouting {
	routings := map[string]namespaceRouting{}
	for _, routing := range namespaceRoutings(cfg) {
		if _, ok := routings[routing.name]; !ok {
			routings[routing.name] = routing
		}
	}
	return routings
}

func routingLabel(name string) string {
	if name == "" {
		return "channel.without_namespace"
	}
	return fmt.Sprintf("namespace %q", name)
}
