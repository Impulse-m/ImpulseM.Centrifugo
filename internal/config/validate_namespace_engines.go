package config

import (
	"fmt"
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
	label  string
	hosts  []string
	prefix string
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
// deliver each publication twice.
func validateNamedRedisPrefixes(c Config) error {
	var refs []redisEngineRef
	if !c.Broker.Enabled && c.Engine.Type == "redis" {
		refs = append(refs, redisEngineRef{
			label:  "engine",
			hosts:  redisHosts(c.Engine.Redis.Redis),
			prefix: redisPrefix(c.Engine.Redis.Prefix),
		})
	} else if c.Broker.Enabled && c.Broker.Type == "redis" {
		refs = append(refs, redisEngineRef{
			label:  "broker",
			hosts:  redisHosts(c.Broker.Redis.Redis),
			prefix: redisPrefix(c.Broker.Redis.Prefix),
		})
	}
	for _, b := range c.Brokers {
		if !b.Enabled || b.Type != "redis" {
			continue
		}
		refs = append(refs, redisEngineRef{
			label:  b.Name,
			hosts:  redisHosts(b.Redis.Redis),
			prefix: redisPrefix(b.Redis.Prefix),
		})
	}
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if refs[i].prefix == refs[j].prefix && sharesHost(refs[i].hosts, refs[j].hosts) {
				return fmt.Errorf(
					"brokers: %q and %q use the same Redis and the same prefix; set a distinct prefix",
					refs[i].label, refs[j].label,
				)
			}
		}
	}
	return nil
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

// CheckNamespaceEnginesReload refuses a reload which changes what the named brokers and presence managers
// route: the set of enabled entries with their types and Redis prefixes, the engine selection of a namespace,
// and the controller. Such a change needs a restart. Connection details of the entries are applied on reload as
// for the default engine.
func CheckNamespaceEnginesReload(oldCfg Config, newCfg Config) error {
	if !slices.Equal(enabledBrokerKeys(oldCfg), enabledBrokerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of brokers")
	}
	if !slices.Equal(enabledPresenceManagerKeys(oldCfg), enabledPresenceManagerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of presence_managers")
	}

	// Every namespace of the old config is compared. A namespace removed by the reload has the empty routing
	// in the new config, so removing a routed namespace is refused: its subscriptions would stay on the named
	// engine. A namespace added by the reload has no channels yet, so it may select an engine.
	newRoutings := map[string]namespaceRouting{}
	for _, r := range namespaceRoutings(newCfg) {
		newRoutings[r.name] = r
	}
	for _, oldRouting := range namespaceRoutings(oldCfg) {
		newRouting := newRoutings[oldRouting.name]
		if oldRouting.brokerName != newRouting.brokerName {
			return namespaceEnginesChanged("broker_name of " + routingLabel(oldRouting.name))
		}
		if oldRouting.presenceManagerName != newRouting.presenceManagerName {
			return namespaceEnginesChanged("presence_manager_name of " + routingLabel(oldRouting.name))
		}
	}

	if oldCfg.Controller.Enabled != newCfg.Controller.Enabled || oldCfg.Controller.Type != newCfg.Controller.Type {
		return namespaceEnginesChanged("controller")
	}
	return nil
}

func namespaceEnginesChanged(what string) error {
	return fmt.Errorf("namespace engines: %s changed, a restart is needed", what)
}

// enabledBrokerKeys identifies the enabled entries by name, type, and the Redis prefix and lists mode, which the
// running broker keeps from startup. The connection details are left out, since a reload applies them.
func enabledBrokerKeys(cfg Config) []string {
	var keys []string
	for _, b := range cfg.Brokers {
		if b.Enabled {
			keys = append(keys, fmt.Sprintf("%s/%s/%s/%t", b.Name, b.Type, redisPrefix(b.Redis.Prefix), b.Redis.UseLists))
		}
	}
	slices.Sort(keys)
	return keys
}

func enabledPresenceManagerKeys(cfg Config) []string {
	var keys []string
	for _, pm := range cfg.PresenceManagers {
		if pm.Enabled {
			keys = append(keys, pm.Name+"/"+pm.Type)
		}
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

func routingLabel(name string) string {
	if name == "" {
		return "channel.without_namespace"
	}
	return fmt.Sprintf("namespace %q", name)
}
