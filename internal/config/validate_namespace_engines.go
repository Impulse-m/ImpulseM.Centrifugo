package config

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

var knownNamedBrokerTypes = []string{"memory", "redis", "nats", "postgres"}

var knownNamedPresenceManagerTypes = []string{"memory", "redis"}

var knownControllerTypes = []string{"postgres", "redis", "nats"}

// redisEngineRef is a Redis broker of the configuration: the default one or a named one.
type redisEngineRef struct {
	label      string
	addressKey string
	prefix     string
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
			label:      "engine",
			addressKey: redisAddressKey(c.Engine.Redis.Redis),
			prefix:     c.Engine.Redis.Prefix,
		})
	} else if c.Broker.Enabled && c.Broker.Type == "redis" {
		refs = append(refs, redisEngineRef{
			label:      "broker",
			addressKey: redisAddressKey(c.Broker.Redis.Redis),
			prefix:     c.Broker.Redis.Prefix,
		})
	}
	for _, b := range c.Brokers {
		if !b.Enabled || b.Type != "redis" {
			continue
		}
		refs = append(refs, redisEngineRef{
			label:      b.Name,
			addressKey: redisAddressKey(b.Redis.Redis),
			prefix:     b.Redis.Prefix,
		})
	}
	for i := range refs {
		for j := i + 1; j < len(refs); j++ {
			if refs[i].addressKey == refs[j].addressKey && refs[i].prefix == refs[j].prefix {
				return fmt.Errorf(
					"brokers: %q and %q use the same Redis and the same prefix; set a distinct prefix",
					refs[i].label, refs[j].label,
				)
			}
		}
	}
	return nil
}

// redisAddressKey identifies a Redis deployment by its addresses and database.
func redisAddressKey(redisCfg configtypes.Redis) string {
	return strings.Join([]string{
		strings.Join(redisCfg.Address, ","),
		strings.Join(redisCfg.ClusterAddress, ","),
		strings.Join(redisCfg.SentinelAddress, ","),
		strconv.Itoa(redisCfg.DB),
	}, "|")
}

// CheckNamespaceEnginesReload refuses a reload which changes what the named brokers and presence managers
// route: the set of enabled entries, the engine selection of a namespace, and the controller. Such a change
// needs a restart. Connection details of the entries are applied on reload as for the default engine.
func CheckNamespaceEnginesReload(oldCfg Config, newCfg Config) error {
	if !slices.Equal(enabledBrokerKeys(oldCfg), enabledBrokerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of brokers")
	}
	if !slices.Equal(enabledPresenceManagerKeys(oldCfg), enabledPresenceManagerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of presence_managers")
	}

	oldRoutings := map[string]namespaceRouting{}
	for _, r := range namespaceRoutings(oldCfg) {
		oldRoutings[r.name] = r
	}
	for _, r := range namespaceRoutings(newCfg) {
		oldRouting, ok := oldRoutings[r.name]
		if !ok {
			continue
		}
		if oldRouting.brokerName != r.brokerName {
			return namespaceEnginesChanged("broker_name of " + routingLabel(r.name))
		}
		if oldRouting.presenceManagerName != r.presenceManagerName {
			return namespaceEnginesChanged("presence_manager_name of " + routingLabel(r.name))
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

func enabledBrokerKeys(cfg Config) []string {
	var keys []string
	for _, b := range cfg.Brokers {
		if b.Enabled {
			keys = append(keys, b.Name+"/"+b.Type)
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
