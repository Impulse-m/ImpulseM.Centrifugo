package config

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// namespaceRouting is the engine selection of a namespace. The top-level
// channel.without_namespace is reported with an empty name.
type namespaceRouting struct {
	name                string
	brokerName          string
	presenceManagerName string
	mapBrokerName       string
}

// selects reports whether the namespace selects a named engine.
func (r namespaceRouting) selects() bool {
	return r.brokerName != "" || r.presenceManagerName != "" || r.mapBrokerName != ""
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
// enabled entry of brokers, presence_managers or map_brokers.
func NewNamespaceEnginesRecord(cfg Config) *NamespaceEnginesRecord {
	if !hasEnabledEngines(cfg) {
		return nil
	}
	record := &NamespaceEnginesRecord{routings: map[string]namespaceRouting{}}
	record.Commit(cfg)
	return record
}

// hasEnabledEngines reports whether cfg has an enabled entry of brokers, presence_managers or map_brokers.
func hasEnabledEngines(cfg Config) bool {
	return len(enabledBrokerKeys(cfg)) > 0 || len(enabledPresenceManagerKeys(cfg)) > 0 || len(enabledMapBrokerKeys(cfg)) > 0
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
		newRouting, present := newRoutings[name]
		if !present {
			if recorded.selects() {
				return namespaceEnginesRemoved(name, recorded)
			}
			continue
		}
		if recorded.brokerName != newRouting.brokerName {
			return namespaceEnginesChanged("broker_name of " + routingLabel(name))
		}
		if recorded.presenceManagerName != newRouting.presenceManagerName {
			return namespaceEnginesChanged("presence_manager_name of " + routingLabel(name))
		}
		if recorded.mapBrokerName != newRouting.mapBrokerName {
			return namespaceEnginesChanged("map.broker_name of " + routingLabel(name))
		}
	}
	return nil
}

// CheckNamespaceEnginesReload refuses a reload which changes what the named brokers, presence managers and map
// brokers route: the set of enabled entries with their types and Redis or PostgreSQL prefixes, the engine selection
// of a namespace (recorded since startup, see NamespaceEnginesRecord), and the controller with its type and prefix.
// Such a change needs a restart. Connection details of the entries and of the controller are applied on reload as
// for the default engine.
func CheckNamespaceEnginesReload(oldCfg Config, newCfg Config, record *NamespaceEnginesRecord) error {
	if !slices.Equal(enabledBrokerKeys(oldCfg), enabledBrokerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of brokers")
	}
	if !slices.Equal(enabledPresenceManagerKeys(oldCfg), enabledPresenceManagerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of presence_managers")
	}
	if !slices.Equal(enabledMapBrokerKeys(oldCfg), enabledMapBrokerKeys(newCfg)) {
		return namespaceEnginesChanged("enabled entries of map_brokers")
	}
	if err := record.check(newCfg); err != nil {
		return err
	}
	if record != nil {
		// The boundary and the private prefix decide which namespace a channel name belongs to, so a change
		// would route the channels which have subscriptions to another engine.
		if oldCfg.Channel.NamespaceBoundary != newCfg.Channel.NamespaceBoundary {
			return namespaceEnginesChanged("channel.namespace_boundary")
		}
		if oldCfg.Channel.PrivatePrefix != newCfg.Channel.PrivatePrefix {
			return namespaceEnginesChanged("channel.private_prefix")
		}
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

// namespaceEnginesRemoved names the selection of a namespace which a reload removed while it selected an engine.
func namespaceEnginesRemoved(name string, recorded namespaceRouting) error {
	var selections []string
	if recorded.brokerName != "" {
		selections = append(selections, fmt.Sprintf("broker_name %q", recorded.brokerName))
	}
	if recorded.presenceManagerName != "" {
		selections = append(selections, fmt.Sprintf("presence_manager_name %q", recorded.presenceManagerName))
	}
	if recorded.mapBrokerName != "" {
		selections = append(selections, fmt.Sprintf("map.broker_name %q", recorded.mapBrokerName))
	}
	return fmt.Errorf(
		"namespace engines: %s selecting %s was removed, a restart is needed",
		routingLabel(name), strings.Join(selections, " and "),
	)
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

// enabledMapBrokerKeys identifies the enabled entries of map_brokers by name, type and the prefix which the running
// map broker keeps from startup: the Redis prefix, or the Postgres table prefix.
func enabledMapBrokerKeys(cfg Config) []string {
	var keys []string
	for _, mb := range cfg.MapBrokers {
		if !mb.Enabled {
			continue
		}
		key := mb.Name + "/" + mb.Type
		switch mb.Type {
		case "redis":
			key += "/" + mb.Redis.Prefix
		case "postgres":
			key += "/" + postgresBrokerPrefix(mb.Postgres.TablePrefix)
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
		mapBrokerName:       cfg.Channel.WithoutNamespace.Map.BrokerName,
	}}
	for _, ns := range cfg.Channel.Namespaces {
		routings = append(routings, namespaceRouting{
			name:                ns.Name,
			brokerName:          ns.BrokerName,
			presenceManagerName: ns.PresenceManagerName,
			mapBrokerName:       ns.Map.BrokerName,
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
