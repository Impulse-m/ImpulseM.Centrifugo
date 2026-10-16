package app

import (
	"context"
	"fmt"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/confighelpers"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/natsbroker"
	"github.com/centrifugal/centrifugo/v6/internal/pgmapbroker"
	"github.com/centrifugal/centrifugo/v6/internal/service"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// namespaceEngines holds the named brokers, presence managers and map brokers which channels select with the
// broker_name, presence_manager_name and map.broker_name channel options. The maps are filled once, before the
// node runs, and never change after that, so the routing functions read them without a lock. A nil
// *namespaceEngines is valid: no namespace selects a named engine, and every method does nothing.
type namespaceEngines struct {
	cfgContainer *config.Container
	// record holds the engine selection of every namespace name seen since startup, see config.NamespaceEnginesRecord.
	record           *config.NamespaceEnginesRecord
	brokers          map[string]centrifuge.Broker
	presenceManagers map[string]centrifuge.PresenceManager
	mapBrokers       map[string]centrifuge.MapBroker
	// redisShards are the Redis connections of the named Redis brokers, presence managers and map brokers. None of
	// them closes its shards, so the engines close them.
	redisShards   []*centrifuge.RedisShard
	closeTimeout  time.Duration
	routeBrokers  bool
	routePresence bool
	routeMap      bool
}

// namespaceEnginesService closes the named engines when the service stops.
type namespaceEnginesService struct {
	engines *namespaceEngines
}

// newNamespaceEngines returns nil when no enabled entry of brokers, presence_managers or map_brokers exists.
// Otherwise it returns the engines which create and route them.
func newNamespaceEngines(cfgContainer *config.Container) *namespaceEngines {
	cfg := cfgContainer.Config()

	// Routing hooks are set for each kind with an enabled entry, not only for the kinds which a namespace
	// selects at startup: a reload may add a namespace which selects an existing entry. The validation
	// guarantees that a selection names an enabled entry, so the entries are the whole condition.
	var routeBrokers bool
	for _, entry := range cfg.Brokers {
		if entry.Enabled {
			routeBrokers = true
		}
	}
	var routePresence bool
	for _, entry := range cfg.PresenceManagers {
		if entry.Enabled {
			routePresence = true
		}
	}
	var routeMap bool
	for _, entry := range cfg.MapBrokers {
		if entry.Enabled {
			routeMap = true
		}
	}

	if !routeBrokers && !routePresence && !routeMap {
		return nil
	}
	return &namespaceEngines{
		cfgContainer:     cfgContainer,
		record:           config.NewNamespaceEnginesRecord(cfg),
		brokers:          make(map[string]centrifuge.Broker),
		presenceManagers: make(map[string]centrifuge.PresenceManager),
		mapBrokers:       make(map[string]centrifuge.MapBroker),
		closeTimeout:     cfg.Shutdown.Timeout.ToDuration(),
		routeBrokers:     routeBrokers,
		routePresence:    routePresence,
		routeMap:         routeMap,
	}
}

// nodeConfig sets the routing hooks of the node config. A hook is set for each kind which has an enabled entry,
// so a reload may route a namespace to that entry. Once set, the hook costs one channel options lookup for every
// channel operation, including the channels of namespaces which select no entry. A kind without an enabled entry
// gets no hook and costs nothing.
func (e *namespaceEngines) nodeConfig(cfg *centrifuge.Config) {
	if e == nil {
		return
	}
	if e.routeBrokers {
		cfg.GetBroker = e.broker
	}
	if e.routePresence {
		cfg.GetPresenceManager = e.presenceManager
	}
	if e.routeMap {
		cfg.Map.GetMapBroker = e.mapBroker
	}
}

// checkReload refuses a reload which changes the routing of the named engines, see config.CheckNamespaceEnginesReload.
// A nil *namespaceEngines has no record, so it still refuses a reload which enables an entry.
func (e *namespaceEngines) checkReload(oldCfg config.Config, newCfg config.Config) error {
	var record *config.NamespaceEnginesRecord
	if e != nil {
		record = e.record
	}
	return config.CheckNamespaceEnginesReload(oldCfg, newCfg, record)
}

// commitReload records the namespaces of a reload which has been applied.
func (e *namespaceEngines) commitReload(newCfg config.Config) {
	if e == nil {
		return
	}
	e.record.Commit(newCfg)
}

// create creates every enabled entry of brokers and presence_managers. Entries are created even when no
// namespace selects them yet, so their connections are checked at startup.
func (e *namespaceEngines) create(node *centrifuge.Node, cfgContainer *config.Container) error {
	if e == nil {
		return nil
	}
	cfg := cfgContainer.Config()
	for _, entry := range cfg.Brokers {
		if !entry.Enabled {
			continue
		}
		broker, mode, err := e.createBroker(node, cfgContainer, entry)
		if err != nil {
			return fmt.Errorf("error creating broker %s: %w", entry.Name, err)
		}
		e.brokers[entry.Name] = broker
		log.Info().
			Str("kind", "broker").
			Str("name", entry.Name).
			Str("type", entry.Type).
			Str("mode", mode).
			Msg("namespace engine created")
	}
	for _, entry := range cfg.PresenceManagers {
		if !entry.Enabled {
			continue
		}
		presenceManager, redisShards, mode, err := createNamedPresenceManager(node, entry)
		e.redisShards = append(e.redisShards, redisShards...)
		if err != nil {
			return fmt.Errorf("error creating presence manager %s: %w", entry.Name, err)
		}
		e.presenceManagers[entry.Name] = presenceManager
		log.Info().
			Str("kind", "presence_manager").
			Str("name", entry.Name).
			Str("type", entry.Type).
			Str("mode", mode).
			Msg("namespace engine created")
	}
	for _, entry := range cfg.MapBrokers {
		if !entry.Enabled {
			continue
		}
		mapBroker, redisShards, mode, err := createNamedMapBroker(node, entry)
		e.redisShards = append(e.redisShards, redisShards...)
		if err != nil {
			return fmt.Errorf("error creating map broker %s: %w", entry.Name, err)
		}
		e.mapBrokers[entry.Name] = mapBroker
		log.Info().
			Str("kind", "map_broker").
			Str("name", entry.Name).
			Str("type", entry.Type).
			Str("mode", mode).
			Msg("namespace engine created")
	}
	return nil
}

// run registers the event handler of every named broker and map broker. It must be called after the node runs:
// the default broker and the default map broker are registered by the node itself.
func (e *namespaceEngines) run(node *centrifuge.Node) error {
	if e == nil {
		return nil
	}
	for name, broker := range e.brokers {
		if err := broker.RegisterBrokerEventHandler(node); err != nil {
			return fmt.Errorf("error running broker %s: %w", name, err)
		}
	}
	for name, mapBroker := range e.mapBrokers {
		if err := mapBroker.RegisterEventHandler(node); err != nil {
			return fmt.Errorf("error running map broker %s: %w", name, err)
		}
	}
	return nil
}

// service returns the service which closes the named engines when the application shuts down.
func (e *namespaceEngines) service() service.Service {
	return &namespaceEnginesService{engines: e}
}

// Run waits for the context to be done, then closes the named engines.
func (s *namespaceEnginesService) Run(ctx context.Context) error {
	<-ctx.Done()
	s.engines.close()
	return ctx.Err()
}

// close closes the named brokers and presence managers within the shutdown timeout, then the Redis connections
// of the named Redis brokers and presence managers.
func (e *namespaceEngines) close() {
	if e == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.closeTimeout)
	defer cancel()
	for name, broker := range e.brokers {
		if closer, ok := broker.(centrifuge.Closer); ok {
			if err := closer.Close(ctx); err != nil {
				log.Error().Err(err).Str("kind", "broker").Str("name", name).Msg("error closing namespace engine")
			}
		}
	}
	for name, presenceManager := range e.presenceManagers {
		if closer, ok := presenceManager.(centrifuge.Closer); ok {
			if err := closer.Close(ctx); err != nil {
				log.Error().Err(err).Str("kind", "presence_manager").Str("name", name).Msg("error closing namespace engine")
			}
		}
	}
	for name, mapBroker := range e.mapBrokers {
		if closer, ok := mapBroker.(centrifuge.Closer); ok {
			if err := closer.Close(ctx); err != nil {
				log.Error().Err(err).Str("kind", "map_broker").Str("name", name).Msg("error closing namespace engine")
			}
		}
	}
	for _, shard := range e.redisShards {
		shard.Close()
	}
}

// broker returns the named broker which the channel selects with broker_name. It returns false for
// channels whose namespace selects no named broker, and it does not allocate.
func (e *namespaceEngines) broker(ch string) (centrifuge.Broker, bool) {
	if e == nil {
		return nil, false
	}
	// ChannelOptionsRef never fails, so the lookup has no error to handle.
	chOpts, ok, _ := e.cfgContainer.ChannelOptionsRef(ch)
	if !ok || chOpts.BrokerName == "" {
		return nil, false
	}
	broker, ok := e.brokers[chOpts.BrokerName]
	return broker, ok
}

// presenceManager returns the named presence manager which the channel selects with
// presence_manager_name. It returns false for channels whose namespace selects none, and it does not allocate.
func (e *namespaceEngines) presenceManager(ch string) (centrifuge.PresenceManager, bool) {
	if e == nil {
		return nil, false
	}
	// ChannelOptionsRef never fails, so the lookup has no error to handle.
	chOpts, ok, _ := e.cfgContainer.ChannelOptionsRef(ch)
	if !ok || chOpts.PresenceManagerName == "" {
		return nil, false
	}
	presenceManager, ok := e.presenceManagers[chOpts.PresenceManagerName]
	return presenceManager, ok
}

// mapBroker returns the named map broker which the channel selects with map.broker_name. It returns false for
// channels whose namespace selects none, and it does not allocate.
func (e *namespaceEngines) mapBroker(ch string) (centrifuge.MapBroker, bool) {
	if e == nil {
		return nil, false
	}
	// ChannelOptionsRef never fails, so the lookup has no error to handle.
	chOpts, ok, _ := e.cfgContainer.ChannelOptionsRef(ch)
	if !ok || chOpts.Map.BrokerName == "" {
		return nil, false
	}
	mapBroker, ok := e.mapBrokers[chOpts.Map.BrokerName]
	return mapBroker, ok
}

// createBroker creates a named broker by its type. It returns the connection mode for logging.
func (e *namespaceEngines) createBroker(
	node *centrifuge.Node,
	cfgContainer *config.Container,
	entry configtypes.NamedBroker,
) (centrifuge.Broker, string, error) {
	switch entry.Type {
	case "memory":
		broker, err := createMemoryBroker(node)
		if err != nil {
			return nil, "", err
		}
		return broker, "", nil
	case "redis":
		redisShards, mode, err := confighelpers.CentrifugeRedisShards(node, entry.Redis.Redis)
		if err != nil {
			return nil, "", fmt.Errorf("error creating Redis shards: %w", err)
		}
		e.redisShards = append(e.redisShards, redisShards...)
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
			Shards:   redisShards,
			Prefix:   entry.Redis.Prefix,
			UseLists: entry.Redis.UseLists,
			Name:     entry.Name,
		})
		if err != nil {
			return nil, mode, fmt.Errorf("error creating Redis broker: %w", err)
		}
		return broker, mode, nil
	case "nats":
		broker, err := natsBrokerFrom(node, cfgContainer, entry.Nats)
		if err != nil {
			return nil, "", err
		}
		return broker, "nats", nil
	case "postgres":
		broker, err := createPostgresStreamBroker(node, entry.Postgres)
		if err != nil {
			return nil, "", err
		}
		return broker, "postgres", nil
	default:
		return nil, "", fmt.Errorf("unknown broker type: %s", entry.Type)
	}
}

// natsBrokerFrom creates a Nats broker with the settings of natsCfg, which applies publication_data_format of
// the channels to the messages of raw mode. The default broker and the named brokers use it.
func natsBrokerFrom(
	node *centrifuge.Node,
	cfgContainer *config.Container,
	natsCfg configtypes.NatsBroker,
) (*natsbroker.NatsBroker, error) {
	broker, err := natsbroker.New(node, natsCfg)
	if err != nil {
		return nil, err
	}
	broker.SetRawModeDataCheck(rawModeDataCheck(cfgContainer))
	return broker, nil
}

// createNamedPresenceManager creates a named presence manager by its type. It returns the Redis shards it
// created, which the caller closes at shutdown, and the connection mode for logging.
func createNamedPresenceManager(
	node *centrifuge.Node,
	entry configtypes.NamedPresenceManager,
) (centrifuge.PresenceManager, []*centrifuge.RedisShard, string, error) {
	switch entry.Type {
	case "memory":
		presenceManager, err := createMemoryPresenceManager(node)
		if err != nil {
			return nil, nil, "", err
		}
		return presenceManager, nil, "", nil
	case "redis":
		redisShards, mode, err := confighelpers.CentrifugeRedisShards(node, entry.Redis.Redis)
		if err != nil {
			return nil, nil, "", fmt.Errorf("error creating Redis shards: %w", err)
		}
		presenceManager, err := confighelpers.CentrifugeRedisPresenceManager(
			node, entry.Redis.Prefix, redisShards, entry.Redis.RedisPresenceManagerCommon)
		if err != nil {
			return nil, redisShards, mode, fmt.Errorf("error creating Redis presence manager: %w", err)
		}
		return presenceManager, redisShards, mode, nil
	default:
		return nil, nil, "", fmt.Errorf("unknown presence manager type: %s", entry.Type)
	}
}

// createNamedMapBroker creates a named map broker by its type. It returns the Redis shards it created, which the
// caller closes at shutdown, and the connection mode for logging.
func createNamedMapBroker(
	node *centrifuge.Node,
	entry configtypes.NamedMapBroker,
) (centrifuge.MapBroker, []*centrifuge.RedisShard, string, error) {
	switch entry.Type {
	case "memory":
		mapBroker, err := centrifuge.NewMemoryMapBroker(node, centrifuge.MemoryMapBrokerConfig{})
		if err != nil {
			return nil, nil, "", err
		}
		return mapBroker, nil, "", nil
	case "redis":
		redisShards, mode, err := confighelpers.CentrifugeRedisShards(node, entry.Redis.Redis)
		if err != nil {
			return nil, nil, "", fmt.Errorf("error creating Redis shards: %w", err)
		}
		mapBroker, err := confighelpers.CentrifugeRedisMapBroker(
			node, entry.Redis.Prefix, redisShards, entry.Redis.RedisMapBrokerCommon)
		if err != nil {
			return nil, redisShards, mode, fmt.Errorf("error creating Redis map broker: %w", err)
		}
		return mapBroker, redisShards, mode, nil
	case "postgres":
		mapBroker, err := createPostgresMapBroker(node, entry.Postgres)
		if err != nil {
			return nil, nil, "", err
		}
		return mapBroker, nil, "postgres", nil
	default:
		return nil, nil, "", fmt.Errorf("unknown map broker type: %s", entry.Type)
	}
}

// createPostgresMapBroker creates a Postgres map broker with the settings of a map_broker section. It initializes
// the schema unless skip_schema_init is set. The default map broker and the named map brokers use it.
func createPostgresMapBroker(
	node *centrifuge.Node,
	pgCfg configtypes.PostgresMapBroker,
) (*pgmapbroker.PostgresMapBroker, error) {
	pgBrokerCfg := pgmapbroker.PostgresMapBrokerConfig{
		DSN:                    pgCfg.DSN,
		TLS:                    pgCfg.TLS,
		PoolSize:               pgCfg.PoolSize,
		NumShards:              pgCfg.NumShards,
		TTLCheckInterval:       pgCfg.TTLCheckInterval.ToDuration(),
		CleanupInterval:        pgCfg.CleanupInterval.ToDuration(),
		IdempotentResultTTL:    pgCfg.IdempotentResultTTL.ToDuration(),
		BinaryData:             pgCfg.BinaryData,
		StreamRetention:        pgCfg.StreamRetention.ToDuration(),
		UseNotify:              pgCfg.UseNotify,
		NotifyDSN:              pgCfg.NotifyDSN,
		TablePrefix:            pgCfg.TablePrefix,
		PartitionLookaheadDays: pgCfg.PartitionLookaheadDays,
		PartitionRetentionDays: pgCfg.PartitionRetentionDays,
		Outbox: pgmapbroker.OutboxConfig{
			PollInterval: pgCfg.Outbox.PollInterval.ToDuration(),
			BatchSize:    pgCfg.Outbox.BatchSize,
		},
	}
	mapBroker, err := pgmapbroker.NewPostgresMapBroker(node, pgBrokerCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating Postgres map broker: %w", err)
	}
	if !pgCfg.SkipSchemaInit {
		if schemaErr := mapBroker.EnsureSchema(context.Background()); schemaErr != nil {
			return nil, fmt.Errorf("error initializing Postgres map broker schema: %w", schemaErr)
		}
	}
	return mapBroker, nil
}
