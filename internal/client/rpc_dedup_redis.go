package client

import (
	"context"
	"fmt"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"
	"github.com/centrifugal/centrifugo/v6/internal/rpcdedup"

	"github.com/rs/zerolog/log"
)

// setupRPCDedupRedis builds the shared tier of exactly-once RPC when rpc.registry.dedup.redis.enabled is set. It
// wraps the node-local store made by newRPCDedupState, and mirrors newRedisUserLimiter: the Redis and the prefix of
// the dedup config, or those of the engine with reuse_from_engine. It makes one connect attempt now, and the others
// run as a service, see Services. A node starts when Redis is unreachable.
func (h *Handler) setupRPCDedupRedis() error {
	if h.rpcDedup == nil {
		return nil
	}
	cfg := h.cfgContainer.ConfigRef()
	dedup := cfg.RPC.Registry.Dedup
	if !dedup.Redis.Enabled {
		return nil
	}
	redisConf, prefix := redisConfigOf(dedup.Redis.ReuseFromEngine, dedup.Redis.RedisPrefixed, cfg.Engine.Redis.RedisPrefixed)
	configs, err := redisshard.ParseRedisShardConfigs(redisConf)
	if err != nil {
		return fmt.Errorf("rpc.registry.dedup.redis: %w", err)
	}
	failOpen := dedup.OnStoreError == configtypes.RPCDedupFailOpen
	connector := redisshard.NewConnector(configs, rpcDedupConnectLog(failOpen))
	// The error is logged by the connect log; the node runs on without Redis.
	_ = connector.Connect()

	shared := rpcdedup.NewShared(rpcdedup.SharedOptions{
		Local:         h.rpcDedup.memory,
		Backend:       rpcdedup.NewRedis(prefix, connector, redisConf.IOTimeout.ToDuration(), h.rpcDedup.metrics),
		NodeID:        h.node.ID(),
		TTL:           rpcdedup.RecordTTL(dedup.Window.ToDuration(), dedup.MaxSkew.ToDuration()),
		MaxReplyBytes: min(dedup.MaxReplySize, dedup.ScopeReserveBytes/dedup.ScopeReserve),
		FailOpen:      failOpen,
		Notifier:      h.notifyRPCDedupCancel,
		Observer:      h.rpcDedup.metrics,
	})
	h.rpcDedup.store = shared
	h.rpcDedup.shared = shared
	h.rpcDedup.connector = connector
	return nil
}

// rpcDedupRedisService runs the shared tier as one service. The store flushes its acks until the service is stopped,
// and the connector closes the shards only after that, so the last acks reach Redis.
type rpcDedupRedisService struct {
	connector *redisshard.Connector
	shared    *rpcdedup.Shared
}

// newRPCDedupRedisService returns the service of the connector and the store of the shared tier.
func newRPCDedupRedisService(connector *redisshard.Connector, shared *rpcdedup.Shared) *rpcDedupRedisService {
	return &rpcDedupRedisService{connector: connector, shared: shared}
}

// Run runs the store until ctx is done, flushing its acks, and then stops the connector, which closes the shards. It
// implements service.Service.
func (s *rpcDedupRedisService) Run(ctx context.Context) error {
	connCtx, stopConnector := context.WithCancel(context.Background())
	defer stopConnector()
	connDone := make(chan struct{})
	go func() {
		defer close(connDone)
		_ = s.connector.Run(connCtx)
	}()
	err := s.shared.Run(ctx)
	stopConnector()
	<-connDone
	return err
}

// rpcDedupConnectLog returns the log of the connect attempts of the shared tier. A failure says what the calls do
// meanwhile, which depends on on_store_error.
func rpcDedupConnectLog(failOpen bool) redisshard.ConnectLog {
	if failOpen {
		return redisshard.LogConnect("exactly-once RPC", "calls run with node-local dedup until it is reachable")
	}
	return redisshard.LogConnect("exactly-once RPC", "tracked calls are answered with dedup_unavailable until it is reachable")
}

// notifyRPCDedupCancel asks the node nodeID to cancel the call of key, which runs there. A failure is logged at most
// once per interval. The scope of the key is not logged.
func (h *Handler) notifyRPCDedupCancel(nodeID string, key rpcdedup.Key) {
	err := h.node.Notify(rpcdedup.CancelNotificationOp, rpcdedup.EncodeCancel(key), nodeID)
	if err == nil {
		return
	}
	if h.rpcDedup.notifyLog.Allow() {
		log.Error().Err(err).Str("node_id", nodeID).Msg("error notifying a node to cancel an exactly-once RPC call")
	}
}

// NotificationHandlers returns the node notification handlers of the client handler, which the node registers. It has
// the cancel of exactly-once RPC when the shared tier is on, and is empty otherwise.
func (h *Handler) NotificationHandlers() map[string]centrifuge.NotificationHandler {
	handlers := map[string]centrifuge.NotificationHandler{}
	if h.rpcDedup == nil || h.rpcDedup.shared == nil {
		return handlers
	}
	handlers[rpcdedup.CancelNotificationOp] = h.onRPCDedupCancel
	return handlers
}

// onRPCDedupCancel cancels the call which a cancel notification names, when it runs on this node. The sender cancels
// its own calls before it notifies, so its own notifications are ignored.
func (h *Handler) onRPCDedupCancel(event centrifuge.NotificationEvent) {
	if event.FromNodeID == h.node.ID() {
		return
	}
	key, err := rpcdedup.DecodeCancel(event.Data)
	if err != nil {
		log.Error().Err(err).Str("from_node_id", event.FromNodeID).Msg("error decoding exactly-once RPC cancel notification")
		return
	}
	h.rpcDedup.shared.CancelFlight(key)
}
