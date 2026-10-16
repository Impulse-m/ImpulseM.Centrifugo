package controllers

import (
	"context"
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/confighelpers"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// newRedisControllerFromConfig creates the Redis controller of the configuration on its own Redis shards.
func newRedisControllerFromConfig(node *centrifuge.Node, config configtypes.Controller) (centrifuge.Controller, error) {
	shards, mode, err := confighelpers.CentrifugeRedisShards(node, config.Redis.Redis)
	if err != nil {
		return nil, fmt.Errorf("error creating Redis shards for controller: %w", err)
	}
	controller, err := NewRedisController(node, shards, config.Redis.Prefix)
	if err != nil {
		for _, shard := range shards {
			shard.Close()
		}
		return nil, fmt.Errorf("error initializing Redis controller: %w", err)
	}
	log.Info().Str("controller_type", config.Type).Str("redis_mode", mode).Msg("controller is enabled")
	return controller, nil
}

// RedisController coordinates the nodes of a cluster over Redis PUB/SUB. It is a Redis broker which
// carries only control messages: the node does not use it for channels, so the connections of channel
// traffic and control traffic are separate.
type RedisController struct {
	broker *centrifuge.RedisBroker
	shards []*centrifuge.RedisShard
}

var _ centrifuge.Controller = (*RedisController)(nil)

// NewRedisController creates a controller on the given Redis shards. Control channels are derived from
// the prefix. It must be created after the node, since it uses the node ID.
func NewRedisController(node *centrifuge.Node, shards []*centrifuge.RedisShard, prefix string) (*RedisController, error) {
	broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
		Shards:     shards,
		Prefix:     prefix,
		Name:       "controller",
		SkipPubSub: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Redis controller: %w", err)
	}
	return &RedisController{broker: broker, shards: shards}, nil
}

// RegisterControlEventHandler starts the control PUB/SUB of every shard and returns once each one has subscribed,
// so the node does not serve clients before it receives the control messages of the other nodes.
func (c *RedisController) RegisterControlEventHandler(h centrifuge.ControlEventHandler) error {
	if err := c.broker.RegisterControlEventHandler(h); err != nil {
		return err
	}
	// Core waits for the control subscription only in RegisterBrokerEventHandler. The controller has SkipPubSub set,
	// so Core starts no channel subscriptions and returns after that wait, without calling the handler.
	return c.broker.RegisterBrokerEventHandler(noopBrokerEventHandler{})
}

// PublishControl publishes a control message to the node with nodeID, or to all nodes when nodeID is empty.
func (c *RedisController) PublishControl(data []byte, nodeID string, shardKey string) error {
	return c.broker.PublishControl(data, nodeID, shardKey)
}

// Close stops the controller and closes its Redis shards.
func (c *RedisController) Close(ctx context.Context) error {
	err := c.broker.Close(ctx)
	for _, shard := range c.shards {
		shard.Close()
	}
	return err
}

// noopBrokerEventHandler is the broker event handler of the Redis controller. The controller subscribes to no
// channels, so Core never calls it.
type noopBrokerEventHandler struct{}

// HandlePublication ignores the publication.
func (noopBrokerEventHandler) HandlePublication(
	_ string,
	_ *centrifuge.Publication,
	_ centrifuge.StreamPosition,
	_ bool,
	_ *centrifuge.Publication,
) error {
	return nil
}

// HandleJoin ignores the join.
func (noopBrokerEventHandler) HandleJoin(_ string, _ *centrifuge.ClientInfo) error {
	return nil
}

// HandleLeave ignores the leave.
func (noopBrokerEventHandler) HandleLeave(_ string, _ *centrifuge.ClientInfo) error {
	return nil
}
