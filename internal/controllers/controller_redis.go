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
	// The controller has no history, so Core does not need the Redis streams capability check (UseLists).
	broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
		Shards:     shards,
		Prefix:     prefix,
		Name:       configtypes.RedisControllerBrokerName,
		UseLists:   true,
		SkipPubSub: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Redis controller: %w", err)
	}
	return &RedisController{broker: broker, shards: shards}, nil
}

// RegisterControlEventHandler starts the control PUB/SUB of every shard. It returns once Core has finished the first
// subscription attempt of each shard. Core discards the error of that attempt and retries a failed one in the
// background, so the return does not prove that the control messages of other nodes are received.
func (c *RedisController) RegisterControlEventHandler(h centrifuge.ControlEventHandler) error {
	if err := c.broker.RegisterControlEventHandler(h); err != nil {
		return err
	}
	// Core waits for the first control subscription of each shard only in RegisterBrokerEventHandler. The controller has
	// SkipPubSub set, so Core starts no channel subscriptions and returns after that wait, without calling the handler.
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

// noopBrokerEventHandler is the broker event handler of the controller. Core never calls it, since the controller has
// SkipPubSub set, so the node never receives a channel message from the controller.
type noopBrokerEventHandler struct{}

// HandlePublication does nothing.
func (noopBrokerEventHandler) HandlePublication(
	ch string,
	pub *centrifuge.Publication,
	sp centrifuge.StreamPosition,
	useDelta bool,
	prevPub *centrifuge.Publication,
) error {
	return nil
}

// HandleJoin does nothing.
func (noopBrokerEventHandler) HandleJoin(ch string, info *centrifuge.ClientInfo) error {
	return nil
}

// HandleLeave does nothing.
func (noopBrokerEventHandler) HandleLeave(ch string, info *centrifuge.ClientInfo) error {
	return nil
}
