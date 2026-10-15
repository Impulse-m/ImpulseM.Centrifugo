package controllers

import (
	"context"
	"fmt"

	"github.com/centrifugal/centrifuge"
)

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

// RegisterControlEventHandler starts the control PUB/SUB of every shard.
func (c *RedisController) RegisterControlEventHandler(h centrifuge.ControlEventHandler) error {
	return c.broker.RegisterControlEventHandler(h)
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
