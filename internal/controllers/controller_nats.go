package controllers

import (
	"context"
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/natsbroker"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// newNatsControllerFromConfig creates the Nats controller of the configuration.
func newNatsControllerFromConfig(node *centrifuge.Node, config configtypes.Controller) (centrifuge.Controller, error) {
	controller, err := NewNatsController(node, config.Nats)
	if err != nil {
		return nil, fmt.Errorf("error initializing Nats controller: %w", err)
	}
	log.Info().Str("controller_type", config.Type).Msg("controller is enabled")
	return controller, nil
}

// NatsController coordinates the nodes of a cluster over Nats. It is a Nats broker which carries only
// control messages: the node does not use it for channels, so the connections of channel traffic and
// control traffic are separate.
type NatsController struct {
	broker *natsbroker.NatsBroker
}

var _ centrifuge.Controller = (*NatsController)(nil)

// NewNatsController creates a controller which connects to Nats when its control event handler is registered.
func NewNatsController(node *centrifuge.Node, cfg configtypes.NatsController) (*NatsController, error) {
	broker, err := natsbroker.New(node, natsbroker.Config{NatsPrefixed: cfg.NatsPrefixed})
	if err != nil {
		return nil, fmt.Errorf("error creating Nats controller: %w", err)
	}
	return &NatsController{broker: broker}, nil
}

// RegisterControlEventHandler connects to Nats and subscribes to the control channels.
func (c *NatsController) RegisterControlEventHandler(h centrifuge.ControlEventHandler) error {
	return c.broker.RegisterControlEventHandler(h)
}

// PublishControl publishes a control message to the node with nodeID, or to all nodes when nodeID is empty.
func (c *NatsController) PublishControl(data []byte, nodeID string, shardKey string) error {
	return c.broker.PublishControl(data, nodeID, shardKey)
}

// Close closes the Nats connection. It must be called after RegisterControlEventHandler.
func (c *NatsController) Close(ctx context.Context) error {
	return c.broker.Close(ctx)
}
