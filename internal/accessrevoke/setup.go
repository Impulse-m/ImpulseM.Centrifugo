package accessrevoke

import (
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/service"

	"github.com/centrifugal/centrifuge"
)

// Components are the access revocation parts of a node, created by Setup.
type Components struct {
	// Store holds revocation state. The client handler checks connections against it.
	Store *Store
	// Revoker applies revocation operations on this node.
	Revoker *Revoker
	// Notifier sends revocation operations to other nodes.
	Notifier *Notifier
	// Source is a source of revocation events. Nil when no source is configured.
	Source Source
	// Ready reports whether the source is ready. Nil when no source is configured.
	Ready func() bool
	// Services must be registered in the service manager to run.
	Services []service.Service
}

// Setup creates access revocation components of the node. Access revocation source is optional: when it
// is not configured, Source and Ready are nil, and nothing gates connections or health.
func Setup(node *centrifuge.Node, cfg config.Config, cfgContainer *config.Container) Components {
	store := NewStore(cfg.AccessRevoke.DefaultTTL.ToDuration())
	queue := NewDisconnectQueue(cfg.AccessRevoke.DisconnectRate, cfg.AccessRevoke.DisconnectQueueSize)
	revoker := NewRevoker(store, node.Hub(), queue)
	revoker.SetMaxTTL(func() time.Duration {
		return cfgContainer.ConfigRef().AccessRevoke.MaxTTL.ToDuration()
	})
	components := Components{
		Store:    store,
		Revoker:  revoker,
		Notifier: NewNotifier(node, cfg.AccessRevoke.NotifyRate),
		Services: []service.Service{
			NewSweeper(store, cfg.AccessRevoke.SweepInterval.ToDuration()),
			queue,
		},
	}
	if cfg.AccessRevoke.Source.Type == "kafka" {
		kafkaSource := NewKafkaSource(cfg.AccessRevoke.Source, cfgContainer, revoker)
		components.Source = kafkaSource
		components.Ready = kafkaSource.Ready
		components.Services = append(components.Services,
			kafkaSource,
			NewCatchUpSweeper(
				kafkaSource.CaughtUpAfterFailOpen(),
				node.Hub(),
				store,
				queue,
				func() CheckOptions {
					return OptionsFromConfig(cfgContainer.ConfigRef())
				},
			),
		)
	}
	return components
}
