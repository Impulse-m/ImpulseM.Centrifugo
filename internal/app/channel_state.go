package app

import (
	"context"
	"errors"
	"slices"

	"github.com/centrifugal/centrifugo/v6/internal/client"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"

	"github.com/centrifugal/centrifuge"
)

// channelStateEngineConfig returns the channel state config of the Redis presence manager. The channel_state
// settings are the ones of the placement the presence manager is configured in. It returns nil when no
// channel state proxy exists, so the presence manager keeps no channel state.
func channelStateEngineConfig(
	cfgContainer *config.Container,
	proxyMap *client.ProxyMap,
	placement configtypes.RedisChannelState,
) *centrifuge.RedisChannelStateConfig {
	if proxyMap.ChannelStateProxy == nil {
		return nil
	}
	handler := proxy.NewChannelStateHandler(proxyMap.ChannelStateProxy, metrics.ChannelStateOldestFailingBatch)
	deliver := handler.Handle()
	vacatedEventDelay := placement.VacatedEventDelay.ToDuration()
	if vacatedEventDelay == 0 {
		// Core reads a zero delay as its default, so a configured zero is passed as no delay.
		vacatedEventDelay = -1
	}
	return &centrifuge.RedisChannelStateConfig{
		Enabled: func(ch string) bool {
			chOpts, ok, err := cfgContainer.ChannelOptionsRef(ch)
			return err == nil && ok && chOpts.StateProxyEnabled
		},
		Handler: func(ctx context.Context, events []centrifuge.ChannelStateEvent) error {
			batch := channelStateBatch(events)
			err := deliver(ctx, batch)
			if err != nil && ctx.Err() != nil {
				// The partition is stopped and another node delivers the batch: it is not retried here.
				handler.Forget(batch)
			}
			return err
		},
		NumPartitions:     placement.NumPartitions,
		VacatedEventDelay: vacatedEventDelay,
	}
}

// channelStateBatch converts the events of Core to the proxy events of one batch.
func channelStateBatch(events []centrifuge.ChannelStateEvent) []*proxyproto.ChannelEvent {
	batch := make([]*proxyproto.ChannelEvent, 0, len(events))
	for _, event := range events {
		batch = append(batch, &proxyproto.ChannelEvent{
			TimeMs:  event.Time.UnixMilli(),
			Channel: event.Channel,
			Type:    string(event.Type),
		})
	}
	return batch
}

// channelStateLayout returns the names of the channel groups with state_proxy_enabled, sorted. The group of
// channels without namespace has the empty name.
func channelStateLayout(cfg config.Config) []string {
	var layout []string
	if cfg.Channel.WithoutNamespace.StateProxyEnabled {
		layout = append(layout, "")
	}
	for _, ns := range cfg.Channel.Namespaces {
		if ns.StateProxyEnabled {
			layout = append(layout, ns.Name)
		}
	}
	slices.Sort(layout)
	return layout
}

// checkChannelStateReload rejects a reloaded config which enables or disables channel state for a channel group.
// The presence keys of a channel are chosen by its group while its subscribers are connected, so the groups
// can change only with a restart.
func checkChannelStateReload(startup []string, newCfg config.Config) error {
	if slices.Equal(startup, channelStateLayout(newCfg)) {
		return nil
	}
	return errors.New("channel state layout changed by reload: enabling or disabling state_proxy_enabled needs a restart, and the whole reload is refused until then")
}
