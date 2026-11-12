package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// validateServerTags checks the server tags options of channel.without_namespace and of every namespace. The negative
// caps are refused in every namespace. The rules of allow_server_tags apply only where it is set: the broker of the
// namespace must be memory or redis, since the other brokers do not carry the server tags of a publication, and the
// namespace must not use shared poll or a subscribe stream proxy, which do not apply a server tags filter. A config
// which sets none of the options is not changed by this check.
func validateServerTags(c Config) error {
	if err := validateServerTagsOptions(c.Channel.WithoutNamespace, c); err != nil {
		return fmt.Errorf("in channel.without_namespace: %v", err)
	}
	for _, ns := range c.Channel.Namespaces {
		if err := validateServerTagsOptions(ns.ChannelOptions, c); err != nil {
			return fmt.Errorf("namespace %s: %v", ns.Name, err)
		}
	}
	return nil
}

// validateServerTagsOptions checks the server tags options of one channel options set.
func validateServerTagsOptions(opts configtypes.ChannelOptions, c Config) error {
	if opts.PublishTagsMaxCount < 0 {
		return fmt.Errorf("publish_tags_max_count must not be negative")
	}
	if opts.PublishTagsMaxBytes < 0 {
		return fmt.Errorf("publish_tags_max_bytes must not be negative")
	}
	if !opts.AllowServerTags {
		return nil
	}
	if opts.SubscriptionType == "shared_poll" {
		return fmt.Errorf("allow_server_tags is not supported with subscription_type shared_poll: shared poll subscriptions do not apply a server tags filter")
	}
	if opts.SubscribeStreamProxyEnabled {
		return fmt.Errorf("allow_server_tags cannot be used with subscribe_stream_proxy_enabled: a subscribe stream proxy subscription does not apply a server tags filter")
	}
	brokerType, ok := serverTagsBrokerType(c, opts.BrokerName)
	if !ok {
		return fmt.Errorf("broker_name %q is not an entry of brokers", opts.BrokerName)
	}
	if brokerType != "memory" && brokerType != "redis" {
		return fmt.Errorf("allow_server_tags requires the memory or redis broker, the broker of the namespace is %s", brokerType)
	}
	return nil
}

// serverTagsBrokerType returns the type of the broker of a namespace: the named broker which brokerName selects, or else
// the default broker, which is the standalone broker when it is enabled and the engine otherwise. It reports false when
// brokerName selects no entry of brokers.
func serverTagsBrokerType(c Config, brokerName string) (string, bool) {
	if brokerName != "" {
		for _, b := range c.Brokers {
			if b.Name == brokerName {
				return b.Type, true
			}
		}
		return "", false
	}
	if c.Broker.Enabled {
		return c.Broker.Type, true
	}
	return c.Engine.Type, true
}
