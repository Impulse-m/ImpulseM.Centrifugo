package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
)

// validateRequireServerTagsFilter checks the require_server_tags_filter option of channel.without_namespace and of every
// namespace which has it. The rules of the option are checked on the raw config fields only: the compiled templates are
// not built before Validate returns. An existing config which does not set the option is not changed by this check.
//
// The rules for subscription_type shared_poll and for subscribe_stream_proxy_enabled are load-bearing, they guard against
// a fail-open: a shared poll subscription and a subscribe stream proxy subscription do not apply the server tags filter
// of their reply, so the option would not refuse them without these rules. The personal channel namespace is refused
// without a template, since its subscription is made with namespace defaults on every connect. A client which skips the
// signature check can mint any filter, so the option is not meaningful with it.
func validateRequireServerTagsFilter(c Config) error {
	if !anyRequireServerTagsFilter(&c) {
		return nil
	}
	if c.Channel.WithoutNamespace.RequireServerTagsFilter {
		if err := validateRequireServerTagsFilterOptions("", c.Channel.WithoutNamespace, c); err != nil {
			return fmt.Errorf("in channel.without_namespace: %v", err)
		}
	}
	for _, ns := range c.Channel.Namespaces {
		if !ns.RequireServerTagsFilter {
			continue
		}
		if err := validateRequireServerTagsFilterOptions(ns.Name, ns.ChannelOptions, c); err != nil {
			return fmt.Errorf("namespace %s: %v", ns.Name, err)
		}
	}
	return nil
}

// validateRequireServerTagsFilterOptions checks the options of the namespace name, "" for channels without namespace.
func validateRequireServerTagsFilterOptions(name string, opts configtypes.ChannelOptions, c Config) error {
	if opts.SubscriptionType == "shared_poll" {
		return fmt.Errorf("require_server_tags_filter is not supported with subscription_type shared_poll: shared poll subscriptions do not apply a server tags filter")
	}
	if opts.SubscribeStreamProxyEnabled {
		return fmt.Errorf("require_server_tags_filter cannot be used with subscribe_stream_proxy_enabled: a subscribe stream proxy subscription cannot carry a server tags filter")
	}
	personalNamespace := c.Client.SubscribeToUserPersonalChannel.Enabled && c.Client.SubscribeToUserPersonalChannel.PersonalChannelNamespace == name
	if personalNamespace && len(opts.ServerTagsFilterTemplate.Filter) == 0 {
		return fmt.Errorf("require_server_tags_filter cannot be used on the personal channel namespace: the personal channel subscription cannot carry a server tags filter")
	}
	if c.Client.InsecureSkipTokenSignatureVerify {
		return fmt.Errorf("require_server_tags_filter cannot be used with client.insecure_skip_token_signature_verify: an unverified token can carry any server tags filter")
	}
	return nil
}
