package client

import (
	"errors"
	"slices"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/logging"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog/log"
)

// serverTagsFilterClaimsKey is a client storage key with the values of the connection token claims which
// the server tags filter templates refer to. They are stored as the connection was verified with them, and
// every template of the connection is rendered with them.
const serverTagsFilterClaimsKey = "server_tags_filter_template_claims"

// connectFilterTemplates applies the server tags filter templates to the subscriptions of a connection. The
// values of the connection are stored, so that later subscriptions and refreshes render the same filters.
// A missing value of a template under reject refuses the connection.
func (h *Handler) connectFilterTemplates(
	cfg *config.Config,
	clientID string,
	credentials *centrifuge.Credentials,
	verified jwtverify.ConnectToken,
	subscriptions map[string]centrifuge.SubscribeOptions,
	storage map[string]any,
) error {
	if !cfg.HasServerTagsFilterTemplates() {
		return nil
	}
	values := filtertemplate.Values{Claims: verified.FilterTemplateClaims, Client: clientID}
	if credentials != nil {
		values.User = credentials.UserID
	}
	if values.Claims.Values != nil {
		storage[serverTagsFilterClaimsKey] = values.Claims
	}
	if err := h.applyFilterTemplates(subscriptions, values); err != nil {
		return centrifuge.DisconnectPermissionDenied
	}
	return nil
}

// subscribeFilterTemplate applies the server tags filter template of the channel to the options of a client
// subscription. A missing value under reject refuses the subscription.
func (h *Handler) subscribeFilterTemplate(client Client, channel string, opts *centrifuge.SubscribeOptions) error {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	filter, err := h.channelFilter(client, channel, opts.ServerTagsFilter)
	if err != nil {
		return centrifuge.ErrorPermissionDenied
	}
	opts.ServerTagsFilter = filter
	return nil
}

// refreshFilterTemplate returns the server tags filter of a subscription refresh which sets explicit. It is the
// template of the channel combined with explicit, so a refresh with a new explicit filter keeps the template.
func (h *Handler) refreshFilterTemplate(client Client, channel string, explicit *protocol.FilterNode) (*protocol.FilterNode, error) {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return explicit, nil
	}
	filter, err := h.channelFilter(client, channel, explicit)
	if err != nil {
		return nil, centrifuge.ErrorPermissionDenied
	}
	return filter, nil
}

// tokenSubscriptionsChanged reports whether the server-side subscriptions of a connection changed on refresh.
// A connection whose template claims changed is changed: its filters are fixed for its life, so it is
// disconnected. Otherwise the templates are applied to the subscriptions of the token before they are compared.
func (h *Handler) tokenSubscriptionsChanged(client *centrifuge.Client, extra RefreshExtra) bool {
	if h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		storage, release := client.AcquireStorage()
		stored, _ := storage[serverTagsFilterClaimsKey].(filtertemplate.Claims)
		release(storage)
		if !slices.Equal(extra.FilterTemplateClaims.Values, stored.Values) {
			return true
		}
		if err := h.templateSubscriptions(client, extra.Subs); err != nil {
			return true
		}
	}
	return tokenChannelsChanged(client, extra.Subs)
}

// rebindFilterTemplates stores the template claims of a connection which was rebound to a token. When the claims
// changed, the channels with a template are unsubscribed, so the client resubscribes them and they get filters
// rendered with the new values.
func (h *Handler) rebindFilterTemplates(c rebindableClient, verified jwtverify.ConnectToken) {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return
	}
	storage, release := c.AcquireStorage()
	stored, _ := storage[serverTagsFilterClaimsKey].(filtertemplate.Claims)
	if slices.Equal(stored.Values, verified.FilterTemplateClaims.Values) {
		release(storage)
		return
	}
	storage[serverTagsFilterClaimsKey] = verified.FilterTemplateClaims
	release(storage)
	for channel := range c.ChannelsWithContext() {
		if h.channelHasFilterTemplate(channel) {
			c.Unsubscribe(channel, centrifuge.Unsubscribe{Code: centrifuge.UnsubscribeCodeInsufficient, Reason: "insufficient state"})
		}
	}
}

// templateSubscriptions applies the server tags filter templates to subscriptions made on a connection after
// its connect, such as the ones of a rebind. A missing value under reject refuses them.
func (h *Handler) templateSubscriptions(c Client, subs map[string]centrifuge.SubscribeOptions) error {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	if err := h.applyFilterTemplates(subs, connectionFilterValues(c)); err != nil {
		return centrifuge.ErrorPermissionDenied
	}
	return nil
}

// applyFilterTemplates sets the server tags filter of every subscription whose namespace has a template to the
// template combined with the explicit filter of the subscription. It returns the error of the first missing value
// under reject.
func (h *Handler) applyFilterTemplates(subs map[string]centrifuge.SubscribeOptions, values filtertemplate.Values) error {
	for channel, opts := range subs {
		chOpts, found, err := h.cfgContainer.ChannelOptionsRef(channel)
		if err != nil {
			return err
		}
		if !found || chOpts.Compiled.ServerTagsFilterTemplate == nil {
			continue
		}
		filter, err := subscriptionFilter(chOpts.Compiled.ServerTagsFilterTemplate, values, channel, opts.ServerTagsFilter)
		if err != nil {
			return err
		}
		opts.ServerTagsFilter = filter
		subs[channel] = opts
	}
	return nil
}

// channelFilter returns the server tags filter of a client subscription to channel which sets explicit, under the
// template of the channel's namespace. A channel without a template keeps explicit.
func (h *Handler) channelFilter(client Client, channel string, explicit *protocol.FilterNode) (*protocol.FilterNode, error) {
	chOpts, found, err := h.cfgContainer.ChannelOptionsRef(channel)
	if err != nil {
		return nil, err
	}
	if !found || chOpts.Compiled.ServerTagsFilterTemplate == nil {
		return explicit, nil
	}
	return subscriptionFilter(chOpts.Compiled.ServerTagsFilterTemplate, connectionFilterValues(client), channel, explicit)
}

// channelHasFilterTemplate reports whether the namespace of channel has a server tags filter template.
func (h *Handler) channelHasFilterTemplate(channel string) bool {
	chOpts, found, err := h.cfgContainer.ChannelOptionsRef(channel)
	return err == nil && found && chOpts.Compiled.ServerTagsFilterTemplate != nil
}

// subscriptionFilter renders tmpl with values and combines it with explicit. A missing value under reject is an
// error, which the caller turns into a refusal of the subscription. Under skip the explicit filter stands alone.
// The decision is logged with the name of the variable.
func subscriptionFilter(
	tmpl *filtertemplate.Template,
	values filtertemplate.Values,
	channel string,
	explicit *protocol.FilterNode,
) (*protocol.FilterNode, error) {
	rendered, err := tmpl.Render(values)
	if err == nil {
		return filtertemplate.And(rendered, explicit), nil
	}
	var missing filtertemplate.MissingValueError
	if !errors.As(err, &missing) {
		return nil, err
	}
	if tmpl.OnMissing() == filtertemplate.OnMissingSkip {
		if logging.Enabled(logging.DebugLevel) {
			log.Debug().Str("channel", channel).Str("client", values.Client).Str("user", values.User).Str("variable", missing.Variable).Msg("server tags filter template skipped, missing value")
		}
		return explicit, nil
	}
	log.Info().Str("channel", channel).Str("client", values.Client).Str("user", values.User).Str("variable", missing.Variable).Msg("server tags filter template refused subscription, missing value")
	return nil, err
}

// connectionFilterValues returns the values which the templates of a connection are rendered with: the claims
// stored on connect, and the user and client of the connection.
func connectionFilterValues(c Client) filtertemplate.Values {
	storage, release := c.AcquireStorage()
	claims, _ := storage[serverTagsFilterClaimsKey].(filtertemplate.Claims)
	release(storage)
	return filtertemplate.Values{Claims: claims, User: c.UserID(), Client: c.ID()}
}
