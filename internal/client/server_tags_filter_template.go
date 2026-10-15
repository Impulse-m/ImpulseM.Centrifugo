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

// serverTagsFilterValuesKey is a client storage key with the values the server tags filter templates of a connection
// are rendered with: the claims of its token and its user, as they were when the connection was verified or rebound.
const serverTagsFilterValuesKey = "server_tags_filter_template_values"

// connectFilterTemplates applies the server tags filter templates to the subscriptions of a connection. The
// values of the connection are stored, so that later subscriptions and refreshes render the same filters. They are
// stored even when all of them are missing, so a refresh compares them with the values of its token.
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
	storage[serverTagsFilterValuesKey] = values
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
// The claim check is the primary rule, and the comparison of the filters in tokenChannelsChanged is the second.
func (h *Handler) tokenSubscriptionsChanged(client *centrifuge.Client, extra RefreshExtra) bool {
	if h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		storage, release := client.AcquireStorage()
		stored := storedFilterValues(storage)
		release(storage)
		if !sameClaimValues(stored.Claims, extra.FilterTemplateClaims) {
			return true
		}
		if err := h.templateSubscriptions(client, extra.Subs); err != nil {
			return true
		}
	}
	return tokenChannelsChanged(client, extra.Subs)
}

// rebindFilterTemplates stores the values of a connection which was rebound. When they render a template differently,
// the channels with a template are unsubscribed, so the client resubscribes them with filters rendered with the new
// values. The user is one of the values: a connection without a user which takes one renders its templates differently
// even when the claims are the same.
func (h *Handler) rebindFilterTemplates(c rebindableClient, values filtertemplate.Values) {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return
	}
	storage, release := c.AcquireStorage()
	stored := storedFilterValues(storage)
	if sameFilterValues(stored, values) {
		release(storage)
		return
	}
	storage[serverTagsFilterValuesKey] = values
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
		tmpl := h.templateOf(channel)
		if tmpl == nil {
			continue
		}
		filter, err := subscriptionFilter(tmpl, values, channel, opts.ServerTagsFilter)
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
	tmpl := h.templateOf(channel)
	if tmpl == nil {
		return explicit, nil
	}
	return subscriptionFilter(tmpl, connectionFilterValues(client), channel, explicit)
}

// channelHasFilterTemplate reports whether the namespace of channel has a server tags filter template.
func (h *Handler) channelHasFilterTemplate(channel string) bool {
	return h.templateOf(channel) != nil
}

// templateOf returns the server tags filter template of the namespace of channel, or nil when it has none. A channel of
// an unknown namespace has zero options, so it has no template. The lookup returns no error.
func (h *Handler) templateOf(channel string) *filtertemplate.Template {
	chOpts, _, _ := h.cfgContainer.ChannelOptionsRef(channel)
	return chOpts.Compiled.ServerTagsFilterTemplate
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
		log.Error().Err(err).Str("channel", channel).Msg("server tags filter template failed to render")
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
// stored on connect or rebind, and the user and client of the connection.
func connectionFilterValues(c Client) filtertemplate.Values {
	storage, release := c.AcquireStorage()
	stored := storedFilterValues(storage)
	release(storage)
	return filtertemplate.Values{Claims: stored.Claims, User: c.UserID(), Client: c.ID()}
}

// storedFilterValues returns the values a connection stored for its server tags filter templates, or the zero value
// when it stored none.
func storedFilterValues(storage map[string]any) filtertemplate.Values {
	stored, _ := storage[serverTagsFilterValuesKey].(filtertemplate.Values)
	return stored
}

// sameFilterValues reports whether two sets of values render every template the same: the same user, and the same
// value for every claim of either set.
func sameFilterValues(a filtertemplate.Values, b filtertemplate.Values) bool {
	return a.User == b.User && sameClaimValues(a.Claims, b.Claims)
}

// sameClaimValues reports whether two sets of claims have the same value for every claim path of either set. A
// missing claim has the value "", so a claim which is missing and one which is empty are the same. The paths of
// the sets may differ, for example after a config reload, so the claims are compared by name and not by position.
func sameClaimValues(a filtertemplate.Claims, b filtertemplate.Claims) bool {
	for _, path := range a.Paths {
		if claimValue(a, path) != claimValue(b, path) {
			return false
		}
	}
	for _, path := range b.Paths {
		if claimValue(a, path) != claimValue(b, path) {
			return false
		}
	}
	return true
}

// claimValue returns the value of the claim at path, or "" when the claims have none.
func claimValue(claims filtertemplate.Claims, path string) string {
	i := slices.Index(claims.Paths, path)
	if i < 0 || i >= len(claims.Values) {
		return ""
	}
	return claims.Values[i]
}
