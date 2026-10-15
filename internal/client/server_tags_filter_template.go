package client

import (
	"errors"

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

// answerSubscribe answers a client subscription with reply, the result of OnSubscribe, after the server tags filter
// template of its channel is combined with the explicit filter. A missing value under reject refuses the subscription.
// The values are read once for the render. cb commits the subscription, and the values are compared again after it:
// a rebind which stored other values in the meantime either finds the channel in the channel map, or the comparison
// here does, so the channel never keeps a filter of values the connection no longer has.
func (h *Handler) answerSubscribe(
	client Client,
	channel string,
	reply centrifuge.SubscribeReply,
	err error,
	cb centrifuge.SubscribeCallback,
) {
	if err != nil || !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		cb(reply, err)
		return
	}
	tmpl := h.templateOf(channel)
	if tmpl == nil {
		cb(reply, nil)
		return
	}
	values := connectionFilterValues(client)
	filter, err := subscriptionFilter(tmpl, values, channel, reply.Options.ServerTagsFilter)
	if err != nil {
		cb(reply, centrifuge.ErrorPermissionDenied)
		return
	}
	reply.Options.ServerTagsFilter = filter
	cb(reply, nil)
	if !sameFilterValues(values, connectionFilterValues(client)) {
		client.Unsubscribe(channel, centrifuge.Unsubscribe{Code: centrifuge.UnsubscribeCodeInsufficient, Reason: "insufficient state"})
	}
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
// A template which refuses a token subscription is not an error of the refresh: the connection is disconnected with
// insufficient state, the client reconnects, and the connect is refused with DisconnectPermissionDenied, the code of a
// refused server-side subscription.
func (h *Handler) tokenSubscriptionsChanged(client *centrifuge.Client, extra RefreshExtra) bool {
	if h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		storage, release := client.AcquireStorage()
		stored, _ := storedFilterValues(storage)
		release(storage)
		if !stored.Claims.Equal(extra.FilterTemplateClaims) {
			return true
		}
		if err := h.templateSubscriptions(client, extra.Subs); err != nil {
			return true
		}
	}
	return tokenChannelsChanged(client, extra.Subs)
}

// storeFilterValues stores values as the values the templates of a connection are rendered with, before its user
// moves, and returns the values it had. A connection which stored none, one verified before the templates were
// configured, had its user and no claims. It stores nothing, and returns false, when no config has templates.
func (h *Handler) storeFilterValues(c rebindableClient, values filtertemplate.Values) (filtertemplate.Values, bool) {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return filtertemplate.Values{}, false
	}
	previous := connectionFilterValues(c)
	storage, release := c.AcquireStorage()
	storage[serverTagsFilterValuesKey] = values
	release(storage)
	return previous, true
}

// rebindFilterTemplates unsubscribes the channels with a template when the values of a connection changed from previous
// to values. The client resubscribes them with filters rendered with the new values. The user is one of the values: a
// connection without a user which takes one renders its templates differently even when the claims are the same.
func (h *Handler) rebindFilterTemplates(c rebindableClient, previous filtertemplate.Values, values filtertemplate.Values) {
	if sameFilterValues(previous, values) {
		return
	}
	for channel := range c.ChannelsWithContext() {
		if h.templateOf(channel) != nil {
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

// templateOf returns the server tags filter template of the namespace of channel, or nil when it has none. A channel of
// an unknown namespace has zero options, so it has no template. The lookup returns no error.
func (h *Handler) templateOf(channel string) *filtertemplate.Template {
	chOpts, _, _ := h.cfgContainer.ChannelOptionsRef(channel)
	return chOpts.Compiled.ServerTagsFilterTemplate
}

// subscriptionFilter renders tmpl with values and combines it with explicit. A missing value under reject is an
// error, which the caller turns into a refusal of the subscription. Under skip the explicit filter stands alone,
// but only for a missing value: any other error is a refusal. The decision is logged with the name of the variable.
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
	if errors.As(err, &missing) && tmpl.OnMissing() == filtertemplate.OnMissingSkip {
		if logging.Enabled(logging.DebugLevel) {
			log.Debug().Str("channel", channel).Str("client", values.Client).Str("user", values.User).Str("variable", missing.Variable).Msg("server tags filter template skipped, missing value")
		}
		return explicit, nil
	}
	log.Info().Err(err).Str("channel", channel).Str("client", values.Client).Str("user", values.User).Str("variable", missing.Variable).Msg("server tags filter template refused subscription")
	return nil, err
}

// connectionFilterValues returns the values which the templates of a connection are rendered with: the claims stored on
// connect or rebind, the user stored with them, and the client of the connection. The user is read from the storage, so
// a render which overlaps a rebind sees the values of one identity, and not the user of the rebind with the claims of the
// previous one. A connection which stored no values renders with its user and no claims.
func connectionFilterValues(c Client) filtertemplate.Values {
	storage, release := c.AcquireStorage()
	stored, ok := storedFilterValues(storage)
	release(storage)
	if !ok {
		stored.User = c.UserID()
	}
	return filtertemplate.Values{Claims: stored.Claims, User: stored.User, Client: c.ID()}
}

// storedFilterValues returns the values a connection stored for its server tags filter templates, and whether it
// stored any.
func storedFilterValues(storage map[string]any) (filtertemplate.Values, bool) {
	stored, ok := storage[serverTagsFilterValuesKey].(filtertemplate.Values)
	return stored, ok
}

// sameFilterValues reports whether two sets of values render every template the same: the same user, and the same
// value for every claim of either set.
func sameFilterValues(a filtertemplate.Values, b filtertemplate.Values) bool {
	return a.User == b.User && a.Claims.Equal(b.Claims)
}
