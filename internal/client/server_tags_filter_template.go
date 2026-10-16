package client

import (
	"errors"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/logging"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/rs/zerolog/log"
)

// serverTagsFilterValuesKey is a client storage key with the values the server tags filter templates of a connection
// are rendered with: the claims of its token and its user, as they were when the connection was verified or rebound.
// The client of the connection is not stored, it is read from the connection.
const serverTagsFilterValuesKey = "server_tags_filter_template_values"

// serverTagsFilterServerSideKey is a client storage key with the subscriptions which connect made to server-side
// channels of uni-connect and connect proxy sources, when their namespace has a template. A rebind to another user
// subscribes them again, since the connection keeps them and their filters are rendered with the new values.
const serverTagsFilterServerSideKey = "server_tags_filter_template_server_side"

// serverTagsFilterTemplatedKey is a client storage key with the set of channels which the connection subscribed with a
// server tags filter template, see recordTemplatedChannel. A rebind with other values unsubscribes them: a reload may have
// removed their template, and the config does not tell which channels carry a template filter.
const serverTagsFilterTemplatedKey = "server_tags_filter_template_channels"

// serverTagsFilterUnboundKey is a client storage key set to true when the connection has no token claims bound to its
// stored values: it connected through the connect proxy, which has no token. Its claims stay missing until it
// reconnects with a token, so a token refresh does not compare them. A rebind with a token binds claims and removes it.
const serverTagsFilterUnboundKey = "server_tags_filter_template_unbound"

// unsubscribeInsufficientState is the unsubscribe of a channel whose filter was rendered with values which the
// connection no longer has. The client resubscribes it.
var unsubscribeInsufficientState = centrifuge.Unsubscribe{Code: centrifuge.UnsubscribeCodeInsufficient, Reason: "insufficient state"}

// connectFilterTemplates applies the server tags filter templates to the subscriptions of a connection. The
// values of the connection are stored, so that later subscriptions and refreshes render the same filters. They are
// stored even when all of them are missing, so a refresh compares them with the values of its token.
// A missing value of a template under reject refuses the connection. A tokenless connection (connect proxy) is marked
// unbound, see serverTagsFilterUnboundKey. The subscriptions are the map of the verified token, owned by the call: the
// templates are applied to it in place, and storeConnectionTokenFilters stores the combined filters from it. The config
// is read here, not from the snapshot of the connect: a reload during the connect proxy call adds a template which
// the subscriptions of the proxy must get.
func (h *Handler) connectFilterTemplates(
	clientID string,
	credentials *centrifuge.Credentials,
	verified jwtverify.ConnectToken,
	tokenless bool,
	subscriptions map[string]centrifuge.SubscribeOptions,
	storage map[string]any,
) error {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	values := filtertemplate.Values{Claims: verified.FilterTemplateClaims, Client: clientID, Unbound: tokenless}
	if credentials != nil {
		values.User = credentials.UserID
	}
	storage[serverTagsFilterValuesKey] = storedValuesOf(values)
	if tokenless {
		storage[serverTagsFilterUnboundKey] = true
	}
	if serverSide := h.serverSideSubscriptions(subscriptions); serverSide != nil {
		storage[serverTagsFilterServerSideKey] = serverSide
	}
	if templated := h.templatedSubscriptionChannels(subscriptions); templated != nil {
		storage[serverTagsFilterTemplatedKey] = templated
	}
	if err := h.applyFilterTemplates(subscriptions, values); err != nil {
		return centrifuge.DisconnectPermissionDenied
	}
	return nil
}

// serverSideSubscriptions returns the subscriptions of the uni-connect and connect proxy channels of subs which have a
// template. They are taken before the templates are applied, so their explicit filter is kept and the template is not:
// a rebind renders the template with its own values. It returns nil when there are none.
func (h *Handler) serverSideSubscriptions(subs map[string]centrifuge.SubscribeOptions) map[string]centrifuge.SubscribeOptions {
	var serverSide map[string]centrifuge.SubscribeOptions
	for channel, opts := range subs {
		if (opts.Source != subsource.UniConnect && opts.Source != subsource.ConnectProxy) || h.templateOf(channel) == nil {
			continue
		}
		if serverSide == nil {
			serverSide = make(map[string]centrifuge.SubscribeOptions)
		}
		serverSide[channel] = opts
	}
	return serverSide
}

// answerSubscribe answers a client subscription with reply, the result of OnSubscribe, after the server tags filter
// template of its channel is combined with the explicit filter. A missing value under reject refuses the subscription.
// The values are read once for the render. cb commits the subscription, and the values are compared again after it:
// a rebind which stored other values in the meantime either finds the channel in the channel map, or the comparison
// here does, so the channel never keeps a filter of values the connection no longer has. The commit of cb is complete
// when it returns, so the unsubscribe does not wait for the subscribe. The window between the commit and that
// unsubscribe is accepted: the channel can deliver publications which the filter of the previous identity allowed,
// and only until the unsubscribe.
func (h *Handler) answerSubscribe(
	client Client,
	channel string,
	reply centrifuge.SubscribeReply,
	err error,
	cb centrifuge.SubscribeCallback,
) {
	if err != nil || !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() || h.templateOf(channel) == nil {
		cb(reply, err)
		return
	}
	values := connectionFilterValues(client)
	filter, err := h.filterWithValues(channel, values, reply.Options.ServerTagsFilter)
	if err != nil {
		cb(reply, centrifuge.ErrorPermissionDenied)
		return
	}
	reply.Options.ServerTagsFilter = filter
	h.recordTemplatedChannel(client, channel)
	cb(reply, nil)
	if !sameFilterValues(values, connectionFilterValues(client)) {
		client.Unsubscribe(channel, unsubscribeInsufficientState)
	}
}

// refreshFilterTemplate applies the template of the channel to the reply of a subscription refresh with err, the result
// of OnSubRefresh. A refresh with a new explicit filter keeps the template, so the filter is combined with it. A refresh
// without a filter keeps the current filter of the subscription, see Core, and an error is returned as it is.
func (h *Handler) refreshFilterTemplate(
	client Client,
	channel string,
	reply centrifuge.SubRefreshReply,
	err error,
) (centrifuge.SubRefreshReply, error) {
	if err != nil || reply.ServerTagsFilter == nil || !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return reply, err
	}
	filter, err := h.filterWithValues(channel, connectionFilterValues(client), reply.ServerTagsFilter)
	if err != nil {
		return centrifuge.SubRefreshReply{}, centrifuge.ErrorPermissionDenied
	}
	reply.ServerTagsFilter = filter
	h.recordTemplatedChannel(client, channel)
	return reply, nil
}

// tokenSubscriptionsChanged reports whether the server-side subscriptions of a connection changed on refresh.
// A connection whose template claims changed is changed: its filters are fixed for its life, so it is
// disconnected. A connection which stored no values while the config has templates is changed too, since a reload
// added them after its connect and its values are unknown; its reconnect binds them. A connection which stored values
// is compared while the config has no templates too: a reload which removed them leaves its channels filtered with the
// claims it had, and the refresh carries no claims to render them again, so a stored connection is disconnected then,
// once, and its reconnect stores no values. Otherwise the templates are applied to the subscriptions of the token before
// they are compared.
// The claim check is the primary rule, and the comparison of the filters in tokenChannelsChanged is the second.
// A bound connection whose template refuses a token subscription is disconnected with insufficient state: the client
// reconnects, and the connect is refused with DisconnectPermissionDenied, the code of a refused server-side
// subscription. An unbound connection (connect proxy) keeps its missing claims, so its refresh does not compare them,
// and a refused token subscription is dropped from the refresh instead: it is not subscribed unfiltered. The refresh is
// answered when the dropped channel is not subscribed. A channel which the connection still holds is found by
// tokenChannelsChanged, so the connection is disconnected with insufficient state, and its reconnect does not subscribe
// the channel without the template. extra.Subs is the map of the refresh, owned by the call, and the templates are
// applied to it in place.
func (h *Handler) tokenSubscriptionsChanged(client *centrifuge.Client, extra RefreshExtra) bool {
	templated := h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates()
	storage, release := client.AcquireStorage()
	stored, stores := storedFilterValues(storage)
	unbound, _ := storage[serverTagsFilterUnboundKey].(bool)
	release(storage)
	if !templated && !stores {
		return tokenChannelsChanged(client, extra.Subs)
	}
	if unbound {
		h.dropRefusedFilterTemplates(extra.Subs, connectionFilterValues(client))
		return tokenChannelsChanged(client, extra.Subs)
	}
	if !stores {
		return true
	}
	if !stored.Claims.Equal(extra.FilterTemplateClaims) {
		return true
	}
	if templated {
		if err := h.templateSubscriptions(client, extra.Subs); err != nil {
			return true
		}
	}
	return tokenChannelsChanged(client, extra.Subs)
}

// templateSubscriptions applies the server tags filter templates to subscriptions made on a connection after
// its connect, such as the ones of a rebind. A missing value under reject refuses them.
func (h *Handler) templateSubscriptions(c Client, subs map[string]centrifuge.SubscribeOptions) error {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	if err := h.applyFilterTemplates(subs, connectionFilterValues(c)); err != nil {
		return err
	}
	for channel := range subs {
		h.recordTemplatedChannel(c, channel)
	}
	return nil
}

// applyFilterTemplates sets the server tags filter of every subscription whose namespace has a template to the
// template combined with the explicit filter of the subscription. It returns the error of the first missing value
// under reject.
func (h *Handler) applyFilterTemplates(subs map[string]centrifuge.SubscribeOptions, values filtertemplate.Values) error {
	return h.applyFilterTemplatesTo(subs, values, false)
}

// dropRefusedFilterTemplates is applyFilterTemplates for an unbound connection: a subscription which a template refuses
// is removed from subs instead of failing the call, so an unbound connection must not subscribe it without its template.
func (h *Handler) dropRefusedFilterTemplates(subs map[string]centrifuge.SubscribeOptions, values filtertemplate.Values) {
	// Dropping a refused subscription never fails, so the error is always nil.
	_ = h.applyFilterTemplatesTo(subs, values, true)
}

// applyFilterTemplatesTo applies the templates of subs as applyFilterTemplates does. A refused subscription is removed from
// subs when dropRefused is set, and the call fails with the first refusal otherwise.
func (h *Handler) applyFilterTemplatesTo(
	subs map[string]centrifuge.SubscribeOptions,
	values filtertemplate.Values,
	dropRefused bool,
) error {
	for channel, opts := range subs {
		filtered, err := h.applyFilterTemplate(channel, opts, values)
		if err != nil {
			if dropRefused {
				delete(subs, channel)
				continue
			}
			return err
		}
		subs[channel] = filtered
	}
	return nil
}

// applyFilterTemplate returns the subscription opts with the template of channel combined with its explicit filter. A
// channel without a template keeps opts as they are.
func (h *Handler) applyFilterTemplate(
	channel string,
	opts centrifuge.SubscribeOptions,
	values filtertemplate.Values,
) (centrifuge.SubscribeOptions, error) {
	filter, err := h.filterWithValues(channel, values, opts.ServerTagsFilter)
	if err != nil {
		return centrifuge.SubscribeOptions{}, err
	}
	opts.ServerTagsFilter = filter
	return opts, nil
}

// filterWithValues returns the server tags filter of a subscription to channel with explicit, rendered under the template
// of the channel's namespace with values. A channel without a template keeps explicit.
func (h *Handler) filterWithValues(
	channel string,
	values filtertemplate.Values,
	explicit *protocol.FilterNode,
) (*protocol.FilterNode, error) {
	tmpl := h.templateOf(channel)
	if tmpl == nil {
		return explicit, nil
	}
	return subscriptionFilter(tmpl, values, channel, explicit)
}

// refusesHistory returns the error of a history call on a channel with chOpts. The template filters the publications of a
// subscription, and history reads publications without the subscription, so a channel with a template refuses it.
// Recovery is not refused: it applies the filter of the subscription.
func refusesHistory(chOpts *configtypes.ChannelOptions) error {
	if chOpts.HasServerTagsFilterTemplate() {
		return centrifuge.ErrorPermissionDenied
	}
	return nil
}

// templateOf returns the server tags filter template of the namespace of channel, or nil when it has none. A channel of
// an unknown namespace has zero options, so it has no template. The lookup returns no error.
func (h *Handler) templateOf(channel string) *filtertemplate.Template {
	chOpts, _, _ := h.cfgContainer.ChannelOptionsRef(channel)
	return chOpts.Compiled.ServerTagsFilterTemplate
}

// subscriptionFilter renders tmpl with values and combines it with explicit. A missing value under reject is an
// error, which the caller turns into a refusal of the subscription. Under skip the explicit filter stands alone,
// but only for a missing value: any other error is a refusal, and so is a missing claim of an unbound connection, see
// unboundClaimMissing. The decision is logged, with the name of the variable when the error is a missing value.
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
	isMissing := errors.As(err, &missing)
	if isMissing && tmpl.OnMissing() == filtertemplate.OnMissingSkip && !unboundClaimMissing(values, missing) {
		if logging.Enabled(logging.DebugLevel) {
			log.Debug().Str("channel", channel).Str("client", values.Client).Str("user", values.User).Str("variable", missing.Variable).Msg("server tags filter template skipped, missing value")
		}
		return explicit, nil
	}
	event := log.Info().Err(err).Str("channel", channel).Str("client", values.Client).Str("user", values.User)
	if isMissing {
		event = event.Str("variable", missing.Variable)
	}
	event.Msg("server tags filter template refused subscription")
	return nil, err
}

// unboundClaimMissing reports whether a missing value is a claim of a connection which has no token claims bound. The
// client chooses the connect path which leaves them unbound, so under skip such a claim is still refused: skipping the
// template would deliver the subscription without it.
func unboundClaimMissing(values filtertemplate.Values, missing filtertemplate.MissingValueError) bool {
	return values.Unbound && strings.HasPrefix(missing.Variable, "claims.")
}

// connectionFilterValues returns the values which the templates of a connection are rendered with: the claims stored on
// connect or rebind, the user stored with them, the client of the connection, and whether the claims are unbound. The
// user is read from the storage, so a render which overlaps a rebind sees the values of one identity, and not the user of
// the rebind with the claims of the previous one. A connection which stored no values renders with its user and no claims.
func connectionFilterValues(c Client) filtertemplate.Values {
	storage, release := c.AcquireStorage()
	stored, ok := storedFilterValues(storage)
	unbound, _ := storage[serverTagsFilterUnboundKey].(bool)
	release(storage)
	if !ok {
		stored.User = c.UserID()
	}
	return filtertemplate.Values{Claims: stored.Claims, User: stored.User, Client: c.ID(), Unbound: unbound}
}

// storedFilterValues returns the values a connection stored for its server tags filter templates, and whether it
// stored any.
func storedFilterValues(storage map[string]any) (filtertemplate.Values, bool) {
	stored, ok := storage[serverTagsFilterValuesKey].(filtertemplate.Values)
	return stored, ok
}

// storedValuesOf returns the part of values which is stored: the user and the claims. The client is not stored.
func storedValuesOf(values filtertemplate.Values) filtertemplate.Values {
	return filtertemplate.Values{Claims: values.Claims, User: values.User}
}

// templatedSubscriptionChannels returns the channels of subs whose namespace has a template, or nil when there are none.
func (h *Handler) templatedSubscriptionChannels(subs map[string]centrifuge.SubscribeOptions) map[string]struct{} {
	var channels map[string]struct{}
	for channel := range subs {
		if h.templateOf(channel) == nil {
			continue
		}
		if channels == nil {
			channels = make(map[string]struct{})
		}
		channels[channel] = struct{}{}
	}
	return channels
}

// recordTemplatedChannel records that channel is subscribed with a template of its namespace, when it has one. The set
// is changed under the storage lock, and it is read by templatedChannelsOf under the same lock.
func (h *Handler) recordTemplatedChannel(c Client, channel string) {
	if h.templateOf(channel) == nil {
		return
	}
	storage, release := c.AcquireStorage()
	channels, _ := storage[serverTagsFilterTemplatedKey].(map[string]struct{})
	if channels == nil {
		channels = make(map[string]struct{})
		storage[serverTagsFilterTemplatedKey] = channels
	}
	channels[channel] = struct{}{}
	release(storage)
}

// forgetTemplatedChannel removes channel from the record of recordTemplatedChannel when the connection unsubscribes it, so
// the record holds only the channels which the connection still subscribes. With no templates in the config nothing is
// recorded by a new subscribe, so the lock is not taken then. A channel recorded before a reload removed its template
// stays in the record until the connection rebinds or closes.
func (h *Handler) forgetTemplatedChannel(c Client, channel string) {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return
	}
	storage, release := c.AcquireStorage()
	channels, _ := storage[serverTagsFilterTemplatedKey].(map[string]struct{})
	delete(channels, channel)
	release(storage)
}

// templatedChannelsOf returns the channels recorded by recordTemplatedChannel, as a copy.
func templatedChannelsOf(c Client) []string {
	storage, release := c.AcquireStorage()
	channels, _ := storage[serverTagsFilterTemplatedKey].(map[string]struct{})
	out := make([]string, 0, len(channels))
	for channel := range channels {
		out = append(out, channel)
	}
	release(storage)
	return out
}

// clearTemplatedChannels removes the record of recordTemplatedChannel.
func clearTemplatedChannels(c Client) {
	storage, release := c.AcquireStorage()
	delete(storage, serverTagsFilterTemplatedKey)
	release(storage)
}

// storedServerSideSubscriptions returns the server-side subscriptions with a template which a connection stored on connect.
func storedServerSideSubscriptions(c Client) map[string]centrifuge.SubscribeOptions {
	storage, release := c.AcquireStorage()
	serverSide, _ := storage[serverTagsFilterServerSideKey].(map[string]centrifuge.SubscribeOptions)
	release(storage)
	return serverSide
}

// sameFilterValues reports whether two sets of values render every template the same: the same user, and the same
// value for every claim of either set.
func sameFilterValues(a filtertemplate.Values, b filtertemplate.Values) bool {
	return a.User == b.User && a.Claims.Equal(b.Claims)
}
