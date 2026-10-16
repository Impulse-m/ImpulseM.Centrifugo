package client

import (
	"errors"
	"maps"

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
	values := filtertemplate.Values{Claims: verified.FilterTemplateClaims, Client: clientID}
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
	if err := h.applyFilterTemplates(subscriptions, values, false); err != nil {
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
	filter, err := h.channelFilter(client, channel, reply.ServerTagsFilter)
	if err != nil {
		return centrifuge.SubRefreshReply{}, centrifuge.ErrorPermissionDenied
	}
	reply.ServerTagsFilter = filter
	return reply, nil
}

// tokenSubscriptionsChanged reports whether the server-side subscriptions of a connection changed on refresh.
// A connection whose template claims changed is changed: its filters are fixed for its life, so it is
// disconnected. Otherwise the templates are applied to the subscriptions of the token before they are compared.
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
	if h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		storage, release := client.AcquireStorage()
		stored, _ := storedFilterValues(storage)
		unbound, _ := storage[serverTagsFilterUnboundKey].(bool)
		release(storage)
		if unbound {
			if err := h.applyFilterTemplates(extra.Subs, connectionFilterValues(client), true); err != nil {
				return true
			}
		} else {
			if !stored.Claims.Equal(extra.FilterTemplateClaims) {
				return true
			}
			if err := h.templateSubscriptions(client, extra.Subs); err != nil {
				return true
			}
		}
	}
	return tokenChannelsChanged(client, extra.Subs)
}

// rebindFilters are the values of the server tags filter templates which a rebind moves a connection between: the values
// of the new identity and the values the connection had. enabled is false when the config had no templates and the
// connection stored no values when the rebind was checked; the rebind then reads the config again before it applies
// them, see reboundTemplateSubscriptions and commitRebindFilters. serverSide are the server-side channels with a template
// which the connection holds and which the rebind subscribes again, read before the rebind moves the connection.
// unstored is true when enabled and the connection stored no values: its previous values are unknown, see
// commitRebindFilters.
type rebindFilters struct {
	enabled    bool
	unstored   bool
	previous   filtertemplate.Values
	values     filtertemplate.Values
	serverSide map[string]centrifuge.SubscribeOptions
}

// prepareRebindFilters returns the template values of a rebind of c to newUser with the claims of its token. The values
// the connection has are read here, before the rebind moves them. A connection which stored values keeps them on every
// rebind, also while the config has no templates: a reload which adds one must render the identity the connection has
// now, not the one it had when the templates were switched off. A connection which stored no values has no previous
// values, see commitRebindFilters.
func (h *Handler) prepareRebindFilters(c rebindableClient, claims filtertemplate.Claims, newUser string) rebindFilters {
	values := filtertemplate.Values{Claims: claims, User: newUser, Client: c.ID()}
	previous, stored := connectionFilterValuesStored(c)
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() && !stored {
		return rebindFilters{values: values}
	}
	return rebindFilters{
		enabled:  true,
		unstored: !stored,
		previous: previous,
		values:   values,
	}
}

// checkReboundFilters returns the template values of a rebind of c to newUser, and refuses the rebind when a template of
// a subscription which the new identity gets has a missing value under reject. The subscriptions are the personal
// channel, the token subscriptions and the server-side channels which the connection holds, read here before the rebind
// moves the connection. They are rendered only to check them: the rebind renders them again once it is applied. A
// connection which holds a server-side channel of a template namespace that connect did not store with its template is
// disconnected with insufficient state, see untemplatedServerSide. It does nothing when no config has templates and the
// connection stored no values.
func (h *Handler) checkReboundFilters(
	c rebindableClient,
	claims filtertemplate.Claims,
	subs map[string]centrifuge.SubscribeOptions,
	newUser string,
) (rebindFilters, error) {
	filters := h.prepareRebindFilters(c, claims, newUser)
	if !filters.enabled {
		return filters, nil
	}
	serverSide := storedServerSideSubscriptions(c)
	if h.untemplatedServerSide(c, serverSide) {
		return rebindFilters{}, centrifuge.DisconnectInsufficientState
	}
	for channel, opts := range serverSide {
		if !c.IsSubscribed(channel) || !h.keepsServerSide(channel, opts, newUser) {
			continue
		}
		if filters.serverSide == nil {
			filters.serverSide = make(map[string]centrifuge.SubscribeOptions)
		}
		filters.serverSide[channel] = opts
	}
	candidates := make(map[string]centrifuge.SubscribeOptions, len(subs)+len(filters.serverSide)+1)
	if newUser != "" && h.cfgContainer.ConfigRef().Client.SubscribeToUserPersonalChannel.Enabled {
		candidates[h.cfgContainer.PersonalChannel(newUser)] = centrifuge.SubscribeOptions{}
	}
	for channel, opts := range subs {
		if _, ok := candidates[channel]; !ok {
			candidates[channel] = opts
		}
	}
	for channel, opts := range filters.serverSide {
		if _, ok := candidates[channel]; !ok {
			candidates[channel] = opts
		}
	}
	if err := h.applyFilterTemplates(candidates, filters.values, false); err != nil {
		return rebindFilters{}, centrifuge.ErrorPermissionDenied
	}
	return filters, nil
}

// untemplatedServerSide reports whether c holds a uni-connect or connect proxy channel of a namespace with a template
// which is not in serverSide, the channels stored with their template on connect. Connect subscribed such a channel
// without a template, and no later rebind can give it one: it has no stored filter to render again. The rebind would
// leave it unsubscribed or unfiltered, so it is refused by a disconnect with insufficient state. The client reconnects,
// the connect proxy and the token run again, and every channel comes back with the template of the config.
func (h *Handler) untemplatedServerSide(c rebindableClient, serverSide map[string]centrifuge.SubscribeOptions) bool {
	for channel, chCtx := range c.ChannelsWithContext() {
		if chCtx.Source != subsource.UniConnect && chCtx.Source != subsource.ConnectProxy {
			continue
		}
		if _, ok := serverSide[channel]; ok {
			continue
		}
		if h.templateOf(channel) != nil {
			return true
		}
	}
	return false
}

// keepsServerSide reports whether a rebind to userID subscribes again a stored server-side channel of channel. The
// anonymous identity gets only the channels which keepsAnonymous allows, the rule of connect and of unsubscribeNotAnonymous,
// so a uni-connect channel which a rebind to anonymous removed is not subscribed again with a skip template.
func (h *Handler) keepsServerSide(channel string, opts centrifuge.SubscribeOptions, userID string) bool {
	return userID != "" || h.keepsAnonymous(channel, opts.Source)
}

// storeFilterValues stores the values as the values the templates of a connection are rendered with, after its user
// moved to the values. The values come from a token, so the connection is no longer unbound.
func storeFilterValues(c rebindableClient, values filtertemplate.Values) {
	storage, release := c.AcquireStorage()
	storage[serverTagsFilterValuesKey] = storedValuesOf(values)
	delete(storage, serverTagsFilterUnboundKey)
	release(storage)
}

// commitRebindFilters applies the template values of a rebind which Core applied: the values move to the new identity,
// and the channels with a template are unsubscribed when the values changed. The client resubscribes them with filters
// rendered with the new values. The server-side channels which the rebind subscribes again, see
// reboundTemplateSubscriptions, come back with the new filters too. A server-side channel which connect did not store
// with a template does not get here: checkReboundFilters disconnects the connection for it. A reload which adds a template
// after checkReboundFilters checked a connection without templates leaves such a channel here: it has no template to
// render, so the rebind would unsubscribe it, and the connection is disconnected with insufficient state instead. The
// rebind is applied already, and the disconnect releases what the connection holds.
func (h *Handler) commitRebindFilters(c rebindableClient, filters rebindFilters) error {
	if !filters.enabled && !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	if !filters.enabled && h.untemplatedServerSide(c, storedServerSideSubscriptions(c)) {
		return centrifuge.DisconnectInsufficientState
	}
	storeFilterValues(c, filters.values)
	// A connection which stored no values has no previous values: its channels were subscribed without the values of a
	// template, or, when a reload added the template after its subscribe, with a fallback which is not a value it
	// had. Its templated channels are unsubscribed whenever the config has templates now, even when the values are the same.
	if filters.enabled && !filters.unstored && sameFilterValues(filters.previous, filters.values) {
		return nil
	}
	for channel := range c.ChannelsWithContext() {
		if h.templateOf(channel) == nil {
			continue
		}
		c.Unsubscribe(channel, unsubscribeInsufficientState)
	}
	return nil
}

// storeReboundTokenFilters updates the stored filters of the connection token subscriptions which a rebind subscribes
// with the filters they are subscribed with, so that a refresh with the same claims compares them with the filters of
// their channels. Only the channels which are not subscribed yet are updated: a channel the connection keeps keeps the
// filter it has, as it did before. It must run after the templates are applied to toSubscribe, and before the channels
// are subscribed. It does nothing when no config has templates.
func (h *Handler) storeReboundTokenFilters(c rebindableClient, toSubscribe map[string]centrifuge.SubscribeOptions) {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return
	}
	var fresh []string
	for channel, opts := range toSubscribe {
		if opts.Source == subsource.ConnectionToken && !c.IsSubscribed(channel) {
			fresh = append(fresh, channel)
		}
	}
	if len(fresh) == 0 {
		return
	}
	storage, release := c.AcquireStorage()
	// The stored map is read without the lock by the comparison of tokenChannelsChanged, so it is copied, not changed.
	stored, _ := storage[connectionTokenFiltersKey].(map[string]*protocol.FilterNode)
	filters := maps.Clone(stored)
	for _, channel := range fresh {
		filter := toSubscribe[channel].ServerTagsFilter
		if filter == nil {
			delete(filters, channel)
			continue
		}
		if filters == nil {
			filters = make(map[string]*protocol.FilterNode)
		}
		filters[channel] = filter
	}
	if len(filters) == 0 {
		delete(storage, connectionTokenFiltersKey)
	} else {
		storage[connectionTokenFiltersKey] = filters
	}
	release(storage)
}

// reboundTemplateSubscriptions completes toSubscribe, the subscriptions of a rebind, with the server-side channels with a
// template which checkReboundFilters read, and applies the templates to all of them with the values of the new identity.
// The config is read here, not when the rebind was checked: a reload which added a template in between must not leave
// a templated channel unfiltered. The stored filters of the token subscriptions are updated after that, see
// storeReboundTokenFilters. A refusal is checked before the rebind, so one found here means the config was reloaded since,
// and the connection is disconnected: the rebind is applied, and a disconnect releases what it holds.
func (h *Handler) reboundTemplateSubscriptions(
	c rebindableClient,
	toSubscribe map[string]centrifuge.SubscribeOptions,
	filters rebindFilters,
) error {
	if !filters.enabled && !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	for channel, opts := range filters.serverSide {
		if _, ok := toSubscribe[channel]; !ok && h.keepsServerSide(channel, opts, filters.values.User) {
			toSubscribe[channel] = opts
		}
	}
	if err := h.applyFilterTemplates(toSubscribe, filters.values, false); err != nil {
		return centrifuge.DisconnectPermissionDenied
	}
	h.storeReboundTokenFilters(c, toSubscribe)
	return nil
}

// templateSubscriptions applies the server tags filter templates to subscriptions made on a connection after
// its connect, such as the ones of a rebind. A missing value under reject refuses them.
func (h *Handler) templateSubscriptions(c Client, subs map[string]centrifuge.SubscribeOptions) error {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() {
		return nil
	}
	if err := h.applyFilterTemplates(subs, connectionFilterValues(c), false); err != nil {
		return centrifuge.ErrorPermissionDenied
	}
	return nil
}

// applyFilterTemplates sets the server tags filter of every subscription whose namespace has a template to the
// template combined with the explicit filter of the subscription. It returns the error of the first missing value
// under reject. With dropRefused, such a subscription is removed from subs instead, and no error is returned: an
// unbound connection must not subscribe it without its template.
func (h *Handler) applyFilterTemplates(
	subs map[string]centrifuge.SubscribeOptions,
	values filtertemplate.Values,
	dropRefused bool,
) error {
	for channel, opts := range subs {
		filtered, err := h.applyFilterTemplate(channel, opts, values)
		if err != nil {
			if !dropRefused {
				return err
			}
			delete(subs, channel)
			continue
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
	tmpl := h.templateOf(channel)
	if tmpl == nil {
		return opts, nil
	}
	filter, err := subscriptionFilter(tmpl, values, channel, opts.ServerTagsFilter)
	if err != nil {
		return centrifuge.SubscribeOptions{}, err
	}
	opts.ServerTagsFilter = filter
	return opts, nil
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
// but only for a missing value: any other error is a refusal. The decision is logged, with the name of the variable
// when the error is a missing value.
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
	if isMissing && tmpl.OnMissing() == filtertemplate.OnMissingSkip {
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

// connectionFilterValues returns the values which the templates of a connection are rendered with: the claims stored on
// connect or rebind, the user stored with them, and the client of the connection. The user is read from the storage, so
// a render which overlaps a rebind sees the values of one identity, and not the user of the rebind with the claims of the
// previous one. A connection which stored no values renders with its user and no claims.
func connectionFilterValues(c Client) filtertemplate.Values {
	values, _ := connectionFilterValuesStored(c)
	return values
}

// connectionFilterValuesStored returns the values of connectionFilterValues, read in one storage access, and whether the
// connection stored values for its templates.
func connectionFilterValuesStored(c Client) (filtertemplate.Values, bool) {
	storage, release := c.AcquireStorage()
	stored, ok := storedFilterValues(storage)
	release(storage)
	if !ok {
		stored.User = c.UserID()
	}
	return filtertemplate.Values{Claims: stored.Claims, User: stored.User, Client: c.ID()}, ok
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
