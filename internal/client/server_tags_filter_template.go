package client

import (
	"errors"
	"maps"

	"github.com/centrifugal/centrifugo/v6/internal/config"
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

// unsubscribeInsufficientState is the unsubscribe of a channel whose filter was rendered with values which the
// connection no longer has. The client resubscribes it.
var unsubscribeInsufficientState = centrifuge.Unsubscribe{Code: centrifuge.UnsubscribeCodeInsufficient, Reason: "insufficient state"}

// connectFilterTemplates applies the server tags filter templates to the subscriptions of a connection. The
// values of the connection are stored, so that later subscriptions and refreshes render the same filters. They are
// stored even when all of them are missing, so a refresh compares them with the values of its token.
// A missing value of a template under reject refuses the connection. The subscriptions are the map of the verified token,
// owned by the call: the templates are applied to it in place, and storeConnectionTokenFilters stores the combined filters
// from it.
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
	storage[serverTagsFilterValuesKey] = storedValuesOf(values)
	if serverSide := h.serverSideSubscriptions(subscriptions); serverSide != nil {
		storage[serverTagsFilterServerSideKey] = serverSide
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
// refused server-side subscription. extra.Subs is the map of the refresh, owned by the call, and the templates are
// applied to it in place.
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

// rebindFilters are the values of the server tags filter templates which a rebind moves a connection between: the values
// of the new identity and the values the connection had. enabled is false when no config has templates, and then the
// rebind does nothing with them.
type rebindFilters struct {
	enabled  bool
	previous filtertemplate.Values
	values   filtertemplate.Values
}

// prepareRebindFilters returns the template values of a rebind of c to newUser with the claims of its token. The values
// the connection has are read here, before the rebind moves them. A connection which stored values keeps them on every
// rebind, also while the config has no templates: a reload which adds one must render the identity the connection has
// now, not the one it had when the templates were switched off.
func (h *Handler) prepareRebindFilters(c rebindableClient, claims filtertemplate.Claims, newUser string) rebindFilters {
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() && !hasStoredFilterValues(c) {
		return rebindFilters{}
	}
	return rebindFilters{
		enabled:  true,
		previous: connectionFilterValues(c),
		values:   filtertemplate.Values{Claims: claims, User: newUser, Client: c.ID()},
	}
}

// storeFilterValues stores the values as the values the templates of a connection are rendered with, after its user
// moved to the values.
func storeFilterValues(c rebindableClient, values filtertemplate.Values) {
	storage, release := c.AcquireStorage()
	storage[serverTagsFilterValuesKey] = storedValuesOf(values)
	release(storage)
}

// commitRebindFilters applies the template values of a rebind which Core applied: the values move to the new identity,
// and the channels with a template are unsubscribed when the values changed. The client resubscribes them with filters
// rendered with the new values. The server-side channels which the rebind subscribes again, see
// reboundTemplateSubscriptions, come back with the new filters too.
func (h *Handler) commitRebindFilters(c rebindableClient, filters rebindFilters) {
	if !filters.enabled {
		return
	}
	storeFilterValues(c, filters.values)
	if sameFilterValues(filters.previous, filters.values) {
		return
	}
	serverSide := storedServerSideSubscriptions(c)
	for channel, chCtx := range c.ChannelsWithContext() {
		if h.templateOf(channel) == nil {
			continue
		}
		// A server-side channel which connect subscribed without a template has no filter which depends on the values,
		// so it keeps its filter. Only the ones stored on connect are subscribed again with the new values.
		isServerSide := chCtx.Source == subsource.UniConnect || chCtx.Source == subsource.ConnectProxy
		if _, stored := serverSide[channel]; isServerSide && !stored {
			continue
		}
		c.Unsubscribe(channel, unsubscribeInsufficientState)
	}
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

// reboundTemplateSubscriptions completes toSubscribe, the subscriptions of a rebind to userID, with the server-side
// channels with a template which the connection holds from its connect, and applies the templates to all of them with
// the values of the new identity. The server-side channels are kept only with a user, since a move to an anonymous
// identity unsubscribes them, see unsubscribeNotAnonymous. A missing value under reject is an error of the refresh.
func (h *Handler) reboundTemplateSubscriptions(
	c rebindableClient,
	userID string,
	toSubscribe map[string]centrifuge.SubscribeOptions,
	filters rebindFilters,
) error {
	if !filters.enabled {
		return nil
	}
	if userID != "" {
		for channel, opts := range storedServerSideSubscriptions(c) {
			if _, ok := toSubscribe[channel]; !ok && c.IsSubscribed(channel) {
				toSubscribe[channel] = opts
			}
		}
	}
	if err := h.applyFilterTemplates(toSubscribe, filters.values); err != nil {
		return centrifuge.ErrorPermissionDenied
	}
	return nil
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

// hasStoredFilterValues reports whether a connection stored values for its server tags filter templates.
func hasStoredFilterValues(c Client) bool {
	storage, release := c.AcquireStorage()
	_, ok := storedFilterValues(storage)
	release(storage)
	return ok
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
