package client

import (
	"maps"

	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/jwtverify"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// rebindFilters are the values of the server tags filter templates which a rebind moves a connection between: the values
// of the new identity and the values the connection had. enabled is false when the config had no templates, the
// connection stored no values and no channel was recorded with a template when the rebind was checked; the rebind then
// reads the config again before it applies them, see reboundTemplateSubscriptions and commitRebindFilters. unstored is
// true when enabled and the connection stored no values: its previous values are unknown, see commitRebindFilters. subs are the subscriptions the rebind subscribes:
// the token subscriptions, and the server-side channels with a template which the connection holds and the new identity
// keeps. They are read before the rebind moves the connection.
type rebindFilters struct {
	enabled  bool
	unstored bool
	previous filtertemplate.Values
	values   filtertemplate.Values
	subs     map[string]centrifuge.SubscribeOptions
	// rendered is subs with the templates of the new identity applied, as checkReboundTemplates rendered them for the check
	// of the require option; nil when no template was rendered.
	rendered map[string]centrifuge.SubscribeOptions
	// dropped are the server-side channels which the connection holds and the require option drops from subs, see
	// requireReboundPrecheck. Their records are forgotten once the rebind is applied.
	dropped []string
}

// prepareRebindFilters returns the template values of a rebind of c to newUser with the claims of its token. The values
// the connection has are read here, before the rebind moves them. A connection which stored values keeps them on every
// rebind, also while the config has no templates: a reload which adds one must render the identity the connection has
// now, not the one it had when the templates were switched off. A connection which stored no values has no previous
// values, see commitRebindFilters.
func (h *Handler) prepareRebindFilters(c rebindableClient, claims filtertemplate.Claims, newUser string) rebindFilters {
	values := filtertemplate.Values{Claims: claims, User: newUser, Client: c.ID()}
	storage, release := c.AcquireStorage()
	stored, ok := storedFilterValues(storage)
	recorded, _ := storage[serverTagsFilterTemplatedKey].(map[string]struct{})
	recordedCount := len(recorded)
	release(storage)
	if !h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates() && !ok && recordedCount == 0 {
		return rebindFilters{values: values}
	}
	previous := filtertemplate.Values{Claims: stored.Claims, User: stored.User, Client: c.ID()}
	if !ok {
		previous.User = c.UserID()
	}
	return rebindFilters{
		enabled:  true,
		unstored: !ok,
		previous: previous,
		values:   values,
	}
}

// checkReboundFilters returns the template values and the subscriptions of a rebind of c to newUser. It refuses the rebind
// when a template of a subscription which the new identity gets has a missing value under reject, and disconnects the
// connection when it holds a server-side channel of a template namespace which connect did not store with its template,
// see untemplatedServerSide. The templates are rendered only to check them: the rebind renders them again once it is
// applied. It does nothing when no config has templates and the connection stored no values. The require option is
// checked last, see requireReboundPrecheck.
func (h *Handler) checkReboundFilters(
	c rebindableClient,
	verified jwtverify.ConnectToken,
	newUser string,
) (rebindFilters, error) {
	filters, err := h.checkReboundTemplates(c, verified, newUser)
	if err != nil {
		return rebindFilters{}, err
	}
	dropped, err := h.requireReboundPrecheck(c, filters.values, newUser, filters.subs, filters.rendered)
	if err != nil {
		return rebindFilters{}, err
	}
	for _, channel := range dropped {
		delete(filters.subs, channel)
	}
	filters.dropped = dropped
	return filters, nil
}

// checkReboundTemplates is the template part of checkReboundFilters.
func (h *Handler) checkReboundTemplates(
	c rebindableClient,
	verified jwtverify.ConnectToken,
	newUser string,
) (rebindFilters, error) {
	filters := h.prepareRebindFilters(c, verified.FilterTemplateClaims, newUser)
	filters.subs = verified.Subs
	if !filters.enabled {
		return filters, nil
	}
	serverSide := storedServerSideSubscriptions(c)
	if h.untemplatedServerSide(c, serverSide) {
		return rebindFilters{}, centrifuge.DisconnectInsufficientState
	}
	held := make(map[string]centrifuge.SubscribeOptions)
	for channel, opts := range serverSide {
		if _, ok := verified.Subs[channel]; ok || !c.IsSubscribed(channel) || !h.keepsServerSide(channel, opts, newUser) {
			continue
		}
		held[channel] = opts
	}
	if len(held) > 0 {
		filters.subs = make(map[string]centrifuge.SubscribeOptions, len(verified.Subs)+len(held))
		maps.Copy(filters.subs, verified.Subs)
		maps.Copy(filters.subs, held)
	}
	rendered, err := h.checkFilterTemplates(filters.values, filters.subs)
	if err != nil {
		return rebindFilters{}, err
	}
	filters.rendered = rendered
	if newUser != "" && h.cfgContainer.ConfigRef().Client.SubscribeToUserPersonalChannel.Enabled {
		personal := map[string]centrifuge.SubscribeOptions{h.cfgContainer.PersonalChannel(newUser): {}}
		if _, err := h.checkFilterTemplates(filters.values, personal); err != nil {
			return rebindFilters{}, err
		}
	}
	return filters, nil
}

// checkFilterTemplates returns subs with the templates of its namespaces applied to a clone, which subs is not changed by.
// It returns ErrorPermissionDenied when a template of subs refuses its subscription with values. The clone is what the
// require option checks, so the templates are rendered once for both.
func (h *Handler) checkFilterTemplates(
	values filtertemplate.Values,
	subs map[string]centrifuge.SubscribeOptions,
) (map[string]centrifuge.SubscribeOptions, error) {
	rendered := maps.Clone(subs)
	if err := h.applyFilterTemplates(rendered, values); err != nil {
		return nil, centrifuge.ErrorPermissionDenied
	}
	return rendered, nil
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
// rendered with the new values. The server-side channels which the rebind subscribes again, see reboundTemplateSubscriptions,
// come back with the new filters too. A server-side channel which connect did not store with a template does not get here:
// checkReboundFilters disconnects the connection for it. A reload which adds a template after checkReboundFilters looked at
// the connection leaves such a channel here: it has no template to render, so the rebind would unsubscribe it or keep it
// unfiltered, and the connection is disconnected with insufficient state instead. The rebind is applied already, and the
// disconnect releases what the connection holds. It is checked whether or not the connection stored values, since a reload
// can add a template to the namespace of any uni-connect channel of the connection.
func (h *Handler) commitRebindFilters(c rebindableClient, filters rebindFilters) error {
	templated := h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates()
	if !filters.enabled && !templated {
		return nil
	}
	if templated && h.untemplatedServerSide(c, storedServerSideSubscriptions(c)) {
		return centrifuge.DisconnectInsufficientState
	}
	storeFilterValues(c, filters.values)
	// A connection which stored no values has no previous values: its channels were subscribed without the values of a
	// template, or, when a reload added the template after its subscribe, with a fallback which is not a value it
	// had. Its templated channels are unsubscribed whenever the config has templates now, even when the values are the same.
	if filters.enabled && !filters.unstored && sameFilterValues(filters.previous, filters.values) {
		return nil
	}
	h.unsubscribeTemplated(c)
	return nil
}

// unsubscribeTemplated unsubscribes the channels of c whose filter may be rendered with the values the connection had: the
// channels recorded by recordTemplatedChannel, and the channels whose namespace has a template now. The record is needed
// after a reload which removed the template of a channel, when the config no longer shows that the channel has a filter
// from the template. The record is cleared, and the channels which the rebind subscribes again are recorded again.
func (h *Handler) unsubscribeTemplated(c rebindableClient) {
	channels := make(map[string]struct{})
	for _, channel := range templatedChannelsOf(c) {
		channels[channel] = struct{}{}
	}
	for channel := range c.ChannelsWithContext() {
		if h.templateOf(channel) != nil {
			channels[channel] = struct{}{}
		}
	}
	for channel := range channels {
		if c.IsSubscribed(channel) {
			c.Unsubscribe(channel, unsubscribeInsufficientState)
		}
	}
	clearTemplatedChannels(c)
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
	// The stored map is copied, not changed: tokenChannelsChanged reads the map it got from storage after the lock is released.
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

// reboundTemplateSubscriptions applies the server tags filter templates to the subscriptions of a rebind, rendered with the
// values the rebind stored in commitRebindFilters, and stores the filters of the connection token subscriptions. The
// config is read here, not when the rebind was checked: a reload which added a template in between must not leave a
// templated channel unfiltered. A refusal is checked before the rebind, so one found here means the config was reloaded
// since, and the connection is disconnected: the rebind is applied, and a disconnect releases what it holds.
func (h *Handler) reboundTemplateSubscriptions(c rebindableClient, toSubscribe map[string]centrifuge.SubscribeOptions) error {
	if err := h.templateSubscriptions(c, toSubscribe); err != nil {
		return centrifuge.DisconnectPermissionDenied
	}
	h.storeReboundTokenFilters(c, toSubscribe)
	return nil
}

// subscribeRestoredPersonal subscribes a connection to its personal channel when it is not subscribed to it, with the
// template of the channel rendered with the stored values. A channel the connection holds is not subscribed again, so its
// template is not rendered: a refresh is answered, and a claim value which the connection has no stored value for
// disconnects it, see tokenSubscriptionsChanged.
func (h *Handler) subscribeRestoredPersonal(c rebindableClient, channel string, opts centrifuge.SubscribeOptions) error {
	if c.IsSubscribed(channel) {
		return nil
	}
	toSubscribe := map[string]centrifuge.SubscribeOptions{channel: opts}
	templated := h.cfgContainer.ConfigRef().HasServerTagsFilterTemplates()
	if templated {
		if err := h.applyFilterTemplates(toSubscribe, connectionFilterValues(c)); err != nil {
			return centrifuge.ErrorPermissionDenied
		}
	}
	// The channel is recorded only once the refusal is passed, so a refused restore leaves no record of it.
	if err := h.refusesRestoredPersonal(c, toSubscribe); err != nil {
		return err
	}
	if templated {
		h.recordTemplatedChannel(c, channel)
	}
	return subscribeChannels(c, toSubscribe)
}
