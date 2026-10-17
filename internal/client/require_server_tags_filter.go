package client

import (
	"maps"

	"github.com/centrifugal/centrifugo/v6/internal/filterrequire"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
	"github.com/centrifugal/centrifugo/v6/internal/subsource"

	"github.com/centrifugal/centrifuge"
)

// requireSubscribeFilter refuses a client subscription without a server tags filter in a namespace which requires one.
// reply and err are the result of OnSubscribe, with the template of the channel combined in. An error is returned as is.
// On a refusal *reply is reset to the zero reply, keeping only SubscriptionReady: Core closes it from the reply it is
// given, so a reply which lost it would leave a stream proxy callback waiting on it. The subscribe stream proxy never
// reaches a refusal here, the config refuses the option with it, this keeps the invariant if that rule is relaxed.
func (h *Handler) requireSubscribeFilter(c Client, channel string, reply *centrifuge.SubscribeReply, err error) error {
	if err != nil || !h.cfgContainer.ConfigRef().RequiresServerTagsFilter() {
		return err
	}
	chOpts, _, _ := h.cfgContainer.ChannelOptionsRef(channel)
	if !filterrequire.Missing(chOpts, reply.Options.ServerTagsFilter) {
		return nil
	}
	filterrequire.LogRefused(channel, c.ID(), c.UserID(), reply.Options.Source)
	*reply = centrifuge.SubscribeReply{SubscriptionReady: reply.SubscriptionReady}
	return centrifuge.ErrorPermissionDenied
}

// requireConnectFilters refuses the connect-time subscriptions of a connection without a server tags filter in a namespace
// which requires one. subscriptions is the map of the connect, owned by the call, and storage is the storage of the
// connection, which the dropped channels are removed from. A channel the client asked for at connect (UniConnect) is
// dropped, as a channel the client may not subscribe to is: the client chose it, and it is not subscribed. Any other
// subscription which the backend asserted (connection token, connect proxy, personal channel) refuses the connection with
// DisconnectPermissionDenied, the disconnect of a token the server refuses, so the SDK does not reconnect with it. required
// is the require option as OnClientConnecting read it for its single connection gate, so both use one value.
func (h *Handler) requireConnectFilters(
	clientID string,
	credentials *centrifuge.Credentials,
	subscriptions map[string]centrifuge.SubscribeOptions,
	storage map[string]any,
	required bool,
) error {
	if !required {
		return nil
	}
	dropped, refused := h.requireFilters(clientID, credentialsUserID(credentials), subscriptions, nil)
	forgetDroppedRecords(storage, dropped)
	if refused {
		return centrifuge.DisconnectPermissionDenied
	}
	return nil
}

// requireFilters is the loop shared by connect, rebind and restore. kept reports a channel whose entry the caller does not
// subscribe (nil keeps none); it is asked only about an entry without a filter, and such an entry is skipped when it is
// kept. A UniConnect entry without a filter is deleted from subs and returned as dropped; any other entry without a filter
// stops the loop with refused true. The first refusal wins. subs is a map, so with several bad channels the one refused is
// not fixed, and tests use one bad channel per assertion.
func (h *Handler) requireFilters(
	clientID string,
	userID string,
	subs map[string]centrifuge.SubscribeOptions,
	kept func(string) bool,
) (dropped []string, refused bool) {
	for channel, opts := range subs {
		chOpts, _, _ := h.cfgContainer.ChannelOptionsRef(channel)
		if !filterrequire.Missing(chOpts, opts.ServerTagsFilter) {
			continue
		}
		if kept != nil && kept(channel) {
			continue
		}
		filterrequire.LogRefused(channel, clientID, userID, opts.Source)
		if opts.Source == subsource.UniConnect {
			delete(subs, channel)
			dropped = append(dropped, channel)
			continue
		}
		return dropped, true
	}
	return dropped, false
}

// requireReboundPrecheck refuses a rebind before anything moves, when a subscription which the rebind makes carries no server
// tags filter in a namespace which requires one. subs are the subscriptions of the rebind, and the personal channel of newUser
// is checked too. rendered is subs with the templates of the new identity applied, see checkFilterTemplates, so a
// template-produced filter counts; nil means no template is rendered and subs are checked as they are. A held channel which
// the rebind keeps is skipped, see keptAtRebindPrecheck. It returns the UniConnect channels of subs which are dropped, which
// the caller removes from subs, or ErrorPermissionDenied, which leaves the connection as it was.
func (h *Handler) requireReboundPrecheck(
	c rebindableClient,
	values filtertemplate.Values,
	newUser string,
	subs map[string]centrifuge.SubscribeOptions,
	rendered map[string]centrifuge.SubscribeOptions,
) ([]string, error) {
	if !h.cfgContainer.ConfigRef().RequiresServerTagsFilter() {
		return nil, nil
	}
	kept := h.keptAtRebindPrecheck(c, newUser)
	checked := rendered
	if checked == nil {
		checked = maps.Clone(subs)
	}
	dropped, refused := h.requireFilters(c.ID(), newUser, checked, kept)
	if refused {
		return nil, centrifuge.ErrorPermissionDenied
	}
	if err := h.requirePersonalRebind(c, values, newUser, kept); err != nil {
		return nil, err
	}
	return dropped, nil
}

// requirePersonalRebind is the personal channel part of requireReboundPrecheck. The personal channel is refused, not
// dropped, when it has no filter. A personal channel which cannot be resolved is left to subscribeRebound, which
// disconnects the connection for it as it does without the option.
func (h *Handler) requirePersonalRebind(
	c rebindableClient,
	values filtertemplate.Values,
	newUser string,
	kept func(string) bool,
) error {
	if newUser == "" || !h.cfgContainer.ConfigRef().Client.SubscribeToUserPersonalChannel.Enabled {
		return nil
	}
	personalChannel, personalOptions, err := h.personalChannelSubscription(newUser)
	if err != nil {
		return nil
	}
	personal := map[string]centrifuge.SubscribeOptions{personalChannel: personalOptions}
	if err := h.applyFilterTemplates(personal, values); err != nil {
		return centrifuge.ErrorPermissionDenied
	}
	if _, refused := h.requireFilters(c.ID(), newUser, personal, kept); refused {
		return centrifuge.ErrorPermissionDenied
	}
	return nil
}

// keptAtRebindPrecheck returns the channels which a rebind keeps and the pre-check skips: the channels the connection holds,
// except the ones whose namespace has a template now or which were recorded with one. unsubscribeTemplated removes those
// after the pre-check, and subscribeChannels subscribes them again from the subscriptions, so they are checked. A rebind to
// no user removes the held channels which keepsAnonymous does not allow, see unsubscribeNotAnonymous, so they are checked too.
// The held channels are copied once, when the first subscription without a filter asks, so a rebind whose subscriptions all
// have a filter copies none of them.
func (h *Handler) keptAtRebindPrecheck(c rebindableClient, newUser string) func(string) bool {
	var (
		built    bool
		held     map[string]centrifuge.ChannelContext
		recorded map[string]struct{}
	)
	return func(channel string) bool {
		if !built {
			built = true
			held = c.ChannelsWithContext()
			recorded = make(map[string]struct{})
			for _, templated := range templatedChannelsOf(c) {
				recorded[templated] = struct{}{}
			}
		}
		chCtx, ok := held[channel]
		if !ok || h.templateOf(channel) != nil {
			return false
		}
		if _, wasRecorded := recorded[channel]; wasRecorded {
			return false
		}
		return newUser != "" || h.keepsAnonymous(channel, chCtx.Source)
	}
}

// requireReboundFallback is the check after a rebind is applied, which a reload between the pre-check and the subscribe can
// leave to do. toSubscribe is the map subscribeRebound subscribes; the UniConnect channels which it drops are removed from it
// and their records forgotten. A refusal disconnects the connection, which releases what the rebind applied.
func (h *Handler) requireReboundFallback(
	c rebindableClient,
	userID string,
	toSubscribe map[string]centrifuge.SubscribeOptions,
) error {
	if !h.cfgContainer.ConfigRef().RequiresServerTagsFilter() {
		return nil
	}
	dropped, refused := h.requireFilters(c.ID(), userID, toSubscribe, c.IsSubscribed)
	forgetRebindRecords(c, dropped)
	if refused {
		return centrifuge.DisconnectPermissionDenied
	}
	return nil
}

// refusesRestoredPersonal refuses the restore of the personal channel of a connection which has no filter for it. It is
// the refresh error of the restore, as the pre-check is: the connection keeps its identity and its channels.
func (h *Handler) refusesRestoredPersonal(
	c rebindableClient,
	toSubscribe map[string]centrifuge.SubscribeOptions,
) error {
	if !h.cfgContainer.ConfigRef().RequiresServerTagsFilter() {
		return nil
	}
	if _, refused := h.requireFilters(c.ID(), c.UserID(), toSubscribe, nil); refused {
		return centrifuge.ErrorPermissionDenied
	}
	return nil
}

// dropRebindChannels drops the channels which the require option dropped from the subscriptions of a rebind, after the rebind
// is applied. A channel the connection holds is unsubscribed, so it does not stay on the connection without the filter, and
// the records of the channels are forgotten.
func dropRebindChannels(c rebindableClient, channels []string) {
	for _, channel := range channels {
		if c.IsSubscribed(channel) {
			c.Unsubscribe(channel, unsubscribeInsufficientState)
		}
	}
	forgetRebindRecords(c, channels)
}

// forgetRebindRecords removes the records of channels which a rebind dropped, see forgetDroppedRecords.
func forgetRebindRecords(c rebindableClient, channels []string) {
	if len(channels) == 0 {
		return
	}
	storage, release := c.AcquireStorage()
	forgetDroppedRecords(storage, channels)
	release(storage)
}

// forgetDroppedRecords removes channels which were dropped from the subscriptions of a connection from the records of its
// server tags filter templates. A rebind to another user subscribes again the stored server-side channels which the
// connection still holds, see checkReboundTemplates, so a record of a channel which is not subscribed would subscribe a
// client-side subscription to that channel again as a server-side one.
func forgetDroppedRecords(storage map[string]any, channels []string) {
	serverSide, _ := storage[serverTagsFilterServerSideKey].(map[string]centrifuge.SubscribeOptions)
	templated, _ := storage[serverTagsFilterTemplatedKey].(map[string]struct{})
	for _, channel := range channels {
		delete(serverSide, channel)
		delete(templated, channel)
	}
}

// credentialsUserID returns the user of the credentials of a connection, or "" for a connection without credentials.
func credentialsUserID(credentials *centrifuge.Credentials) string {
	if credentials == nil {
		return ""
	}
	return credentials.UserID
}
