package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"

	"github.com/centrifugal/centrifuge"
)

// validateServerTagsFilterTemplate checks the server tags filter template of one namespace with the rules it is
// compiled with. The name is the namespace name, or "" for channels without namespace. The error names the namespace.
// Presence and join/leave messages are not filtered by a template, so a namespace with a template must not have them.
// The shared poll subscriptions do not apply the server tags filter of a subscribe reply, so a template there would
// leave every subscriber unfiltered. The structure of the rendered template is checked with the rules of the server
// tags filter of Core, on a placeholder render.
func validateServerTagsFilterTemplate(name string, c configtypes.ChannelOptions) error {
	set, err := filtertemplate.CompileSet(map[string]filtertemplate.Source{
		name: filterTemplateSource(c.ServerTagsFilterTemplate),
	})
	if err != nil {
		return err
	}
	tmpl := set.Template(name)
	if tmpl == nil {
		return nil
	}
	if c.Presence || c.JoinLeave || c.ForcePushJoinLeave || c.MapClientsPresenceChannelPrefix != "" || c.MapUsersPresenceChannelPrefix != "" {
		return fmt.Errorf("%s: presence, join_leave, force_push_join_leave and the map presence prefixes must be off with server_tags_filter_template", filtertemplate.Label(name))
	}
	if c.SubscriptionType == "shared_poll" {
		return fmt.Errorf("%s: server_tags_filter_template is not supported with subscription_type shared_poll", filtertemplate.Label(name))
	}
	if err := centrifuge.ValidateFilterNode(tmpl.Placeholder()); err != nil {
		return fmt.Errorf("%s: filter: %w", filtertemplate.Label(name), err)
	}
	return nil
}
