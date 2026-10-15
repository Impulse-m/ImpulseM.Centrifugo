package config

import (
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
)

// validateServerTagsFilterTemplate checks the server tags filter template of one namespace with the rules it is
// compiled with. The name is the namespace name, or "" for channels without namespace. The error names the namespace.
// Presence and join/leave messages are not filtered by a template, so a namespace with a template must not have them.
func validateServerTagsFilterTemplate(name string, c configtypes.ChannelOptions) error {
	_, err := filtertemplate.CompileSet(map[string]filtertemplate.Source{
		name: filterTemplateSource(c.ServerTagsFilterTemplate),
	})
	if err != nil {
		return err
	}
	if len(c.ServerTagsFilterTemplate.Filter) == 0 {
		return nil
	}
	if c.Presence || c.JoinLeave || c.ForcePushJoinLeave || c.MapClientsPresenceChannelPrefix != "" || c.MapUsersPresenceChannelPrefix != "" {
		return fmt.Errorf("%s: presence, join_leave, force_push_join_leave and the map presence prefixes must be off with server_tags_filter_template", templateLabel(name))
	}
	return nil
}

// templateLabel names the channels a template applies to in an error message.
func templateLabel(name string) string {
	if name == "" {
		return "channel.without_namespace"
	}
	return fmt.Sprintf("namespace %q", name)
}
