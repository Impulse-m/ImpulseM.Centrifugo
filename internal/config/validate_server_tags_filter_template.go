package config

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
)

// validateServerTagsFilterTemplate checks the server tags filter template of one namespace with the rules it is
// compiled with. The name is the namespace name, or "" for channels without namespace. The error names the namespace.
func validateServerTagsFilterTemplate(name string, c configtypes.ChannelOptions) error {
	_, err := filtertemplate.CompileSet(map[string]filtertemplate.Source{
		name: filterTemplateSource(c.ServerTagsFilterTemplate),
	})
	return err
}
