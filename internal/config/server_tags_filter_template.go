package config

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/filtertemplate"
)

// buildCompiledFilterTemplates compiles the server tags filter templates of the channel namespaces and of
// channels without namespace. The compiled templates are set on the options, and the claim paths and the
// presence of templates are kept in the config, so a connection knows what to read from its token.
func buildCompiledFilterTemplates(config Config) (Config, error) {
	sources := map[string]filtertemplate.Source{
		"": filterTemplateSource(config.Channel.WithoutNamespace.ServerTagsFilterTemplate),
	}
	for _, ns := range config.Channel.Namespaces {
		if _, ok := sources[ns.Name]; ok {
			// The first namespace of a name wins, see buildNamespaceOptions.
			continue
		}
		sources[ns.Name] = filterTemplateSource(ns.ServerTagsFilterTemplate)
	}
	set, err := filtertemplate.CompileSet(sources)
	if err != nil {
		return config, err
	}

	hasTemplates := false
	config.Channel.WithoutNamespace.Compiled.ServerTagsFilterTemplate = set.Template("")
	if config.Channel.WithoutNamespace.Compiled.ServerTagsFilterTemplate != nil {
		hasTemplates = true
	}
	var namespaces []configtypes.ChannelNamespace
	for _, ns := range config.Channel.Namespaces {
		ns.Compiled.ServerTagsFilterTemplate = set.Template(ns.Name)
		if ns.Compiled.ServerTagsFilterTemplate != nil {
			hasTemplates = true
		}
		namespaces = append(namespaces, ns)
	}
	config.Channel.Namespaces = namespaces
	config.serverTagsFilterClaims = set.Claims()
	config.hasServerTagsFilterTemplates = hasTemplates
	return config, nil
}

func filterTemplateSource(template configtypes.ServerTagsFilterTemplate) filtertemplate.Source {
	return filtertemplate.Source{
		Filter:         template.Filter,
		OnMissingValue: template.OnMissingValue,
	}
}

// ServerTagsFilterClaims returns the sorted union of the claim paths which the server tags filter templates
// of the config refer to. The slice is shared and must not be modified. Nil when no template refers to a claim.
func (c *Config) ServerTagsFilterClaims() []string {
	return c.serverTagsFilterClaims
}

// HasServerTagsFilterTemplates reports whether any channel namespace, or channels without namespace, has a
// server tags filter template.
func (c *Config) HasServerTagsFilterTemplates() bool {
	return c.hasServerTagsFilterTemplates
}
