package config

// anyRequireServerTagsFilter reports whether channel.without_namespace or any namespace has require_server_tags_filter.
func anyRequireServerTagsFilter(config *Config) bool {
	if config.Channel.WithoutNamespace.RequireServerTagsFilter {
		return true
	}
	for _, ns := range config.Channel.Namespaces {
		if ns.RequireServerTagsFilter {
			return true
		}
	}
	return false
}

// RequiresServerTagsFilter reports whether any channel namespace, or channels without namespace, has the
// require_server_tags_filter option. It is one bool read, so the hot paths check it first.
func (c *Config) RequiresServerTagsFilter() bool {
	return c.requiresServerTagsFilter
}
