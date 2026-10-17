package configtypes

// TagsFilterNode is a tags filter tree of a server tags filter template in its JSON form, with the
// fields op, key, cmp, val, vals and nodes. It is a map rather than a struct: the config walkers do not
// descend into a map, and the keys of the tree are checked when the template is compiled.
type TagsFilterNode map[string]any

// ServerTagsFilterTemplate is a tags filter applied to every subscription of a namespace. Its val and
// vals strings may refer to the values of the connection, and it is combined with an explicit server
// tags filter of the subscription by the and operation.
type ServerTagsFilterTemplate struct {
	// Filter is the tags filter tree of the template. Empty means no template.
	Filter TagsFilterNode `mapstructure:"filter" json:"filter" envconfig:"-" yaml:"filter" toml:"filter" doc:"Tags filter tree applied to every subscription in the namespace, in the same form as a server tags filter. Its <<val>> and <<vals>> strings may use <<{{claims.<path>}}>>, <<{{user}}>> and <<{{client}}>>. Empty disables the template."`

	// OnMissingValue is the policy for a subscription whose template refers to a missing value: reject
	// refuses the subscription, skip applies no template to it.
	OnMissingValue string `mapstructure:"on_missing_value" json:"on_missing_value" envconfig:"on_missing_value" yaml:"on_missing_value" toml:"on_missing_value" doc:"What to do when a template variable has no value for the connection: <<reject>> (default) refuses the subscription, <<skip>> applies no template to it."`
}

// HasServerTagsFilterTemplate reports whether the channel options have a server tags filter template, which is compiled
// from the namespace config.
func (o *ChannelOptions) HasServerTagsFilterTemplate() bool {
	return o.Compiled.ServerTagsFilterTemplate != nil
}
