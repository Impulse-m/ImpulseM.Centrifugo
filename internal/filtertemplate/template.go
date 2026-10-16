// Package filtertemplate compiles tags filter templates of channel namespaces and renders them
// for one connection. A template is a tags filter tree whose val and vals strings may refer to
// the values of the connection: {{claims.<path>}} for a token claim, {{user}} for the user ID and
// {{client}} for the client ID.
package filtertemplate

import (
	"fmt"
	"slices"

	"github.com/centrifugal/protocol"
)

// OnMissing is the policy of a template whose variable has no value.
type OnMissing uint8

const (
	// OnMissingReject refuses the subscription which needs the template.
	OnMissingReject OnMissing = iota
	// OnMissingSkip applies no template to the subscription, the explicit filter still applies.
	OnMissingSkip
)

// Source is one namespace template as configured.
type Source struct {
	// Filter is the tags filter tree in the JSON form of protocol filter nodes.
	Filter map[string]any
	// OnMissingValue is "reject", "skip" or empty, which means reject.
	OnMissingValue string
}

// Claims are the values of the token claims a connection was verified with. Paths is the union
// of the claim paths of the config the connection was verified under. The slice is shared and is
// not copied per connection. Values is aligned with Paths, and a missing claim has an empty value.
type Claims struct {
	Paths  []string
	Values []string
}

// Value returns the value of the claim at path, or "" when the claims have none. The paths may differ from the
// paths of the config, for example after a reload, so the claim is found by name.
func (c Claims) Value(path string) string {
	i := slices.Index(c.Paths, path)
	if i < 0 || i >= len(c.Values) {
		return ""
	}
	return c.Values[i]
}

// Equal reports whether c and d have the same value for every path of either set. A missing claim has the value "",
// so a claim which is missing and one which is empty are the same.
func (c Claims) Equal(d Claims) bool {
	for _, path := range c.Paths {
		if c.Value(path) != d.Value(path) {
			return false
		}
	}
	for _, path := range d.Paths {
		if c.Value(path) != d.Value(path) {
			return false
		}
	}
	return true
}

// Values are the values a template is rendered with.
type Values struct {
	Claims Claims
	User   string
	Client string
	// Unbound is set for a connection which has no token claims bound, such as one connected through a connect proxy
	// without a token. Render does not read it: the caller decides what a missing claim of such a connection means.
	Unbound bool
}

// MissingValueError is returned by Render for a variable which has no value.
type MissingValueError struct {
	// Variable is the name of the variable, for example claims.sid.
	Variable string
}

// Error implements error.
func (e MissingValueError) Error() string {
	return fmt.Sprintf("missing value of %q", e.Variable)
}

// Template is a compiled namespace template. It is immutable and safe for concurrent use.
type Template struct {
	root      *node
	onMissing OnMissing
	// nodeCount, pointerCount and valueCount are the sizes of the parts of the tree a render
	// allocates in one piece each: the nodes, the child pointers and the vals.
	nodeCount    int
	pointerCount int
	valueCount   int
}

// OnMissing returns the policy of the template for a missing value.
func (t *Template) OnMissing() OnMissing {
	return t.onMissing
}

// And combines a rendered template with an explicit filter by the and operation. A nil filter
// is left out, so And returns the other one, or nil when both are nil.
func And(template *protocol.FilterNode, explicit *protocol.FilterNode) *protocol.FilterNode {
	if explicit == nil {
		return template
	}
	if template == nil {
		return explicit
	}
	return &protocol.FilterNode{
		Op:    "and",
		Nodes: []*protocol.FilterNode{template, explicit},
	}
}
