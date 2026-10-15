// Package filtertemplate compiles tags filter templates of channel namespaces and renders them
// for one connection. A template is a tags filter tree whose val and vals strings may refer to
// the values of the connection: {{claims.<path>}} for a token claim, {{user}} for the user ID and
// {{client}} for the client ID.
package filtertemplate

import (
	"fmt"

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

// Values are the values a template is rendered with.
type Values struct {
	Claims Claims
	User   string
	Client string
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
	set       *Set
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
