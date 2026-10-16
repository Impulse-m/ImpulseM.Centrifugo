package filtertemplate

import (
	"io"
	"strings"

	"github.com/centrifugal/protocol"
)

// renderer builds one fresh tree of a template. The nodes, the child pointers and the vals of the
// tree are carved from three slices allocated once per render, so a render allocates one slice per
// part and one string per templated field.
type renderer struct {
	v      Values
	nodes  []protocol.FilterNode
	ptrs   []*protocol.FilterNode
	values []string
}

// Render returns a fresh filter tree of the template with the values substituted. The template is
// not changed, so every call builds its own tree. A variable without a value gives a
// MissingValueError which names the variable.
func (t *Template) Render(v Values) (*protocol.FilterNode, error) {
	r := renderer{
		v:      v,
		nodes:  make([]protocol.FilterNode, t.nodeCount),
		ptrs:   make([]*protocol.FilterNode, t.pointerCount),
		values: make([]string, t.valueCount),
	}
	return r.node(t.root)
}

func (r *renderer) node(n *node) (*protocol.FilterNode, error) {
	out := &r.nodes[0]
	r.nodes = r.nodes[1:]
	out.Op = n.op
	out.Key = n.key
	out.Cmp = n.cmp
	val, err := r.field(&n.val)
	if err != nil {
		return nil, err
	}
	out.Val = val
	if n.vals != nil {
		out.Vals = r.values[:len(n.vals):len(n.vals)]
		r.values = r.values[len(n.vals):]
		for i := range n.vals {
			s, err := r.field(&n.vals[i])
			if err != nil {
				return nil, err
			}
			out.Vals[i] = s
		}
	}
	if len(n.nodes) > 0 {
		out.Nodes = r.ptrs[:len(n.nodes):len(n.nodes)]
		r.ptrs = r.ptrs[len(n.nodes):]
		for i, child := range n.nodes {
			c, err := r.node(child)
			if err != nil {
				return nil, err
			}
			out.Nodes[i] = c
		}
	}
	return out, nil
}

// field returns the value of a field: its text when it has no variables, otherwise the text with
// the variables substituted by fasttemplate.
func (r *renderer) field(f *field) (string, error) {
	if f.tmpl == nil {
		return f.text, nil
	}
	return f.tmpl.ExecuteFuncStringWithErr(r.tag)
}

// tag writes the value of the variable of one tag. A variable without a value is a MissingValueError, which
// fasttemplate returns as it is.
func (r *renderer) tag(w io.Writer, tag string) (int, error) {
	name := strings.TrimSpace(tag)
	value := r.value(name)
	if value == "" {
		return 0, MissingValueError{Variable: name}
	}
	return io.WriteString(w, value)
}

// value returns the value of a variable, which is checked when the template is compiled. The claims are found by
// name, so the values stay correct after a config reload changed the claim paths of the set.
func (r *renderer) value(name string) string {
	switch name {
	case userVariable:
		return r.v.User
	case clientVariable:
		return r.v.Client
	}
	return r.v.Claims.Value(strings.TrimPrefix(name, claimPrefix))
}
