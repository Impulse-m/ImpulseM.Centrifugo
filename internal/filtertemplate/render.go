package filtertemplate

import (
	"strings"

	"github.com/centrifugal/protocol"
)

// renderer builds one fresh tree of a template. The nodes, the child pointers and the vals of the
// tree are carved from three slices allocated once per render, so a render allocates one slice per
// part and one string per templated field.
type renderer struct {
	t      *Template
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
		t:      t,
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
// the variables substituted.
func (r *renderer) field(f *field) (string, error) {
	if !f.templated {
		return f.text, nil
	}
	var b strings.Builder
	b.Grow(f.size)
	for i := range f.parts {
		p := &f.parts[i]
		b.WriteString(p.text)
		if p.v == nil {
			continue
		}
		value := r.variable(p.v)
		if value == "" {
			return "", MissingValueError{Variable: p.v.name}
		}
		b.WriteString(value)
	}
	return b.String(), nil
}

func (r *renderer) variable(v *variable) string {
	switch v.kind {
	case variableUser:
		return r.v.User
	case variableClient:
		return r.v.Client
	}
	return r.claim(v)
}

// claim returns the value of a claim, or "" when the token has no such claim. The values of the
// connection are read by index when they are aligned with the claim paths of the set, which is the
// case unless the config was reloaded after the connection was verified. Otherwise the path is
// searched by name.
func (r *renderer) claim(v *variable) string {
	paths := r.v.Claims.Paths
	values := r.v.Claims.Values
	if sameSlice(paths, r.t.set.claims) {
		if v.index < len(values) {
			return values[v.index]
		}
		return ""
	}
	return r.v.Claims.Value(v.path)
}

// sameSlice reports whether a and b are the same non-empty slice.
func sameSlice(a []string, b []string) bool {
	return len(a) > 0 && len(a) == len(b) && &a[0] == &b[0]
}
