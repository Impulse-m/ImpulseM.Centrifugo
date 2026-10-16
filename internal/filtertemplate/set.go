package filtertemplate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"unicode"

	"github.com/centrifugal/protocol"
	"github.com/valyala/fasttemplate"
)

const (
	variableOpen          = "{{"
	variableClose         = "}}"
	userVariable          = "user"
	clientVariable        = "client"
	claimPrefix           = "claims."
	withoutNamespaceLabel = "channel.without_namespace"
)

// node is a compiled filter node. Op, key and cmp are literal.
type node struct {
	op    string
	key   string
	cmp   string
	val   field
	vals  []field
	nodes []*node
}

// field is a val or vals string. A field without variables is literal text. A templated field is
// executed by fasttemplate, and names lists the names of its variables in the order they appear.
type field struct {
	text  string
	tmpl  *fasttemplate.Template
	names []string
}

// Set is the compiled templates of all namespaces of a config. It is immutable and safe for
// concurrent use.
type Set struct {
	claims    []string
	templates map[string]*Template
}

// CompileSet compiles the templates of the namespaces. The key of sources is the namespace name,
// "" for channels without namespace. A source without a filter has no template.
func CompileSet(sources map[string]Source) (*Set, error) {
	set := &Set{templates: make(map[string]*Template)}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		t, err := compileSource(name, sources[name])
		if err != nil {
			return nil, err
		}
		if t != nil {
			set.templates[name] = t
		}
	}
	var paths []string
	for _, t := range set.templates {
		t.root.variables(func(name string) {
			if path, ok := strings.CutPrefix(name, claimPrefix); ok {
				paths = append(paths, path)
			}
		})
	}
	slices.Sort(paths)
	set.claims = slices.Compact(paths)
	return set, nil
}

// Claims returns the sorted union of the claim paths of all templates of the set. The slice is
// shared and must not be modified.
func (s *Set) Claims() []string {
	return s.claims
}

// Template returns the template of a namespace, or nil when the namespace has none.
func (s *Set) Template(name string) *Template {
	return s.templates[name]
}

// Label names the channels of a namespace in an error message: the namespace, or channel.without_namespace for "".
// The config errors of the templates use it too, so one spelling reaches the operator.
func Label(name string) string {
	if name == "" {
		return withoutNamespaceLabel
	}
	return fmt.Sprintf("namespace %q", name)
}

func compileSource(name string, src Source) (*Template, error) {
	label := Label(name)
	if len(src.Filter) == 0 {
		if src.OnMissingValue != "" {
			return nil, fmt.Errorf("%s: on_missing_value requires filter", label)
		}
		return nil, nil
	}
	onMissing, err := parseOnMissing(src.OnMissingValue)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	data, err := json.Marshal(src.Filter)
	if err != nil {
		return nil, fmt.Errorf("%s: filter: %w", label, err)
	}
	var sp protocol.FilterNode
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sp); err != nil {
		return nil, fmt.Errorf("%s: filter: %w", label, err)
	}
	root, err := compileNode(&sp)
	if err != nil {
		return nil, fmt.Errorf("%s: filter: %w", label, err)
	}
	t := &Template{root: root, onMissing: onMissing}
	t.countParts()
	return t, nil
}

func parseOnMissing(value string) (OnMissing, error) {
	switch value {
	case "", "reject":
		return OnMissingReject, nil
	case "skip":
		return OnMissingSkip, nil
	}
	return 0, fmt.Errorf("unknown on_missing_value %q", value)
}

// compileNode compiles a node of the filter tree as decoded from the configured JSON.
func compileNode(sp *protocol.FilterNode) (*node, error) {
	for _, literal := range []string{sp.Op, sp.Key, sp.Cmp} {
		if strings.Contains(literal, variableOpen) {
			return nil, fmt.Errorf("variables are allowed only in val and vals, got %q", literal)
		}
	}
	n := &node{op: sp.Op, key: sp.Key, cmp: sp.Cmp}
	val, err := parseField(sp.Val)
	if err != nil {
		return nil, fmt.Errorf("val: %w", err)
	}
	n.val = val
	if sp.Vals != nil {
		n.vals = make([]field, len(sp.Vals))
		for i, s := range sp.Vals {
			f, err := parseField(s)
			if err != nil {
				return nil, fmt.Errorf("vals[%d]: %w", i, err)
			}
			n.vals[i] = f
		}
	}
	if len(sp.Nodes) > 0 {
		n.nodes = make([]*node, len(sp.Nodes))
		for i, child := range sp.Nodes {
			if child == nil {
				return nil, fmt.Errorf("nodes[%d]: null node", i)
			}
			c, err := compileNode(child)
			if err != nil {
				return nil, fmt.Errorf("nodes[%d]: %w", i, err)
			}
			n.nodes[i] = c
		}
	}
	return n, nil
}

// parseField compiles a val or vals string. The variables are found by executing the template once with a tag
// function which checks each tag and writes nothing, so the rules of the tags are the rules of the render.
func parseField(s string) (field, error) {
	if !strings.Contains(s, variableOpen) {
		return field{text: s}, nil
	}
	tmpl, err := fasttemplate.NewTemplate(s, variableOpen, variableClose)
	if err != nil {
		return field{}, err
	}
	f := field{tmpl: tmpl}
	_, err = tmpl.ExecuteFuncStringWithErr(func(_ io.Writer, tag string) (int, error) {
		name := strings.TrimSpace(tag)
		if err := checkVariable(name); err != nil {
			return 0, err
		}
		f.names = append(f.names, name)
		return 0, nil
	})
	if err != nil {
		return field{}, err
	}
	return f, nil
}

// checkVariable returns the error of a variable name which is not one of user, client and claims.<path>.
func checkVariable(name string) error {
	switch {
	case name == userVariable || name == clientVariable:
		return nil
	case strings.HasPrefix(name, claimPrefix):
		path := strings.TrimPrefix(name, claimPrefix)
		if path == "" || strings.ContainsFunc(path, unicode.IsSpace) {
			return fmt.Errorf("invalid claim path in variable %q", name)
		}
		return nil
	}
	return fmt.Errorf("unknown variable %q", name)
}

// variables calls fn with the name of every variable of the tree.
func (n *node) variables(fn func(name string)) {
	n.val.variables(fn)
	for i := range n.vals {
		n.vals[i].variables(fn)
	}
	for _, child := range n.nodes {
		child.variables(fn)
	}
}

func (f *field) variables(fn func(name string)) {
	for _, name := range f.names {
		fn(name)
	}
}

// countParts sets the sizes of the parts of the tree which a render allocates in one piece each.
func (t *Template) countParts() {
	t.root.count(t)
}

func (n *node) count(t *Template) {
	t.nodeCount++
	t.pointerCount += len(n.nodes)
	t.valueCount += len(n.vals)
	for _, child := range n.nodes {
		child.count(t)
	}
}
